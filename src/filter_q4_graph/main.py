import os
import logging
import signal
import zlib
from collections import defaultdict

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_DATE_AMOUNT = int(os.environ["FILTER_DATE_AMOUNT"])
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]
SCATTER_VALUE = int(os.environ["SCATTER_VALUE"])
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/graph_filter_{ID}")


class GraphFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            FILTER_PREFIX,
            [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"],
            ID
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            OUTPUT_PREFIX,
            [],
            ID,
            publish_only=True,
            source_id=f"GraphFilter_{ID}"
        )
        self.eof_count = {}
        self.origin_groups = {}
        self.destination_groups = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        default_state = {
            "eof": {},
            "origin": {},
            "destination": {},
            "__msg_counters": {}
        }
        loaded_state, _, _ = self.wal.backup_load(default=(default_state, 0, set()))
        state = loaded_state if isinstance(loaded_state, dict) else default_state
        state.setdefault("eof", {})
        state.setdefault("origin", {})
        state.setdefault("destination", {})
        state.setdefault("__msg_counters", {})
        self.eof_count = state["eof"]
        self.origin_groups = state["origin"]
        self.destination_groups = state["destination"]
        middleware._init_msg_id_counters(state["__msg_counters"])
        self._orphans = self.wal.recover(self._wal_apply, state)

    @staticmethod
    def _wal_apply(entry, state):
        """Reaplica las mutaciones del log al estado en memoria durante la recuperación."""
        client_id = entry.get("client_id")
        
        if entry["type"] == "data":
            transaction = entry["transaction"]
            origin_key = transaction["origin_key"]
            destination_key = transaction["destination_key"]

            origin = state["origin"].setdefault(client_id, {})
            destination = state["destination"].setdefault(client_id, {})
            
            origin_data = origin.setdefault(origin_key, {"destinations": {}})["destinations"]
            destination_data = destination.setdefault(destination_key, {"transactions": {}})["transactions"]
            
            if transaction["to_account"] is not None or transaction["to_bank"] is not None:
                if destination_key not in origin_data:
                    origin_data[destination_key] = {
                        "account": transaction["account"], "from_bank": transaction["from_bank"], 
                        "to_account": transaction["to_account"], "to_bank": transaction["to_bank"]
                    }
                if origin_key not in destination_data:
                    destination_data[origin_key] = {
                        "account": transaction["account"], "from_bank": transaction["from_bank"], 
                        "to_account": transaction["to_account"], "to_bank": transaction["to_bank"]
                    }

        elif entry["type"] == "eof_count":
            state["eof"][client_id] = entry["count"]

        elif entry["type"] == "eof_done":
            state["eof"].pop(client_id, None)
            state["origin"].pop(client_id, None)
            state["destination"].pop(client_id, None)

    def _process_data(self, transaction, msg_id):
        client_id = transaction.get("client_id")
        if client_id is None:
            return

        origin_account = transaction.get("account")
        origin_bank = transaction.get("from_bank")
        destination_account = transaction.get("to_account")
        destination_bank = transaction.get("to_bank")

        origin_key = f"{origin_bank},{origin_account}"
        destination_key = f"{destination_bank},{destination_account}"
        self.wal.append(msg_id, {
            "type": "data",
            "client_id": client_id,
            "transaction": {
                "origin_key": origin_key,
                "destination_key": destination_key,
                "account": origin_account,
                "from_bank": origin_bank,
                "to_account": destination_account,
                "to_bank": destination_bank
            }
        })

        origin = self.origin_groups.setdefault(client_id, {})
        destination = self.destination_groups.setdefault(client_id, {})

        origin_data = origin.setdefault(origin_key, {"destinations": {}})["destinations"]
        destination_data = destination.setdefault(destination_key, {"transactions": {}})["transactions"]

        if destination_account is not None or destination_bank is not None:
            if destination_key not in origin_data:
                origin_data[destination_key] = {
                    "account": origin_account, "from_bank": origin_bank, 
                    "to_account": destination_account, "to_bank": destination_bank
                }
            if origin_key not in destination_data:
                destination_data[origin_key] = {
                    "account": origin_account, "from_bank": origin_bank, 
                    "to_account": destination_account, "to_bank": destination_bank
                }
            

    def _process_eof(self, deserialized_message, msg_id):
        client_id = deserialized_message.get("client_id")
        if client_id is None:
            return

        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        if self.eof_count[client_id] < FILTER_DATE_AMOUNT:
            return
        if client_id in self.origin_groups.keys():
            for  origin_key, data2 in self.origin_groups[client_id].items():
                if len(data2["destinations"].keys()) >= SCATTER_VALUE:
                    # Se hace un broadcast de todas las cuentas sospechosas (las que le enviaron
                    # dinero a >=5 cuentas distintas)
                    self.output_exchange.send_by_key(
                        message_protocol.internal.serialize(
                            {"client_id": client_id, "origin_account": origin_key, "transactions": data2["destinations"]}
                        ),
                        OUTPUT_PREFIX,
                    )
            for destination_key, data2 in self.destination_groups[client_id].items():
                # Se rutea en base al destino (to_bank y to_account)
                routing_key=self._get_output_routing_key(destination_key[0], destination_key[1])
                self.output_exchange.send_by_key(
                    message_protocol.internal.serialize(
                        {"client_id": client_id, "destination_account": destination_key, "transactions": data2["transactions"]}
                    ),
                    routing_key,
                )
        self.output_exchange.send_by_key(
            message_protocol.internal.serialize(
                {"nodo_id": ID, "client_id": client_id}
            ),
            OUTPUT_PREFIX,
        )
        self.origin_groups.pop(client_id, None)
        self.destination_groups.pop(client_id, None)
        self.eof_count.pop(client_id, None)
        self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})

    def _format_node(self, node_key):
        bank, account = node_key
        return f"bank={bank or 'unknown'} account={account or 'unknown'}"

    def _format_edges(self, edges):
        return {
            self._format_node(node): count for node, count in edges.items()
        }

    def _get_output_routing_key(self, bank, account):
        origin_hash = zlib.crc32(f"{bank}:{account}".encode("utf-8"))
        return OUTPUT_PREFIX + str(origin_hash % OUTPUT_AMOUNT)

    def _send_result(self, result, bank, account):
        routing_key = self._get_output_routing_key(bank, account)
        self.output_exchange.send_by_key(
            message_protocol.internal.serialize(result), routing_key
        )

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            if len(deserialized_message) == 2:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message, msg_id)
            if msg_id:
                self.wal.processed_ids.add(msg_id)
            current_state = {
                    "eof": self.eof_count, 
                    "origin": self.origin_groups, 
                    "destination": self.destination_groups,
                    "__msg_counters": middleware.get_msg_id_counters()
                }
            self.wal.checkpoint(current_state)
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
                heartbeat.start()
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_exchange.close()

    def stop(self):
        state = {
            "eof": self.eof_count, 
            "origin": self.origin_groups, 
            "destination": self.destination_groups,
            "__msg_counters": middleware.get_msg_id_counters()
        }
        self.wal.backup_save(state, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        self.output_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    graph_filter = GraphFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: graph_filter.stop())
    graph_filter.start()
    graph_filter.close()
    return 0


if __name__ == "__main__":
    main()
