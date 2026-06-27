import os
import logging
import zlib
import signal

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_DATE_AMOUNT = int(os.environ["FILTER_DATE_AMOUNT"])
FILTER_DATE_PREFIX = os.environ["FILTER_DATE_PREFIX"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/usd_filter_{ID}")

DONE = True
WORKING = False
TOTAL_QUERIES = 3
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]


class CurrencyFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", f"{ID}"], ID
        )
        self.filter_q_prefixes = [
            os.environ[f"FILTER_Q{i}_PREFIX"] for i in range(1, TOTAL_QUERIES + 1)
        ]
        self.filter_q_amounts = [
            int(os.environ[f"FILTER_Q{i}_AMOUNT"]) for i in range(1, TOTAL_QUERIES + 1)
        ]
        self.counter = 0
        self.output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                self.filter_q_prefixes[i],
                [],
                ID,
                publish_only=True,
            )
            for i in range(TOTAL_QUERIES)
        ]
        self.date_filter_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            FILTER_DATE_PREFIX,
            [],
            ID,
            publish_only=True,
        )
        self.eof_count = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(
                heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT)
            )
        self.wal = WAL(WAL_DIR)
        state, _, _ = self.wal.backup_load(default=({"eof": {}, "__msg_counters": {}}, 0, set()))
        self.eof_count = state["eof"]
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)

    @staticmethod
    def _wal_apply(entry, state):
        if entry["type"] == "eof_count":
            state["eof"][entry["client_id"]] = entry["count"]
        elif entry["type"] == "eof_done":
            state["eof"].pop(entry["client_id"], None)

    def _process_data(self, transaction):
        for i in range(TOTAL_QUERIES):
            send_to_query_i = getattr(self, f"_send_to_query_{i + 1}")
            send_to_query_i(transaction)
        self._send_to_date_filter(transaction)

    def _send_to_query_1(self, transaction):
        if transaction["payment_currency"] == "US Dollar":
            output = {
                "client_id": transaction["client_id"],
                "from_bank": transaction.get("from_bank", ""),
                "account": transaction["account"],
                "to_bank": transaction.get("to_bank", ""),
                "to_account": transaction["to_account"],
                "amount_paid": transaction["amount_paid"],
            }
            routing_key = self.filter_q_prefixes[0] + str(
                zlib.crc32(output["account"].encode("utf-8")) % self.filter_q_amounts[0]
            )
            self.output_exchanges[0].send_by_key(
                message_protocol.internal.serialize(output), str(routing_key)
            )

    def _send_to_query_2(self, transaction):
        if transaction["payment_currency"] == "US Dollar":
            output = {
                "client_id": transaction["client_id"],
                "account": transaction["account"],
                "amount_paid": transaction["amount_paid"],
                "from_bank": transaction["from_bank"],
            }
            routing_key = self.filter_q_prefixes[1] + str(
                zlib.crc32(output["from_bank"].encode("utf-8"))
                % self.filter_q_amounts[1]
            )
            self.output_exchanges[1].send_by_key(
                message_protocol.internal.serialize(output), routing_key
            )

    def _send_to_query_3(self, transaction):
        if transaction["payment_currency"] == "US Dollar":
            output = {
                "client_id": transaction["client_id"],
                "account": transaction["account"],
                "amount_paid": transaction["amount_paid"],
                "timestamp": transaction["timestamp"],
                "payment_format": transaction["payment_format"],
                "from_bank": transaction["from_bank"],
            }
            routing_key = self.filter_q_prefixes[2] + str(
                zlib.crc32(output["account"].encode("utf-8")) % self.filter_q_amounts[2]
            )
            self.output_exchanges[2].send_by_key(
                message_protocol.internal.serialize(output), routing_key
            )

    def _send_to_date_filter(self, transaction):
        if transaction["payment_currency"] == "US Dollar":
            self.counter += 1
            routing_key = FILTER_DATE_PREFIX + str(
                zlib.crc32(transaction["account"].encode("utf-8")) % FILTER_DATE_AMOUNT
            )
            self.date_filter_exchange.send_by_key(
                message_protocol.internal.serialize(transaction), routing_key
            )

    def _process_eof(self, deserialized_message, msg_id=None):
        client_id = deserialized_message[0]
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        for i, output_exchange in enumerate(self.output_exchanges):
            output_exchange.send_by_key(
                message_protocol.internal.serialize(
                    {"nodo_id": ID, "client_id": client_id}
                ),
                self.filter_q_prefixes[i],
            )
        logging.warning(f"{self.counter} passed the filter")
        self.date_filter_exchange.send_by_key(
            message_protocol.internal.serialize(
                {"nodo_id": ID, "client_id": client_id}
            ),
            FILTER_DATE_PREFIX,
        )
        self.eof_count.pop(client_id, None)
        self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            if self.wal.is_checkpoint_necessary():  
                self.wal.checkpoint({"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()})
            ack()
            return
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            if len(deserialized_message) == 1:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message)
            if msg_id:
                self.wal.processed_ids.add(msg_id)
            if self.wal.is_checkpoint_necessary():  
                self.wal.checkpoint({"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("error processing message")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
            heartbeat.start()
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        self.wal.backup_save({"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()}, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        for ex in self.output_exchanges:
            ex.close()
        self.date_filter_exchange.close()


def main():
    logging.basicConfig(level=logging.WARNING)
    dollar_amt_filter = CurrencyFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: dollar_amt_filter.stop(),
    )
    dollar_amt_filter.start()
    dollar_amt_filter.close()
    return 0


if __name__ == "__main__":
    main()
