from collections import defaultdict
import os
import logging
import signal
from common import middleware, message_protocol, heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SCATTER_DETECTOR_PREFIX = os.environ["SCATTER_DETECTOR_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
Q4_GRAPH_AMOUNT = int(os.environ["Q4_GRAPH_AMOUNT"])
SCATTER_DETECTOR_STORAGE = "/output/q4_scatter_"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/scatter_det_{ID}")


class ScatterGatherDetector:
    def __init__(self):

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SCATTER_DETECTOR_PREFIX,
            [SCATTER_DETECTOR_PREFIX, SCATTER_DETECTOR_PREFIX + str(ID)],
            ID
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_PREFIX,  source_id=f"ScatterGatherDetector_{ID}"
        )
        self.suspicious_accounts = {}
        self.accounts = {}
        self.eof_count = {}
        self.results = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        
        default_state = {
            "suspicious_accounts": {},
            "accounts": {},
            "eof": {},
            "__msg_counters": {}
        }
        
        loaded_state, _, _ = self.wal.backup_load(default=(default_state, 0, set()))
        state = loaded_state if isinstance(loaded_state, dict) else default_state
        
        state.setdefault("suspicious_accounts", {})
        state.setdefault("accounts", {})
        state.setdefault("eof", {})
        state.setdefault("__msg_counters", {})

        self.suspicious_accounts = state["suspicious_accounts"]
        self.accounts = state["accounts"]
        self.eof_count = state["eof"]
        
        middleware._init_msg_id_counters(state["__msg_counters"])

        self._orphans = self.wal.recover(self._wal_apply, state)

    @staticmethod
    def _wal_apply(entry, state):
        """Aplica las transacciones del log al estado en memoria tras un reinicio."""
        client_id = entry.get("client_id")
        
        if entry["type"] == "origin":
            suspicious = state["suspicious_accounts"].setdefault(client_id, {})
            suspicious[entry["origin_account"]] = entry["transactions"]
            
        elif entry["type"] == "destination":
            accounts = state["accounts"].setdefault(client_id, {})
            account_data = accounts.setdefault(entry["destination_account"], {})
            account_data.update(entry["transactions"])
            
        elif entry["type"] == "eof_count":
            state["eof"][client_id] = entry["count"]
            
        elif entry["type"] == "eof_done":
            state["eof"].pop(client_id, None)
            state["suspicious_accounts"].pop(client_id, None)
            state["accounts"].pop(client_id, None)

    def _process_eof(self, message, msg_id):
        client_id = message.get("client_id")
        if client_id is None:
            return

        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1

        if self.eof_count[client_id] < Q4_GRAPH_AMOUNT:
            return
        
        final_dict = {}
        if client_id in self.accounts.keys():
            for account, transactions in self.suspicious_accounts[client_id].items():
                for middle_man, transaction in transactions.items():
                    for final_account, final_transactions in self.accounts[client_id].items():
                        if middle_man in final_transactions.keys():
                            if account not in final_dict.keys():
                                final_dict[account] = {}
                            if final_account not in final_dict[account].keys():
                                final_dict[account][final_account]=0
                            final_dict[account][final_account] += 1
        self.output_queue.send(message_protocol.internal.serialize({"client_id": client_id, "suspicious_accounts": final_dict}))
        self.output_queue.send( message_protocol.internal.serialize(
            {"nodo_id": ID, "client_id": client_id}
        ))
        self.suspicious_accounts.pop(client_id, None)
        self.accounts.pop(client_id, None)
        self.eof_count.pop(client_id, None)
        self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})

    def process_message(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            deserialized = message_protocol.internal.deserialize(message)
            if len(deserialized) == 2:
                self._process_eof(deserialized, msg_id)
            elif "origin_account" in deserialized.keys():
                client_id=deserialized.pop("client_id")
                self.wal.append(msg_id, {
                        "type": "origin", 
                        "client_id": client_id, 
                        "origin_account": deserialized["origin_account"], 
                        "transactions": deserialized["transactions"]
                    })
                if client_id not in self.suspicious_accounts:
                    self.suspicious_accounts[client_id] = {}
                self.suspicious_accounts[client_id][deserialized["origin_account"]] = deserialized["transactions"]
            elif "destination_account" in deserialized.keys():
                client_id=deserialized.pop("client_id")
                self.wal.append(msg_id, {
                        "type": "destination", 
                        "client_id": client_id, 
                        "destination_account": deserialized["destination_account"], 
                        "transactions": deserialized["transactions"]
                    })
                accounts = self.accounts.setdefault(client_id, {})
                account_data = accounts.setdefault(deserialized["destination_account"], {})
                account_data.update(deserialized["transactions"])
            if msg_id:
                    self.wal.processed_ids.add(msg_id)
            current_state = {
                    "suspicious_accounts": self.suspicious_accounts,
                    "accounts": self.accounts,
                    "eof": self.eof_count,
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
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        self.output_queue.close()

    def stop(self):
        state = {
            "suspicious_accounts": self.suspicious_accounts,
            "accounts": self.accounts,
            "eof": self.eof_count,
            "__msg_counters": middleware.get_msg_id_counters()
        }
        self.wal.backup_save(state, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        try:
            if self.input_exchange:
                self.input_exchange.close()
        except Exception as e:
            logging.warning(f"Input exchange ya estaba cerrado: {e}")

        try:
            if self.output_queue:
                self.output_queue.close()
        except Exception as e:
            logging.warning(f"Output queue ya estaba cerrada: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    scatter_detector = ScatterGatherDetector()
    signal.signal(signal.SIGTERM, lambda signum, frame: scatter_detector.stop())
    scatter_detector.start()
    scatter_detector.close()
    return 0

if __name__ == "__main__":
    main()