from collections import defaultdict
import os
import logging
import signal
import time
from common import middleware, message_protocol, heartbeat
from common.wal import WAL
from common.client_state_ttl import ClientStateTTL

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
        self.client_state_ttl = ClientStateTTL()
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

        self.suspicious_accounts = {str(k): v for k, v in state["suspicious_accounts"].items()}
        state["suspicious_accounts"] = self.suspicious_accounts
        self.accounts = {str(k): v for k, v in state["accounts"].items()}
        state["accounts"] = self.accounts
        self.eof_count = {str(k): v for k, v in state["eof"].items()}
        state["eof"] = self.eof_count
        
        middleware._init_msg_id_counters(state["__msg_counters"])

        self._orphans = self.wal.recover(self._wal_apply, state)
        for orphan in self._orphans:
            if orphan.startswith("results_"):
                self.wal.tx_commit(orphan)
        for cid in list(self.eof_count):
            self._try_send(cid)

    @staticmethod
    def _wal_apply(entry, state):
        client_id = str(entry.get("client_id"))
        
        if entry["type"] == "origin":
            suspicious = state["suspicious_accounts"].setdefault(client_id, {})
            suspicious[entry["origin_account"]] = entry["transactions"]
            
        elif entry["type"] == "destination":
            accounts = state["accounts"].setdefault(client_id, {})
            account_data = accounts.setdefault(entry["destination_account"], {})
            account_data.update(entry["transactions"])
            
        elif entry["type"] == "eof_count":
            state["eof"][client_id] = entry["count"]

    def _try_send(self, client_id):
        if self.eof_count.get(client_id, 0) < Q4_GRAPH_AMOUNT:
            return
        final_dict = {}
        if client_id in self.accounts:
            for account, transactions in self.suspicious_accounts.get(client_id, {}).items():
                for middle_man, transaction in transactions.items():
                    for final_account, final_transactions in self.accounts[client_id].items():
                        if middle_man in final_transactions:
                            if account not in final_dict:
                                final_dict[account] = {}
                            if final_account not in final_dict[account]:
                                final_dict[account][final_account] = 0
                            final_dict[account][final_account] += 1
        self.wal.tx_begin(f"results_{client_id}")
        self.output_queue.send(message_protocol.internal.serialize({"client_id": int(client_id), "suspicious_accounts": final_dict}))
        self.output_queue.send(message_protocol.internal.serialize({"nodo_id": ID, "client_id": int(client_id)}))
        self.wal.tx_commit(f"results_{client_id}")
        self.results.pop(client_id, None)
        self.eof_count.pop(client_id, None)
        self.suspicious_accounts.pop(client_id, None)
        self.accounts.pop(client_id, None)
        self.client_state_ttl.remove(client_id)

    def _process_eof(self, message, msg_id):
        client_id = str(message.get("client_id"))
        if client_id == "None":
            return

        current_count = self.eof_count.get(client_id, 0)
        if current_count >= Q4_GRAPH_AMOUNT:
            return
        self.eof_count[client_id] = current_count + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        if self.eof_count[client_id] < Q4_GRAPH_AMOUNT:
            return
        self._try_send(client_id)

    def _expire_client_state(self, client_id):
        self.results.pop(client_id, None)
        self.eof_count.pop(client_id, None)
        self.suspicious_accounts.pop(client_id, None)
        self.accounts.pop(client_id, None)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

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
                client_id=str(deserialized.pop("client_id"))
                self._cleanup_expired_clients()
                self._update_last_seen(client_id)
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
                client_id=str(deserialized.pop("client_id"))
                self._cleanup_expired_clients()
                self._update_last_seen(client_id)
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
        self.client_state_ttl.clear()

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