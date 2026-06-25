import os
import logging
import signal
import csv

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q4_SCATTER_AMOUNT = int(os.environ["Q4_SCATTER_AMOUNT"])
RESULTS_STORAGE = "/output/q4_agg_"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/agg_q4_{0}")

class AggregatorQ4:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.worker_finished_with_client = {}
        self.results = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        
        default_state = {"workers": {}, "results": {}, "__msg_counters": {}}        
        loaded_state, _, _ = self.wal.backup_load(default=(default_state, 0, set()))
        
        if not isinstance(loaded_state, dict):
            state = default_state
        else:
            state = loaded_state
            state.setdefault("workers", {})
            state.setdefault("results", {})
            state.setdefault("__msg_counters", {})
            
        self.worker_finished_with_client = state["workers"]
        self.results = state["results"]
        
        if hasattr(middleware, '_init_msg_id_counters'):
            middleware._init_msg_id_counters(state["__msg_counters"])
            
        self._orphans = self.wal.recover(self._wal_apply, state)

    @staticmethod
    def _wal_apply(entry, state):
        """Reconstruye el estado a partir de los logs de la WAL en caso de caída."""
        client_id = entry["client_id"]
        
        if entry["type"] == "data":
            sus_accounts = entry["sus_accounts"]
            if client_id not in state["results"]:
                state["results"][client_id] = {}
            for account, final_accounts in sus_accounts.items():
                if account not in state["results"][client_id]:
                    state["results"][client_id][account] = {}
                for final_account, transaction_amount in final_accounts.items():
                    state["results"][client_id][account][final_account] = state["results"][client_id][account].get(final_account, 0) + transaction_amount

        elif entry["type"] == "eof_count":
            nodo_id = entry["nodo_id"]
            state["workers"].setdefault(client_id, set()).add(nodo_id)
            
        elif entry["type"] == "eof_done":
            state["workers"].pop(client_id, None)
            state["results"].pop(client_id, None)

    def _process_data(self, result, msg_id):
        try:
            client_id = result.get("client_id")
            sus_accounts = result.get("suspicious_accounts")
            if client_id not in  self.results.keys():
                self.results[client_id] = {}
            for account, final_accounts in sus_accounts.items():
                if account not in self.results[client_id].keys():
                    self.results[client_id][account] = {}
                for final_account, transaction_amount in final_accounts.items():
                    self.results[client_id][account][final_account]=self.results[client_id][account].get(final_account, 0) + transaction_amount
            self.wal.append(msg_id, {"type": "data", "client_id": client_id, "sus_accounts": sus_accounts})
        except Exception as e:
            logging.error(f"PROCESS DATA ERROR: {e}")

    def _process_eof(self, eof_message, msg_id):
        try:
            client_id = eof_message["client_id"]
            nodo_id = eof_message["nodo_id"]

            if client_id not in self.worker_finished_with_client:
                self.worker_finished_with_client[client_id] = set()
            self.worker_finished_with_client[client_id].add(nodo_id)
            self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "nodo_id": nodo_id})

            if len(self.worker_finished_with_client[client_id]) == Q4_SCATTER_AMOUNT:
                path = RESULTS_STORAGE + f"{client_id}.csv"
                results = []
                for account, final_accounts in self.results[client_id].items():
                    for final_account, transaction_amount in final_accounts.items():
                        origin = account.split(',')
                        dest =final_account.split(',')
                        if transaction_amount >= 5:
                            message = {
                                "from_bank":origin[0],
                                "from_account":origin[1],
                                "to_bank" : dest[0],
                                "to_account":dest[1]
                            }
                            self.output_queue.send(message_protocol.internal.serialize([client_id, "q4", [message]]))
                self.output_queue.send(message_protocol.internal.serialize([client_id, "q4"]))
                logging.info(f"Q4 RESULTS SENT for client {client_id}")
                self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})
                del self.worker_finished_with_client[client_id]
                self.results.pop(client_id, None)
        except Exception as e:
            logging.error(f"EOF ERROR: {e}")

    def process_message(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            deserialized = message_protocol.internal.deserialize(message)
            if "nodo_id" in deserialized.keys():  # EOF
                self._process_eof(deserialized, msg_id)
            else:
                self._process_data(deserialized, msg_id)
            current_state = {
                    "workers": self.worker_finished_with_client, 
                    "results": self.results, 
                    "__msg_counters": middleware.get_msg_id_counters() if hasattr(middleware, 'get_msg_id_counters') else {}
                }
            self.wal.checkpoint(current_state)
            ack()
        except Exception as e:
            logging.error(f"error: {e}")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
            heartbeat.start()
        self.input_queue.start_consuming(self.process_message)


    def stop(self):
        self.input_queue.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_queue.close()
        self.output_queue.close()

def main():
    try:
        logging.basicConfig(level=logging.INFO)
        aggregator = AggregatorQ4()
        signal.signal(signal.SIGTERM, lambda signum, frame: aggregator.stop())
        aggregator.start()
        aggregator.close()
        return 0
    except Exception as e:
        logging.error(f"error: {e}")

if __name__ == "__main__":
    main()