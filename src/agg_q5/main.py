import os
import logging
import signal
import time

from common import middleware, message_protocol,heartbeat
from common.client_state_ttl import ClientStateTTL
from common.wal import WAL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q5_FILTER_AMOUNT = int(os.environ["Q5_FILTER_AMOUNT"])
Q5_FILTER_PREFIX = os.environ["Q5_FILTER_PREFIX"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]
ID = int(os.environ.get("ID", "0"))
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/agg_q5_{ID}")

class AggregatorQ5:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.count = {}
        self.worker_finished_with_client = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        default_state = {"workers": {}, "count": {}, "__msg_counters": {}}        
        loaded_state, _, _ = self.wal.backup_load(default=(default_state, 0, set()))
        loaded_state, _, _ = self.wal.backup_load(default=(default_state, 0, set()))
        
        
        if not isinstance(loaded_state, dict):
            state = default_state
        else:
            state = loaded_state
            state.setdefault("workers", {})
            state.setdefault("count", {})
            state.setdefault("__msg_counters", {})
        self.worker_finished_with_client = state["workers"]
        self.count = state["count"]
        middleware._init_msg_id_counters(state["__msg_counters"])
        self._orphans = self.wal.recover(self._wal_apply, state)
        self.client_state_ttl = ClientStateTTL()

    @staticmethod
    def _wal_apply(entry, state):
        client_id = entry["client_id"]
        if entry["type"] == "count":
            state["count"][client_id] = state["count"].get(client_id, 0) + 1
        elif entry["type"] == "eof_count":
            nodo_id = entry["nodo_id"]
            state["workers"].setdefault(client_id, set()).add(nodo_id)
        elif entry["type"] == "eof_done":
            state["workers"].pop(client_id, None)
            state["count"].pop(client_id, None)

    def _expire_client_state(self, client_id):
        logging.info(
            f"Client {client_id} expired after {self.client_state_ttl.ttl_seconds} seconds without updates; dropping state"
        )
        self.count.pop(client_id, None)
        self.worker_finished_with_client.pop(client_id, None)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

    def _process_data(self, transaction: dict, msg_id):
        client_id = transaction.pop("client_id")
        self._cleanup_expired_clients()
        self._update_last_seen(client_id)
        self.count[client_id] = self.count.get(client_id, 0) + 1
        self.wal.append(msg_id, {"type": "count", "client_id": client_id, "count_value":f"{self.count[client_id]}"})
        logging.info(f"Processed transaction for client {client_id}. Current count: {self.count[client_id]}")

    def _process_eof(self, eof_message, msg_id):
        client_id = eof_message["client_id"]
        nodo_id = eof_message["nodo_id"]
        logging.info(f"Processing EOF for client {client_id} and node {nodo_id}")
        self._cleanup_expired_clients()
        self._update_last_seen(client_id)
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "nodo_id": nodo_id})
        if len(self.worker_finished_with_client[client_id]) == Q5_FILTER_AMOUNT:
            count = self.count.pop(client_id, 0)
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q5", [{"count": count}]])
            )
            del self.worker_finished_with_client[client_id]
            self.client_state_ttl.remove(client_id)
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q5"])
            )
            self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:

            deserialized_message = message_protocol.internal.deserialize(message)
            logging.debug(f"Received message: {deserialized_message}")
            if len(deserialized_message) == 2:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message, msg_id)
            if msg_id:
                    self.wal.processed_ids.add(msg_id)
            current_state = {
                    "workers": self.worker_finished_with_client, 
                    "count": self.count, 
                    "__msg_counters": middleware.get_msg_id_counters()
                }
            self.wal.checkpoint(current_state)
            
            ack()
        except Exception as e:
            logging.info(f"error: {e}")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
                heartbeat.start()
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        self.input_queue.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()
        self.client_state_ttl.clear()

    def close(self):
        self.wal.close()
        self.input_queue.close()
        self.output_queue.close()


def main():
    try:
        logging.basicConfig(level=logging.INFO)
        aggregator = AggregatorQ5()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: aggregator.stop(),
        )
        aggregator.start()
        aggregator.close()
        return 0
    except Exception:
        logging.exception(f"An error occurred while running the {Q5_FILTER_PREFIX} aggregator")


if __name__ == "__main__":
    main()
