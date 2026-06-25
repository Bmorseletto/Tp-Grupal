import os
import logging
import signal
import time

from common import middleware, message_protocol, heartbeat
from common.client_state_ttl import ClientStateTTL
from common.wal import WAL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q1_FILTER_AMOUNT = int(os.environ["Q1_FILTER_AMOUNT"])
Q1_FILTER_PREFIX = os.environ["Q1_FILTER_PREFIX"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
ID = int(os.environ.get("ID", "0"))
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/agg_q1_{ID}")


class JoinFilterQ1:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.worker_finished_with_client = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        self.client_state_ttl = ClientStateTTL()
        state, _, _ = self.wal.backup_load(default=({"workers": {}, "__msg_counters": {}}, 0, set()))
        self.worker_finished_with_client = {str(k): v for k, v in state["workers"].items()}
        state["workers"] = self.worker_finished_with_client
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)
        for cid in list(self.worker_finished_with_client):
            if len(self.worker_finished_with_client[cid]) == Q1_FILTER_AMOUNT:
                self.wal.tx_begin(f"results_{cid}")
                self.output_queue.send(
                    message_protocol.internal.serialize([int(cid), "q1"])
                )
                self.wal.tx_commit(f"results_{cid}")
                del self.worker_finished_with_client[cid]
                self.wal.append(None, {"type": "eof_done", "client_id": cid})

    @staticmethod
    def _wal_apply(entry, state):
        cid = str(entry["client_id"])
        if entry["type"] == "eof_count":
            state["workers"].setdefault(cid, set())
            state["workers"][cid].add(entry["nodo_id"])
        elif entry["type"] == "eof_done":
            state["workers"].pop(cid, None)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

    def _expire_client_state(self, client_id):
        logging.info(
            f"Client {client_id} expired after {self.client_state_ttl.ttl_seconds} seconds without updates; dropping state"
        )
        self.results.pop(client_id, None)
        self.worker_finished_with_client.pop(client_id, None)

    def _process_data(self, transaction: dict, msg_id=None):
        client_id = transaction.pop("client_id")
        cid = str(client_id)
        self.worker_finished_with_client.setdefault(cid, set())
        self._cleanup_expired_clients()
        self._update_last_seen(client_id)
        self.worker_finished_with_client.setdefault(client_id, set())
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, "q1", [{
                "from_bank": transaction.get("from_bank", ""),
                "account": transaction.get("account", ""),
                "to_bank": transaction.get("to_bank", ""),
                "to_account": transaction.get("to_account", ""),
                "amount_paid": transaction.get("amount_paid", ""),
            }]])
        )

    def _process_eof(self, eof_message, msg_id=None):
        client_id = eof_message["client_id"]
        cid = str(client_id)
        nodo_id = eof_message["nodo_id"]
        self.worker_finished_with_client.setdefault(cid, set()).add(nodo_id)
        self.wal.append(msg_id, {"type": "eof_count", "client_id": cid, "nodo_id": nodo_id})
        if len(self.worker_finished_with_client[cid]) == Q1_FILTER_AMOUNT:
            self.wal.tx_begin(f"results_{cid}")
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q1"])
            )
            self.wal.tx_commit(f"results_{cid}")
            del self.worker_finished_with_client[cid]
            self.client_state_ttl.remove(client_id)
            self.wal.append(msg_id, {"type": "eof_done", "client_id": cid})

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        deserialized_message = message_protocol.internal.deserialize(message)
        is_eof = len(deserialized_message) == 2
        if is_eof and msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            if is_eof:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message, msg_id)
            if is_eof and msg_id:
                self.wal.processed_ids.add(msg_id)
            self.wal.checkpoint({"workers": self.worker_finished_with_client, "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("error processing message")
            nack()

    def start(self):
        try:
            for heartbeat in self.heartbeats:
                heartbeat.start()
            self.input_queue.start_consuming(self.process_messsage)
        except Exception as e:
            logging.exception(f"Error consuming messages: {e}")

    def stop(self):
        self.wal.backup_save({"workers": self.worker_finished_with_client, "__msg_counters": middleware.get_msg_id_counters()}, self.wal.last_seq())
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
        logging.basicConfig(level=logging.WARNING)
        join_filter = JoinFilterQ1()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: join_filter.stop(),
        )
        join_filter.start()
        join_filter.close()
        return 0
    except Exception:
        logging.exception(f"An error occurred while running the {Q1_FILTER_AMOUNT} filter")


if __name__ == "__main__":
    main()
