import os
import logging
import signal

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q1_FILTER_AMOUNT = int(os.environ["Q1_FILTER_AMOUNT"])
Q1_FILTER_PREFIX = os.environ["Q1_FILTER_PREFIX"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]

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
        state, _, _ = self.wal.backup_load(default=({"workers": {}, "__msg_counters": {}}, 0, set()))
        self.worker_finished_with_client = state["workers"]
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)

    @staticmethod
    def _wal_apply(entry, state):
        if entry["type"] == "eof_count":
            state["workers"].setdefault(entry["client_id"], set())
            state["workers"][entry["client_id"]].add(entry["nodo_id"])
        elif entry["type"] == "eof_done":
            state["workers"].pop(entry["client_id"], None)

    def _process_data(self, transaction: dict, msg_id=None):
        client_id = transaction.pop("client_id")
        self.worker_finished_with_client.setdefault(client_id, set())
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, "q1", [{
            "from_bank":transaction.get("from_bank", ""),
            "account": transaction.get("account", ""),
            "to_bank":transaction.get("to_bank", ""),
            "to_account": transaction.get("to_account", ""),
            "amount_paid": transaction.get("amount_paid", ""),
        }]])
        )

    def _process_eof(self, eof_message, msg_id=None):
        client_id = eof_message["client_id"]
        nodo_id = eof_message["nodo_id"]
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "nodo_id": nodo_id})
        if len(self.worker_finished_with_client[client_id]) == Q1_FILTER_AMOUNT:
            self.wal.tx_begin(f"results_{client_id}")
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q1"])
            )
            self.wal.tx_commit(f"results_{client_id}")
            del self.worker_finished_with_client[client_id]
            self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})

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
