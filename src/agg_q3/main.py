import os
import logging
import signal

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q3_FILTER_AMOUNT = int(os.environ["Q3_FILTER_AMOUNT"])
Q3_FILTER_PREFIX = os.environ["Q3_FILTER_PREFIX"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
ID = int(os.environ.get("ID", "0"))
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/agg_q3_{ID}")


class JoinFilterQ3:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, source_id=f"AggQ3_{ID}"
        )
        self.worker_finished_with_client = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        state, _, _ = self.wal.backup_load(default=({"workers": {}, "__msg_counters": {}}, 0, set()))
        self.worker_finished_with_client = {str(k): set(v) for k, v in state["workers"].items()}
        state["workers"] = self.worker_finished_with_client
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)
        for orphan in self._orphans:
            if orphan.startswith("results_"):
                cid = orphan[len("results_"):]
                self.worker_finished_with_client.pop(cid, None)
                self.wal.tx_commit(orphan)
                self.wal.append(None, {"type": "eof_done", "client_id": cid})
        for cid in list(self.worker_finished_with_client):
            if len(self.worker_finished_with_client[cid]) == Q3_FILTER_AMOUNT:
                self.wal.tx_begin(f"results_{cid}")
                self.output_queue.send(message_protocol.internal.serialize([int(cid), "q3"]))
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

    def _process_data(self, transaction):
        client_id = transaction.get("client_id")
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, "q3", [{
                "from_bank": transaction.get("from_bank", ""),
                "account": transaction.get("account", ""),
                "amount_paid": transaction.get("amount_paid", ""),
                "payment_format": transaction.get("payment_format", ""),
            }]])
        )

    def _process_eof(self, eof_message, msg_id=None):
        client_id = str(eof_message["client_id"])
        nodo_id = eof_message["nodo_id"]
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "nodo_id": nodo_id})
        if len(self.worker_finished_with_client[client_id]) == Q3_FILTER_AMOUNT:
            self.wal.tx_begin(f"results_{client_id}")
            self.output_queue.send(message_protocol.internal.serialize([int(client_id), "q3"]))
            self.wal.tx_commit(f"results_{client_id}")
            del self.worker_finished_with_client[client_id]
            self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})
            logging.info(f"finished processing EOF of {client_id} sent results to join")

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        deserialized_message = message_protocol.internal.deserialize(message)
        is_eof = "results" not in deserialized_message
        if is_eof and msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            if is_eof:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message["results"])
            if is_eof and msg_id:
                self.wal.processed_ids.add(msg_id)
            if self.wal.is_checkpoint_necessary():
                self.wal.checkpoint({"workers": self.worker_finished_with_client, "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("error processing message")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
            heartbeat.start()
        self.input_queue.start_consuming(self.process_messsage)

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
        logging.basicConfig(level=logging.INFO)
        join_filter = JoinFilterQ3()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: join_filter.stop(),
        )
        join_filter.start()
        join_filter.close()
        return 0
    except Exception:
        logging.exception(f"An error occurred while running the {Q3_FILTER_PREFIX} aggregator")


if __name__ == "__main__":
    main()
