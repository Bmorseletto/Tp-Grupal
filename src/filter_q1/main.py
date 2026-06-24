import os
import logging
import signal
import socket
import time

from common import middleware, message_protocol,heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/filter_q1_{ID}")

DONE = True
WORKING = False
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]


class DollarAmtFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"], ID
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, source_id=f"DollarAmtFilter_{ID}"
        )
        self.eof_count = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
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

    def _process_data(self, transaction, msg_id=None):
        if transaction["amount_paid"] < 50:
            output = {
                "client_id": transaction["client_id"],
                "from_bank":transaction.get("from_bank", ""),
                "account": transaction["account"],
                "to_bank":transaction.get("to_bank", ""),
                "to_account": transaction["to_account"],
                "amount_paid": transaction["amount_paid"],
            }
            self.output_queue.send(message_protocol.internal.serialize(output))

    def _process_eof(self, deserialized_message, msg_id=None):
        client_id = deserialized_message["client_id"]
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        self.output_queue.send(
            message_protocol.internal.serialize(
                {"nodo_id": ID, "client_id": client_id}
            )
        )
        self.eof_count.pop(client_id, None)
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
            self.wal.checkpoint({"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("error processing message")
            nack()



    def start(self):
        try:
            for heartbeat in self.heartbeats:
                heartbeat.start()
            self.input_exchange.start_consuming(self.process_messsage)
        except Exception as e:
            logging.exception(f"Error consuming messages: {e}")

    def stop(self):
        state = {"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()}
        self.wal.backup_save(state, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        self.output_queue.close()



def main():
    logging.basicConfig(level=logging.INFO)
    dollar_amt_filter = DollarAmtFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: dollar_amt_filter.stop(),
    )
    dollar_amt_filter.start()
    dollar_amt_filter.close()
    return 0


if __name__ == "__main__":
    main()
