import os
import logging
import signal

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
CLIENT_ID_KEY = "client_id"
BANK_KEY = "from_bank"
AMMOUNT_PAID_KEY = "amount_paid"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/filter_q2_{ID}")


class MaxTransactionFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"], ID
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, source_id=f"MaxTransFilter_{ID}"
        )
        self.max_transaction_per_bank = {}
        self.eof_count = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        state, _, _ = self.wal.backup_load(default=({"max": {}, "eof": {}, "__msg_counters": {}}, 0, set()))
        self.max_transaction_per_bank = {str(k): {str(b): v for b, v in banks.items()} for k, banks in state["max"].items()}
        state["max"] = self.max_transaction_per_bank
        self.eof_count = {str(k): v for k, v in state["eof"].items()}
        state["eof"] = self.eof_count
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)

    @staticmethod
    def _wal_apply(entry, state):
        cid = str(entry["client_id"])
        if entry["type"] == "max_update":
            bid = str(entry["bank_id"])
            state["max"].setdefault(cid, {})[bid] = entry["transaction"]
        elif entry["type"] == "eof_count":
            state["eof"][cid] = entry["count"]
        elif entry["type"] == "eof_done":
            state["eof"].pop(cid, None)
            state["max"].pop(cid, None)

    def _process_data(self, transaction, msg_id=None):
        client_id = str(transaction.pop(CLIENT_ID_KEY))
        bank_id = str(transaction[BANK_KEY])
        if client_id not in self.max_transaction_per_bank:
            self.max_transaction_per_bank[client_id] = {}
        if bank_id in self.max_transaction_per_bank[client_id]:
            if self.max_transaction_per_bank[client_id][bank_id][AMMOUNT_PAID_KEY] >= transaction[AMMOUNT_PAID_KEY]:
                return
        self.max_transaction_per_bank[client_id][bank_id] = transaction
        self.wal.append(msg_id, {"type": "max_update", "client_id": client_id, "bank_id": bank_id, "transaction": transaction})

    def _process_eof(self, deserialized_message, msg_id=None):
        client_id = str(deserialized_message["client_id"])
        current_count = self.eof_count.get(client_id, 0)
        if current_count >= UPSTREAM_AMOUNT:
            self.eof_count.pop(client_id, None)
            self.max_transaction_per_bank.pop(client_id, None)
            self.wal.append(msg_id, {"type": "eof_done", "client_id": client_id})
            return
        self.eof_count[client_id] = current_count + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        results = list(self.max_transaction_per_bank.get(client_id, {}).values())
        if results:
            self.output_queue.send(message_protocol.internal.serialize({"nodo_id": ID, CLIENT_ID_KEY: int(client_id), "results": results}))
        self.eof_count.pop(client_id, None)
        self.max_transaction_per_bank.pop(client_id, None)
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
            if self.wal.is_checkpoint_necessary():
                self.wal.checkpoint({"max": self.max_transaction_per_bank, "eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()})
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
        self.wal.backup_save({"max": self.max_transaction_per_bank, "eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()}, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    max_trans_filter = MaxTransactionFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: max_trans_filter.stop(),
    )
    max_trans_filter.start()
    max_trans_filter.close()
    return 0


if __name__ == "__main__":
    main()