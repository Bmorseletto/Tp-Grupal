import os
import logging
import signal

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/{FILTER_PREFIX}_{ID}")


class AvgCalculator:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"], ID
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, OUTPUT_PREFIX, [], ID, publish_only=True,
            source_id=f"AvgCalc_{ID}",
        )
        self.transactions_per_payment_format = {}
        self.eof_count = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        state, _, _ = self.wal.backup_load(default=({"data": {}, "eof": {}, "__msg_counters": {}}, 0, set()))
        self.transactions_per_payment_format = {
            str(k): {str(pf): v for pf, v in pfs.items()}
            for k, pfs in state["data"].items()
        }
        state["data"] = self.transactions_per_payment_format
        self.eof_count = {str(k): v for k, v in state["eof"].items()}
        state["eof"] = self.eof_count
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)
        for orphan in self._orphans:
            if orphan.startswith("avg_"):
                self.wal.tx_commit(orphan)
        for cid in list(self.eof_count):
            self._try_send(cid)

    @staticmethod
    def _wal_apply(entry, state):
        cid = str(entry["client_id"])
        if entry["type"] == "data_update":
            pf = str(entry["payment_format"])
            state["data"].setdefault(cid, {}).setdefault(pf, {"transactions": 0, "total amount paid": 0})
            state["data"][cid][pf]["transactions"] += entry["transactions"]
            state["data"][cid][pf]["total amount paid"] += entry["total_amount"]
        elif entry["type"] == "eof_count":
            state["eof"][cid] = entry["count"]

    def _try_send(self, client_id):
        if self.eof_count.get(client_id, 0) < UPSTREAM_AMOUNT:
            return
        results = {}
        if client_id in self.transactions_per_payment_format:
            for payment_format, data in self.transactions_per_payment_format[client_id].items():
                results[payment_format] = data["total amount paid"] / data["transactions"]
        logging.info(f"AVG RESULTS: client_id: {client_id}, results: {results}")
        self.wal.tx_begin(f"avg_{client_id}")
        self.output_exchange.send_by_key(
            message_protocol.internal.serialize({"nodo_id": ID, "client_id": int(client_id), "avg": results}),
            OUTPUT_PREFIX,
        )
        self.wal.tx_commit(f"avg_{client_id}")

    def _process_data(self, transaction, msg_id=None):
        payment_format = str(transaction["payment_format"])
        client_id = str(transaction["client_id"])
        if client_id not in self.transactions_per_payment_format:
            self.transactions_per_payment_format[client_id] = {}
        if payment_format not in self.transactions_per_payment_format[client_id]:
            self.transactions_per_payment_format[client_id][payment_format] = {"transactions": 0, "total amount paid": 0}
        pf_data = self.transactions_per_payment_format[client_id][payment_format]
        pf_data["transactions"] += 1
        pf_data["total amount paid"] += transaction["amount_paid"]
        self.wal.append(msg_id, {
            "type": "data_update",
            "client_id": client_id,
            "payment_format": payment_format,
            "transactions": 1,
            "total_amount": transaction["amount_paid"],
        })

    def _process_eof(self, deserialized_message, msg_id=None):
        client_id = str(deserialized_message["client_id"])
        current_count = self.eof_count.get(client_id, 0)
        if current_count >= UPSTREAM_AMOUNT:
            return
        self.eof_count[client_id] = current_count + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        self._try_send(client_id)

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
                self.wal.checkpoint({
                    "data": self.transactions_per_payment_format,
                    "eof": self.eof_count,
                    "__msg_counters": middleware.get_msg_id_counters(),
                })
            ack()
        except Exception:
            logging.exception("error processing message")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
            heartbeat.start()
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        self.wal.backup_save({
            "data": self.transactions_per_payment_format,
            "eof": self.eof_count,
            "__msg_counters": middleware.get_msg_id_counters(),
        }, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        self.output_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    avg_calculator = AvgCalculator()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: avg_calculator.stop(),
    )
    avg_calculator.start()
    avg_calculator.close()
    return 0


if __name__ == "__main__":
    main()
