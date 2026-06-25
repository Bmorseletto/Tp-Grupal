import os
import logging
import signal
import time

from common import middleware, message_protocol, heartbeat
from common.wal import WAL
from common.client_state_ttl import ClientStateTTL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
AVG_CALC_AMOUNT = int(os.environ["AVG_CALC_AMOUNT"])
DATE_FILTER_AMOUNT = int(os.environ["DATE_FILTER_AMOUNT"])
NODO_ID = "nodo_id"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/{FILTER_PREFIX}_{ID}")


class AvgFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"], ID
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, source_id=f"{FILTER_PREFIX}_{ID}"
        )
        self.avg_worker_finished_with_client = {}
        self.date_filter_finished_with_client = {}
        self.payment_formats_averages = {}
        self.transactions_per_client = {}
        self.client_state_ttl = ClientStateTTL()
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        state, _, _ = self.wal.backup_load(default=(
            {"transactions": {}, "averages": {}, "avg_workers": {}, "date_workers": {}, "__msg_counters": {}},
            0, set(),
        ))
        self.transactions_per_client = {
            str(k): {str(pf): txns for pf, txns in pfs.items()}
            for k, pfs in state["transactions"].items()
        }
        state["transactions"] = self.transactions_per_client
        self.payment_formats_averages = {
            str(k): {str(pf): avg for pf, avg in avs.items()}
            for k, avs in state["averages"].items()
        }
        state["averages"] = self.payment_formats_averages
        self.avg_worker_finished_with_client = {str(k): set(v) for k, v in state["avg_workers"].items()}
        state["avg_workers"] = self.avg_worker_finished_with_client
        self.date_filter_finished_with_client = {str(k): set(v) for k, v in state["date_workers"].items()}
        state["date_workers"] = self.date_filter_finished_with_client
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)
        for orphan in self._orphans:
            if orphan.startswith("results_"):
                self.wal.tx_commit(orphan)
        for cid in list(self.avg_worker_finished_with_client):
            self._try_send_results(cid)

    @staticmethod
    def _wal_apply(entry, state):
        cid = str(entry["client_id"])
        if entry["type"] == "transaction_add":
            pf = str(entry["payment_format"])
            state["transactions"].setdefault(cid, {}).setdefault(pf, [])
            state["transactions"][cid][pf].append(entry["transaction"])
        elif entry["type"] == "avg_eof":
            state["avg_workers"].setdefault(cid, set()).add(entry["nodo_id"])
            state["averages"].setdefault(cid, {}).update(entry["avg"])
        elif entry["type"] == "date_eof":
            state["date_workers"].setdefault(cid, set()).add(entry["nodo_id"])

    def _process_data(self, data, msg_id=None):
        client_id = str(data.pop("client_id"))
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)
        self.client_state_ttl.update_last_seen(client_id)
        payment_format = str(data.get("payment_format", ""))
        if client_id not in self.transactions_per_client:
            self.transactions_per_client[client_id] = {}
        if payment_format not in self.transactions_per_client[client_id]:
            self.transactions_per_client[client_id][payment_format] = []
        txn = {
            "client_id": int(client_id),
            "from_bank": data.get("from_bank", ""),
            "account": data.get("account", ""),
            "amount_paid": data.get("amount_paid", 0),
            "payment_format": payment_format,
        }
        self.transactions_per_client[client_id][payment_format].append(txn)
        self.wal.append(msg_id, {
            "type": "transaction_add",
            "client_id": client_id,
            "payment_format": payment_format,
            "transaction": txn,
        })

    def _process_eof(self, deserialized_message, msg_id=None):
        client_id = str(deserialized_message["client_id"])
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)
        self.client_state_ttl.update_last_seen(client_id)
        nodo_id = deserialized_message["nodo_id"]
        if "avg" in deserialized_message:
            self.avg_worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
            avg_data = {str(k): v for k, v in deserialized_message["avg"].items()}
            self.payment_formats_averages.setdefault(client_id, {}).update(avg_data)
            self.wal.append(msg_id, {
                "type": "avg_eof",
                "client_id": client_id,
                "nodo_id": nodo_id,
                "avg": avg_data,
            })
        else:
            self.date_filter_finished_with_client.setdefault(client_id, set()).add(nodo_id)
            self.wal.append(msg_id, {
                "type": "date_eof",
                "client_id": client_id,
                "nodo_id": nodo_id,
            })

        self._try_send_results(client_id, msg_id)
        self.client_state_ttl.remove(client_id)

    def _try_send_results(self, client_id, msg_id=None):
        if client_id not in self.avg_worker_finished_with_client:
            return
        if len(self.avg_worker_finished_with_client[client_id]) < AVG_CALC_AMOUNT:
            return
        if client_id not in self.date_filter_finished_with_client:
            return
        if len(self.date_filter_finished_with_client[client_id]) < DATE_FILTER_AMOUNT:
            return
        self._send_results(client_id, msg_id)

    def _send_results(self, client_id, msg_id=None):
        payment_formats_averages = self.payment_formats_averages.get(client_id, {})
        client_transactions = self.transactions_per_client.get(client_id, {})
        self.wal.tx_begin(f"results_{client_id}")
        for payment_format, average in payment_formats_averages.items():
            avg_threshold = float(average) / 100
            for transaction in client_transactions.get(payment_format, []):
                try:
                    if float(transaction["amount_paid"]) < avg_threshold:
                        self.output_queue.send(message_protocol.internal.serialize({"results": transaction}))
                except (TypeError, ValueError):
                    continue
        self.output_queue.send(message_protocol.internal.serialize({"nodo_id": ID, "client_id": int(client_id)}))
        self.wal.tx_commit(f"results_{client_id}")

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            if NODO_ID in deserialized_message:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message, msg_id)
            if msg_id:
                self.wal.processed_ids.add(msg_id)
            self.wal.checkpoint({
                "transactions": self.transactions_per_client,
                "averages": self.payment_formats_averages,
                "avg_workers": self.avg_worker_finished_with_client,
                "date_workers": self.date_filter_finished_with_client,
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
 
    def _expire_client_state(self, client_id):
        self.avg_worker_finished_with_client.pop(client_id, None)
        self.date_filter_finished_with_client.pop(client_id, None)
        self.payment_formats_averages.pop(client_id, None)
        self.transactions_per_client.pop(client_id, None)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

    def _expire_client_state(self, client_id):
        self.avg_worker_finished_with_client.pop(client_id, None)
        self.date_filter_finished_with_client.pop(client_id, None)
        self.payment_formats_averages.pop(client_id, None)
        self.transactions_per_client.pop(client_id, None)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

    def stop(self):
        self.wal.backup_save({
            "transactions": self.transactions_per_client,
            "averages": self.payment_formats_averages,
            "avg_workers": self.avg_worker_finished_with_client,
            "date_workers": self.date_filter_finished_with_client,
            "__msg_counters": middleware.get_msg_id_counters(),
        }, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()
        self.client_state_ttl.clear()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    avg_filter = AvgFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: avg_filter.stop(),
    )
    avg_filter.start()
    avg_filter.close()
    return 0


if __name__ == "__main__":
    main()
