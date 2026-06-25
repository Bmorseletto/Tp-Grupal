import os
import logging
import signal

from common import middleware, message_protocol, heartbeat
from common.wal import WAL
import csv
import time

from common import middleware, message_protocol, heartbeat
from common.client_state_ttl import ClientStateTTL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q2_FILTER_AMOUNT = int(os.environ["Q2_FILTER_AMOUNT"])
Q2_FILTER_PREFIX = os.environ["Q2_FILTER_PREFIX"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
ID = int(os.environ.get("ID", "0"))
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/agg_q2_{ID}")


class JoinFilterQ2:

    def __init__(self):
        self.input_queue = middleware.MultiQueueConsumer(MOM_HOST)
        self.input_queue.add_queue(INPUT_QUEUE, self._on_transaction_message)
        accounts_queue_name = INPUT_QUEUE + "_accounts"
        self.input_queue.add_queue(accounts_queue_name, self._on_accounts_message)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE, source_id=f"AggQ2_{ID}"
        )
        self.results = {}
        self.worker_finished_with_client = {}
        self.banks = {}
        self.clients_accounts_eof = set()
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.client_state_ttl = ClientStateTTL()
        self.wal = WAL(WAL_DIR)
        self._recover_state()
        
    def _recover_state(self):
        state, _, _ = self.wal.backup_load(default=({"results": {}, "workers": {}, "banks": {}, "accounts_eof": [], "__msg_counters": {}}, 0, set()))
        self.results = {str(k): {str(bk): v for bk, v in rd.items()} for k, rd in state["results"].items()}
        state["results"] = self.results
        self.worker_finished_with_client = {str(k): v for k, v in state["workers"].items()}
        state["workers"] = self.worker_finished_with_client
        self.banks = {str(k): v for k, v in state["banks"].items()}
        state["banks"] = self.banks
        self.clients_accounts_eof = set(str(c) for c in state["accounts_eof"])
        state["accounts_eof"] = list(self.clients_accounts_eof)
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)
        self.clients_accounts_eof = set(str(c) for c in state["accounts_eof"])
        for cid in list(self.clients_accounts_eof):
            self._try_send_results(cid)

    def _expire_client_state(self, client_id):
        logging.info(
            f"Client {client_id} expired after {self.client_state_ttl.ttl_seconds} seconds without updates; dropping state"
        )
        self.results.pop(client_id, None)
        self.worker_finished_with_client.pop(client_id, None)
        self.clients_accounts_eof.discard(client_id)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

    @staticmethod
    def _wal_apply(entry, state):
        if entry["type"] == "transaction_result":
            cid = str(entry["client_id"])
            nid = entry["nodo_id"]
            state["workers"].setdefault(cid, set()).add(nid)
            results_dict = state["results"].setdefault(cid, {})
            for r in entry["results"]:
                results_dict[str(r["from_bank"])] = r
        elif entry["type"] == "bank_map":
            state["banks"][str(entry["bank_id"])] = entry["bank_name"]
        elif entry["type"] == "accounts_eof":
            cid = str(entry["client_id"])
            if cid not in state["accounts_eof"]:
                state["accounts_eof"].append(cid)
        elif entry["type"] == "results_sent":
            cid = str(entry["client_id"])
            state["results"].pop(cid, None)
            state["workers"].pop(cid, None)
            state["accounts_eof"] = [c for c in state["accounts_eof"] if c != cid]

    def _all_banks_available(self, client_id):
        for r in self.results.get(client_id, {}).values():
            if str(r["from_bank"]) not in self.banks:
                return False
        return True

    def _try_send_results(self, client_id, msg_id=None):
        if client_id not in self.worker_finished_with_client:
            return
        if len(self.worker_finished_with_client[client_id]) != Q2_FILTER_AMOUNT:
            return
        if client_id not in self.clients_accounts_eof:
            return
        if not self._all_banks_available(client_id):
            logging.warning(f"Deferring results for client {client_id}: not all bank names available yet")
            return
        self._send_results(client_id, msg_id)

    def _process_transaction(self, transaction_message, msg_id=None):
        client_id = transaction_message["client_id"]
        self._cleanup_expired_clients()
        self._update_last_seen(client_id)
        cid = str(client_id)
        nodo_id = transaction_message["nodo_id"]
        results = transaction_message["results"]
        self.worker_finished_with_client.setdefault(cid, set()).add(nodo_id)
        logging.info(f"Received transaction results for client {cid} from nodo {nodo_id}")
        results_dict = self.results.setdefault(cid, {})
        for r in results:
            results_dict[str(r["from_bank"])] = r
        self.wal.append(msg_id, {"type": "transaction_result", "client_id": cid, "nodo_id": nodo_id, "results": results})
        self._try_send_results(cid, msg_id)

    def _send_results(self, client_id, msg_id=None):
        results = self._relate_bank_id_bank_name(client_id)
        logging.info(f"Sending {len(results)} results to {OUTPUT_QUEUE}")
        self.wal.tx_begin(f"results_{client_id}")
        for result in results:
            self.output_queue.send(message_protocol.internal.serialize([int(client_id), "q2", [result]]))
        self.output_queue.send(message_protocol.internal.serialize([int(client_id), "q2"]))
        self.wal.tx_commit(f"results_{client_id}")
        self.results.pop(client_id, None)
        del self.worker_finished_with_client[client_id]
        self.clients_accounts_eof.discard(client_id)
        self.wal.append(msg_id, {"type": "results_sent", "client_id": client_id})
        self.client_state_ttl.remove(client_id)
        logging.info(f"finished processing EOF of {client_id} sent results to join")

    def _relate_bank_id_bank_name(self, client_id):
        enriched = []
        for r in self.results.get(client_id, {}).values():
            enriched.append({
                "account": r["account"],
                "amount_paid": r["amount_paid"],
                "from_bank": self.banks.get(str(r["from_bank"]), r["from_bank"]),
            })
        return enriched

    def _on_transaction_message(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            self._process_transaction(deserialized_message, msg_id)
            if msg_id:
                self.wal.processed_ids.add(msg_id)
            self.wal.checkpoint({"results": self.results, "workers": self.worker_finished_with_client, "banks": self.banks, "accounts_eof": list(self.clients_accounts_eof), "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("An error occurred while processing a transaction message")
            nack()

    def _on_accounts_message(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            if isinstance(deserialized_message, list):
                client_id = deserialized_message[0]
                cid = str(client_id)
                self.clients_accounts_eof.add(cid)
                self.wal.append(msg_id, {"type": "accounts_eof", "client_id": cid})
                self._try_send_results(cid, msg_id)
                self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)
                self.client_state_ttl.update_last_seen(client_id)
                # if client_id in self.worker_finished_with_client and len(self.worker_finished_with_client[client_id]) == Q2_FILTER_AMOUNT:
                #     self._send_results(client_id)
            else:
                self.banks[str(deserialized_message["bank_id"])] = deserialized_message["bank_name"]
                self.wal.append(msg_id, {"type": "bank_map", "bank_id": str(deserialized_message["bank_id"]), "bank_name": deserialized_message["bank_name"]})
                for cid in list(self.clients_accounts_eof):
                    self._try_send_results(cid, msg_id)            
            if msg_id:
                self.wal.processed_ids.add(msg_id)
            self.wal.checkpoint({"results": self.results, "workers": self.worker_finished_with_client, "banks": self.banks, "accounts_eof": list(self.clients_accounts_eof), "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("An error occurred while processing an accounts message")
            nack()

    def start(self):
        try:
            for heartbeat in self.heartbeats:
                heartbeat.start()
            self.input_queue.start_consuming()
        except Exception as e:
            logging.exception(f"Error consuming messages: {e}")
            raise

    def stop(self):
        self.wal.backup_save({"results": self.results, "workers": self.worker_finished_with_client, "banks": self.banks, "accounts_eof": list(self.clients_accounts_eof), "__msg_counters": middleware.get_msg_id_counters()}, self.wal.last_seq())
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
        join_filter = JoinFilterQ2()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: join_filter.stop(),
        )
        join_filter.start()
        join_filter.close()
        return 0
    except Exception:
        logging.exception(f"An error occurred while running the {Q2_FILTER_PREFIX} filter")


if __name__ == "__main__":
    main()
