import os
import logging
import signal
import csv
import time

from common import middleware, message_protocol, heartbeat
from common.client_state_ttl import ClientStateTTL

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q1_FILTER_AMOUNT = int(os.environ["Q1_FILTER_AMOUNT"])
Q1_FILTER_PREFIX = os.environ["Q1_FILTER_PREFIX"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]


class JoinFilterQ1:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.results = {}
        self.worker_finished_with_client = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.client_state_ttl = ClientStateTTL()

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

    def _process_data(self, transaction: dict):
        client_id = transaction.pop("client_id")
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

    def _process_eof(self, eof_message):
        client_id = eof_message["client_id"]
        nodo_id = eof_message["nodo_id"]
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        if len(self.worker_finished_with_client[client_id]) == Q1_FILTER_AMOUNT:
            results = self.results.pop(client_id, [])
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q1"])
            )
            del self.worker_finished_with_client[client_id]
            self.client_state_ttl.remove(client_id)
            logging.info(f"finished processing EOF of {client_id} sent results to gateway")

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        if len(deserialized_message) == 2:
            self._process_eof(deserialized_message)
        else:
            self._process_data(deserialized_message)
        ack()

    def start(self):
        try:
            for heartbeat in self.heartbeats:
                heartbeat.start()
            self.input_queue.start_consuming(self.process_messsage)
        except Exception as e:
            logging.exception(f"Error consuming messages: {e}")

    def stop(self):
        self.input_queue.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()
        self.client_state_ttl.clear()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()


def main():
    try:
        logging.basicConfig(level=logging.INFO)
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
