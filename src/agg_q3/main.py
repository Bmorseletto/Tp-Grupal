import os
import logging
import signal
import time
# import csv


from common import middleware, message_protocol, heartbeat

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q3_FILTER_AMOUNT = int(os.environ["Q3_FILTER_AMOUNT"])
Q3_FILTER_PREFIX = os.environ["Q3_FILTER_PREFIX"]
RESULTS_STORAGE = "/output/q3_"
AVG_STORAGE = "/output/q3_avg_"
TRANSACTION_STORAGE = "/output/q3_transaction_"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]
CLIENT_STATE_TTL_SECONDS = int(os.environ.get("CLIENT_STATE_TTL_SECONDS", "300"))

class JoinFilterQ3:

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
        self.last_seen = {}

    def _cleanup_expired_clients(self):
        now = time.time()
        expired_clients = [
            client_id
            for client_id, last_seen in self.last_seen.items()
            if now - last_seen > CLIENT_STATE_TTL_SECONDS
        ]
        for client_id in expired_clients:
            logging.info(
                f"Client {client_id} expired after {CLIENT_STATE_TTL_SECONDS} seconds without updates; dropping state"
            )
            self.results.pop(client_id, None)
            self.worker_finished_with_client.pop(client_id, None)
            self.last_seen.pop(client_id, None)

    def _update_last_seen(self, client_id):
        self.last_seen[client_id] = time.time()

    def _process_data(self, transaction):
        try:
            client_id = transaction.pop("client_id")
            # with open(RESULTS_STORAGE+f"{client_id}.csv", "a") as csvfile:
            #     csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            #     csv_writer.writerow(transaction.values())
            #     logging.info(f"writing {transaction} down")
            self._cleanup_expired_clients()
            self._update_last_seen(client_id)
            self.worker_finished_with_client.setdefault(client_id, set())
            if client_id not in self.results:
                self.results[client_id] = []
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q3", [{
                "from_bank":transaction.get("from_bank", ""),
                "account": transaction.get("account", ""),
                "amount_paid": transaction.get("amount_paid", ""),
                "payment_format": transaction.get("payment_format", ""),
            }]])
            )
        except Exception as e:
            logging.error(f"ERROR: {e}")

    def _process_eof(self, eof_message):
        try:
            client_id = eof_message["client_id"]
            nodo_id = eof_message["nodo_id"]
            self._cleanup_expired_clients()
            self._update_last_seen(client_id)
            self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)

            if len(self.worker_finished_with_client[client_id]) == Q3_FILTER_AMOUNT:
                results = sorted(self.results.pop(client_id, []), key=lambda x: x['payment_format'])
                self.output_queue.send(message_protocol.internal.serialize([client_id, "q3"]))
                del self.worker_finished_with_client[client_id]
                self.last_seen.pop(client_id, None)
                avg_path = AVG_STORAGE + f"{client_id}.csv"
                if os.path.isfile(avg_path):
                    os.remove(avg_path)
                logging.info(f"finished processing EOF of {client_id} sent results to join")
        except Exception as e:
            logging.error(f"ERROR: {e}")

    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        if len(desiriized_message) == 2:
            self._process_eof(desiriized_message)
        else:
            self._process_data(desiriized_message["results"])
        ack()

    def start(self):
        for heartbeat in self.heartbeats:
                heartbeat.start()
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        self.input_queue.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()
        self.last_seen.clear()

    def close(self):
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
    except Exception as e:
        logging.error(f"error: {e}")


if __name__ == "__main__":
    main()
