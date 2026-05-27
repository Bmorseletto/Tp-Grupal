import os
import logging
import signal
import csv

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q2_FILTER_AMOUNT = int(os.environ["Q2_FILTER_AMOUNT"])
Q2_FILTER_PREFIX = os.environ["Q2_FILTER_PREFIX"]
ACCOUNTS_FILE = os.environ["ACCOUNTS_FILE"]
PATH_TRANSACTIONS = "/output/q2_transaction_"


class JoinFilterQ2:

    def __init__(self):
        logging.info("starting JoinFilterQ2")
        self.input_queue = middleware.MultiQueueConsumer(MOM_HOST)
        self.input_queue.add_queue(INPUT_QUEUE, self._on_transaction_message)
        accounts_queue_name = INPUT_QUEUE + "_accounts"
        self.input_queue.add_queue(accounts_queue_name, self._on_accounts_message)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.worker_finished_with_client = {}
        self.acc_number_to_bank_name = {}
        self.clients_accounts_eof = set()
        logging.info("started JoinFilterQ2")

    def _process_transaction(self, transaction_message):
        client_id = transaction_message["client_id"]
        nodo_id = transaction_message["nodo_id"]
        results = transaction_message["results"]
        logging.info(f"processing EOF of {client_id} from filter {nodo_id}")
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        with open(PATH_TRANSACTIONS + f"{client_id}.csv", "a") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            for result in results:
                csv_writer.writerow(result.values())
                logging.info(f"writing {result} down")
        if len(self.worker_finished_with_client[client_id]) == Q2_FILTER_AMOUNT and client_id in self.clients_accounts_eof:
            self._send_results(client_id)

    # def _process_eof(self, eof_message):
    #     client_id = eof_message["client_id"]
    #     nodo_id = eof_message["nodo_id"]
    #     logging.info(f"processing EOF of {client_id} from filter {nodo_id}")
    #     self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
    #     if len(self.worker_finished_with_client[client_id]) == Q2_FILTER_AMOUNT and client_id in self.clients_accounts_eof:
    #         self._send_results(client_id)

    def _send_results(self, client_id):
        results = self._relate_bank_id_bank_name(client_id)
        self.output_queue.send(message_protocol.internal.serialize([client_id, "q2", results]))
        csv_path = PATH_TRANSACTIONS + f"{client_id}.csv"
        if os.path.exists(csv_path):
            os.remove(csv_path)
        del self.worker_finished_with_client[client_id]
        self.clients_accounts_eof.discard(client_id)
        logging.info(f"finished processing EOF of {client_id} sent results to join")

    def _relate_bank_id_bank_name(self, client_id):
        with open(PATH_TRANSACTIONS + f"{client_id}.csv", "r", newline="") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            results = []
            for transaction in csv_reader:
                logging.info(f"saving transaction: {transaction}")
                values = {
                    "account": transaction[0],
                    "amount_paid": transaction[1],
                    "from_bank": self.acc_number_to_bank_name[transaction[0]],
                }
                results.append(values)
            return results

    def _on_transaction_message(self, message, ack, nack):
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            logging.info(f"transaction msg received {deserialized_message}")
            self._process_transaction(deserialized_message)
            ack()
        except Exception:
            logging.exception("An error occurred while processing a transaction message")
            nack()

    def _on_accounts_message(self, message, ack, nack):
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            logging.info(f"account msg received {deserialized_message}")
            if isinstance(deserialized_message, list):
                client_id = deserialized_message[0]
                self.clients_accounts_eof.add(client_id)
                logging.info(f"accounts EOF received for client {client_id}")
                # if client_id in self.worker_finished_with_client and len(self.worker_finished_with_client[client_id]) == Q2_FILTER_AMOUNT:
                #     self._send_results(client_id)
            else:
                self.acc_number_to_bank_name[deserialized_message["account_number"]] = deserialized_message["bank_name"]
            ack()
        except Exception:
            logging.exception("An error occurred while processing an accounts message")
            nack()

    def start(self):
        self.input_queue.start_consuming()

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
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