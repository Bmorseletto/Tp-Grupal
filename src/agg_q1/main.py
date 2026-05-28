import os
import logging
import signal
import csv

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q1_FILTER_AMOUNT = int(os.environ["Q1_FILTER_AMOUNT"])
Q1_FILTER_PREFIX = os.environ["Q1_FILTER_PREFIX"]


class JoinFilterQ1:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.filtered_transactions = {}
        self.worker_finished_with_client = {}

    def _process_data(self, transaction: dict):
        logging.info(f"transaction {transaction}")
        client_id = transaction.pop("client_id")
        if client_id not in self.worker_finished_with_client.keys():
            logging.info(f"first time processing data of {client_id}")
            self.worker_finished_with_client[client_id] = set()
        logging.info(f"processing data OF {client_id}")
        with open(f"/output/q1_{client_id}.csv", "a") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            csv_writer.writerow(transaction.values())
            logging.info(f"writing {transaction} down")

    def _process_eof(self, eof_message):
        client_id = eof_message["client_id"]
        nodo_id = eof_message["nodo_id"]
        logging.info(f"processing EOF of {client_id} from filter {nodo_id}")
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        if len(self.worker_finished_with_client[client_id]) == Q1_FILTER_AMOUNT:
            csv_path = f"/output/q1_{client_id}.csv"
            if os.path.exists(csv_path):
                with open(csv_path, "r", newline="") as csvfile:
                    csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                    results = []
                    for transaction in csv_reader:
                        logging.info(f"sending transaction: {transaction}, to gateway")
                        values = {
                            "account": transaction[0],
                            "to_account": transaction[1],
                            "amount_paid": transaction[2],
                        }
                        results.append(values)
                os.remove(csv_path)
            else:
                results = []
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q1", results])
            )
            del self.worker_finished_with_client[client_id]
            logging.info(f"finished processing EOF of {client_id} sent results to gateway")

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        if len(deserialized_message) == 2:  # modificar
            self._process_eof(deserialized_message)
        else:
            self._process_data(deserialized_message)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        self.input_queue.stop_consuming()

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
        logging.exception(f"An error occurred while running the {Q1_FILTER_PREFIX} filter")


if __name__ == "__main__":
    main()
