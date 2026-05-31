import os
import logging
import signal
# import csv


from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q3_FILTER_AMOUNT = int(os.environ["Q3_FILTER_AMOUNT"])
Q3_FILTER_PREFIX = os.environ["Q3_FILTER_PREFIX"]
RESULTS_STORAGE = "/output/q3_"
AVG_STORAGE = "/output/q3_avg_"
TRANSACTION_STORAGE = "/output/q3_transaction_"

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

    def _process_data(self, transaction):
        try:
            client_id = transaction.pop("client_id")
            # with open(RESULTS_STORAGE+f"{client_id}.csv", "a") as csvfile:
            #     csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            #     csv_writer.writerow(transaction.values())
            #     logging.info(f"writing {transaction} down")
            self.worker_finished_with_client.setdefault(client_id, set())
            if client_id not in self.results:
                self.results[client_id] = []
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q3", [{
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
            self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)

            if len(self.worker_finished_with_client[client_id]) == Q3_FILTER_AMOUNT:
                # if os.path.isfile(RESULTS_STORAGE+f"{client_id}.csv"):
                #     with open(RESULTS_STORAGE+f"{client_id}.csv", "r", newline="") as csvfile:
                #         csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                #         for transaction in csv_reader:
                #             logging.info(f"sending transaction: {transaction}, to gateway")
                #             values = {
                #                 "account" : transaction[0],
                #                 "amount_paid" : transaction[1],
                #                 "payment_format":  transaction[2]
                #             }
                #             results.append(values)
                #     os.remove(RESULTS_STORAGE+f"{client_id}.csv")
                results = sorted(self.results.pop(client_id, []), key=lambda x: x['payment_format'])
                self.output_queue.send(message_protocol.internal.serialize([client_id, "q3"]))
                del self.worker_finished_with_client[client_id]
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
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        self.input_queue.stop_consuming()

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
