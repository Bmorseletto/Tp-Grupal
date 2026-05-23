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
ACCOUNTS_EOF = True
PATH_TRANSACTIONS = "/output/q2_transaction_"

class JoinFilterQ2:

    def __init__(self):
        logging.info("starting JoinFilterQ2")
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.max_transactions = {}
        self.worker_finished_with_client = {}
        logging.info("started JoinFilterQ2")


    def _process_transaction(self, transaction_message):
        try:
            client_id=transaction_message["client_id"] 
            nodo_id = transaction_message["nodo_id"] 
            results = transaction_message["results"]
            logging.info(f"RESULTS {results}")
            if client_id not in self.worker_finished_with_client.keys():
                self.worker_finished_with_client[client_id] = set()
            self.worker_finished_with_client[client_id].add(nodo_id)
            with open(PATH_TRANSACTIONS+f"{client_id}.csv", "a") as csvfile:
                csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
                for result in results:
                    csv_writer.writerow(result.values())
                    logging.info(f"writing {result} down")
            if len(self.worker_finished_with_client[client_id]) == Q2_FILTER_AMOUNT and ACCOUNTS_EOF:
                results=self._relate_bank_id_bank_name(client_id)
                # self.output_queue.send(message_protocol.internal.serialize([client_id,results]))
                os.remove(PATH_TRANSACTIONS+f"{client_id}.csv")
                logging.info(f"Q2 RESULTS TRANSACTIONS SENT")
        except Exception as e:
            logging.error(f"error: {e}")

    def _relate_bank_id_bank_name(self, client_id):
        with open(PATH_TRANSACTIONS+f"{client_id}.csv", "r", newline="") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                results = []
                for transaction in csv_reader:
                    logging.info(f"saving transaction: {transaction}")
                    values = {
                        "account" : transaction[0],
                        "amount_paid":  transaction[1],
                        "from_bank": transaction[2]
                    }
                    results.append(values)
        return

    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        logging.info(f"msg recived {desiriized_message}")
        if "nodo_id" in desiriized_message.keys():
            self._process_transaction(desiriized_message)
        else:
            pass
            #self._process_account(desiriized_message)
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
        join_filter = JoinFilterQ2()
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