import fcntl
import os
import logging
import signal
import csv


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
        self.filtered_transactions = {}
        self.worker_finished_with_client = {}

    def _process_data(self, transactions):
        try:
            for transaction in transactions:
                logging.info(f"transaction {transaction}")
                client_id= transaction.pop("client_id")
                logging.info(f"processing data OF {client_id}")
                if len(transaction.values()) == 0:
                    logging.info(f"empty results no transaction complied with filters {transaction}")
                    return
                with open(RESULTS_STORAGE+f"{client_id}.csv", "a") as csvfile:
                    csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
                    csv_writer.writerow(transaction.values())
                    logging.info(f"writing {transaction} down")
        except Exception as e:
            logging.error(f"ERROR: {e}")
       

    def _process_eof(self, eof_message):
        try:
            client_id=eof_message["client_id"] 
            nodo_id = eof_message["nodo_id"] 
            if client_id not in self.worker_finished_with_client.keys():
                self.worker_finished_with_client[client_id] = set()
            self.worker_finished_with_client[client_id].add(nodo_id)
            
            if len(self.worker_finished_with_client[client_id]) == Q3_FILTER_AMOUNT:
                results = []
                if os.path.isfile(RESULTS_STORAGE+f"{client_id}.csv"):
                    with open(RESULTS_STORAGE+f"{client_id}.csv", "r", newline="") as csvfile:
                        csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                        for transaction in csv_reader:
                            logging.info(f"sending transaction: {transaction}, to gateway")
                            values = {
                                "account" : transaction[0],
                                "amount_paid" : transaction[1],
                                "payment_format":  transaction[2]
                            }
                            results.append(values)
                    os.remove(RESULTS_STORAGE+f"{client_id}.csv")
                results = sorted(results, key=lambda x: x['payment_format'])
                self.output_queue.send(message_protocol.internal.serialize([client_id,"q3",results]))
                if os.path.isfile(AVG_STORAGE+f"{client_id}.csv"):
                    os.remove(AVG_STORAGE+f"{client_id}.csv")
                logging.info(f"Q3 RESULTS TRANSACTIONS SENT")
        except Exception as e:
            logging.error(f"ERROR: {e}")



    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        logging.info(f"MESSAGE: {desiriized_message}")
        if len(desiriized_message) == 2: #modificar 
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