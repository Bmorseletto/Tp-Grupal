import csv
import os
import logging
import bisect
import signal
import fcntl

from common import middleware, message_protocol
import multiprocessing

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
AVG_CALC_AMOUNT =  int(os.environ["AVG_CALC_AMOUNT"])
DATE_FILTER_AMOUNT =  int(os.environ["DATE_FILTER_AMOUNT"])
DONE = True
WORKING = False
NODO_TRANSACTIONS = 0
NODO_AVG = 1
NODO_ID = "nodo_id"
AVG_STORAGE = "/output/q3_avg_"
TRANSACTION_STORAGE = "/output/q3_transaction_"
PAYMENT_METHOD = 1
AVERAGE =0

class AvgFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}",FILTER_PREFIX+f"{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.avg_worker_finished_with_client = {}
        self.date_filter_finished_with_client ={}
        self.payment_formats_averages = {}

    def _process_data(self, data):
        # logging.debug(f"transaction data {data}")
        client_id= data.pop("client_id")
        logging.debug(f"processing data OF {client_id}")
        with open(TRANSACTION_STORAGE+f"{client_id}_{ID}.csv", "a") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            csv_writer.writerow(data.values())
            # logging.debug(f"writing {data} down")

    def _process_eof(self, deserialized_message):
        try:
            client_id = deserialized_message["client_id"]
            nodo_id = deserialized_message["nodo_id"]
            if client_id not in self.avg_worker_finished_with_client.keys():
                    self.avg_worker_finished_with_client[client_id] = set()
                    self.date_filter_finished_with_client[client_id] = set()
            if "avg" in deserialized_message.keys():
                self.avg_worker_finished_with_client[client_id].add(nodo_id)
                self.payment_formats_averages.setdefault(client_id, {}).update(deserialized_message["avg"])
            else:
                self.date_filter_finished_with_client[client_id].add(nodo_id)
            if len(self.date_filter_finished_with_client[client_id]) < DATE_FILTER_AMOUNT or len(self.avg_worker_finished_with_client[client_id]) < AVG_CALC_AMOUNT:
                logging.debug("WAITING FOR PREVIOUS WORKERS TO FINISH")
                return
            payment_formats_averages = self.payment_formats_averages.get(client_id, {})
            processes = []
            results=multiprocessing.Queue()
            for payment_format, average in payment_formats_averages.items():
                process = multiprocessing.Process(target=_filter_transactions, args=(payment_format, float(average), results, client_id))
                processes.append(process)
                process.start()
            for process in processes:
                result=results.get()
                for transaction in result:
                    self.output_queue.send(message_protocol.internal.serialize({"results": transaction}))
                logging.info(f"some data sent")
            self.output_queue.send(message_protocol.internal.serialize({"nodo_id":ID, "client_id":client_id}))
            if os.path.isfile(TRANSACTION_STORAGE+f"{client_id}_{ID}.csv"):
                os.remove(TRANSACTION_STORAGE+f"{client_id}_{ID}.csv")
            for process in processes:
                process.join()
        except Exception as e:
            logging.info(f"ERROR: {e}")


    
    def process_messsage(self, message, ack, nack):
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            # logging.debug(f"MESSAGE {deserialized_message}")
            if NODO_ID in deserialized_message.keys():
                self._process_eof(deserialized_message)
            else:    
                self._process_data(deserialized_message)
        except Exception as e:
            logging.info(f"ERROR: {e}")
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()

    
    def stop(self):
        self.input_exchange.stop_consuming()
    def close(self):
        self.input_exchange.close()
        self.output_queue.close()
       

# def _get_payment_formats(client_id):
#     averages = {}
#     if os.path.isfile(AVG_STORAGE+f"{client_id}.csv"):
#         with open(AVG_STORAGE+f"{client_id}.csv", "r") as csvfile:
#             fcntl.flock(csvfile, fcntl.LOCK_SH)
#             try:
#                 csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
#                 for avg in csv_reader:
#                     averages[avg[PAYMENT_METHOD]] = avg[AVERAGE]
#             finally:
#                 fcntl.flock(csvfile, fcntl.LOCK_UN)
#     return averages
        
                    

def _filter_transactions(payment_format,average,results_queue, client_id):
    logging.basicConfig(level=logging.INFO)
    transactions = []
    logging.info(f"payment_format: {payment_format}, average: {average/100}")
    if os.path.isfile(TRANSACTION_STORAGE+f"{client_id}_{ID}.csv"):
        with open(TRANSACTION_STORAGE+f"{client_id}_{ID}.csv", "r") as csvfile:
            fcntl.flock(csvfile, fcntl.LOCK_SH)
            try:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                for line in csv_reader:
                    # logging.debug(f"file line: {line}, payment_format: {payment_format}, average: {average/100}")
                    if line[3] != payment_format:
                        continue
                    if float(line[1]) < (average/100):
                        transaction ={
                            "client_id": client_id,
                            "account": line[0],
                            "amount_paid": line[1],
                            "payment_format": line[3]
                        }
                        transactions.append(transaction)
            except Exception as e:
                logging.error(f"ERROR {e}")
            finally:
                fcntl.flock(csvfile, fcntl.LOCK_UN)
    results_queue.put(transactions)

def main():
    logging.basicConfig(level=logging.INFO)
    avg_calculator = AvgFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: avg_calculator.stop(),
    )
    avg_calculator.start()
    avg_calculator.close()
    return 0


if __name__ == "__main__":
    main()