import csv
import fcntl
import os
import logging
import bisect
import signal

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
DONE = True
WORKING = False
NODO_TYPE = 1
AVG_STORAGE = "/output/q3_avg_"
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])

class AvgCalculator:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}",FILTER_PREFIX+f"{ID}"]
        )
        self.output_exchange =  middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                OUTPUT_PREFIX,
                [OUTPUT_PREFIX]
                + [
                    OUTPUT_PREFIX + str(j)
                    for j in range(OUTPUT_AMOUNT)
                ],
        )
        self.transactions_per_payment_format = {}
        self.eof_count = {}

    def _process_data(self, transaction):
        try:
            payment_format = transaction["payment_format"]
            client_id =transaction["client_id"]
            if client_id not in self.transactions_per_payment_format:
                logging.info(f"new_entry: {client_id}")
                self.transactions_per_payment_format[client_id] = {}
            if payment_format not in self.transactions_per_payment_format[client_id].keys():
                self.transactions_per_payment_format[client_id][payment_format] = {"transactions":0,"total amount paid":0}
            payment_format_current_data=self.transactions_per_payment_format[client_id][payment_format]
            payment_format_current_data["transactions"] +=1
            payment_format_current_data["total amount paid"] += transaction["amount_paid"]
            logging.info(f"dic: {self.transactions_per_payment_format[client_id]}")
        except Exception as e:
            logging.error(f"ERROR: {e}")

    def _process_eof(self, deserialized_message):
        client_id = deserialized_message["client_id"]
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        logging.info(f"transactions per payment: {self.transactions_per_payment_format}")
        if client_id in self.transactions_per_payment_format.keys():
            for payment_format, data in self.transactions_per_payment_format[client_id].items():
                average = {
                    "avg": data["total amount paid"] / data["transactions"], 
                    "payment_format": payment_format
                }
                with open(AVG_STORAGE+f"{client_id}.csv", "a") as csvfile:
                    fcntl.flock(csvfile, fcntl.LOCK_EX)
                    try:
                        csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
                        csv_writer.writerow(average.values())
                    except Exception as e:
                        logging.error(f"ERROR: {e}")
                    finally:
                        fcntl.flock(csvfile, fcntl.LOCK_UN)
                logging.info(f"writing {average} down")
            self.transactions_per_payment_format.pop(client_id)
        logging.info(f"SENDING EOF")
        self.output_exchange.send_by_key(message_protocol.internal.serialize({"nodo_id":ID, "client_id":client_id, "avg": True}), OUTPUT_PREFIX)

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        logging.info(f"MESSAGE {deserialized_message}")
        if len(deserialized_message) == 2:
            logging.info(f"EOF {deserialized_message}")
            self._process_eof(deserialized_message)
        else:    
            self._process_data(deserialized_message)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    
    def stop(self):
        self.input_exchange.stop_consuming()
    def close(self):
        self.input_exchange.close()
        self.output_exchange.close()
       

def main():
    logging.basicConfig(level=logging.INFO)
    avg_calculator = AvgCalculator()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: avg_calculator.stop(),
    )
    avg_calculator.start()
    avg_calculator.close()
    return 0


if __name__ == "__main__":
    main()