import os
import logging
import signal
import csv

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q4_SCATTER_AMOUNT = int(os.environ["Q4_SCATTER_AMOUNT"])
RESULTS_STORAGE = "/output/q4_agg_"

class AggregatorQ4:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.worker_finished_with_client = {}
        self.results = {}

    def _process_data(self, result):
        try:
            client_id = result.get("client_id")
            sus_accounts = result.get("suspicious_accounts")
            if client_id not in  self.results.keys():
                self.results[client_id] = {}
            for account, final_accounts in sus_accounts.items():
                if account not in self.results[client_id].keys():
                    self.results[client_id][account] = {}
                for final_account, transaction_amount in final_accounts.items():
                    self.results[client_id][account][final_account]=self.results[client_id][account].get(final_account, 0) + transaction_amount
        except Exception as e:
            logging.error(f"PROCESS DATA ERROR: {e}")

    def _process_eof(self, eof_message):
        try:
            client_id = eof_message["client_id"]
            nodo_id = eof_message["nodo_id"]

            if client_id not in self.worker_finished_with_client:
                self.worker_finished_with_client[client_id] = set()
            self.worker_finished_with_client[client_id].add(nodo_id)

            if len(self.worker_finished_with_client[client_id]) == Q4_SCATTER_AMOUNT:
                path = RESULTS_STORAGE + f"{client_id}.csv"
                results = []
                for account, final_accounts in self.results[client_id].items():
                    for final_account, transaction_amount in final_accounts.items():
                        origin = account.split(',')
                        dest =final_account.split(',')
                        if transaction_amount >= 5:
                            message = {
                                "from_bank":origin[0],
                                "from_account":origin[1],
                                "to_bank" : dest[0],
                                "to_account":dest[1]
                            }
                            self.output_queue.send(message_protocol.internal.serialize([client_id, "q4", [message]]))
                self.output_queue.send(message_protocol.internal.serialize([client_id, "q4"]))
                logging.info(f"Q4 RESULTS SENT for client {client_id}")
        except Exception as e:
            logging.error(f"EOF ERROR: {e}")

    def process_message(self, message, ack, nack, ctx):
        deserialized = message_protocol.internal.deserialize(message)
        if "nodo_id" in deserialized.keys():  # EOF
            self._process_eof(deserialized)
        else:
            self._process_data(deserialized)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

def main():
    try:
        logging.basicConfig(level=logging.INFO)
        aggregator = AggregatorQ4()
        signal.signal(signal.SIGTERM, lambda signum, frame: aggregator.stop())
        aggregator.start()
        aggregator.close()
        return 0
    except Exception as e:
        logging.error(f"error: {e}")

if __name__ == "__main__":
    main()