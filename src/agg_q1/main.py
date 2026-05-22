import os
import logging
import signal

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
        self.filtered_transactions = []
        self.worker_finished_with_client = {}

    def _process_data(self, transaction):
        self.filtered_transactions.append(transaction)

    def _process_eof(self, eof_message):
        client_id=eof_message["client_id"] 
        nodo_id = eof_message["nodo_id"] 
        if client_id not in self.worker_finished_with_client.keys():
            self.worker_finished_with_client[client_id] = set()
        self.worker_finished_with_client[client_id].add(nodo_id)
        if len(self.worker_finished_with_client[client_id]) == Q1_FILTER_AMOUNT:
            for transaction in self.filtered_transactions:
                logging.info(f"sending transaction: {transaction}, to gateway")
                self.output_queue.send(message_protocol.internal.serialize([client_id,transaction]))
        logging.info(f"Q1 RESULTS TRANSACTIONS SENT")

    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        if len(desiriized_message) == 2: #modificar 
            self._process_eof(desiriized_message)
        else:
            self._process_data(desiriized_message)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        self.input_queue.stop_consuming()
    def close(self):
        self.input_queue.close()
        self.output_queue.close()
def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilterQ1()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: join_filter.stop(),
    )
    join_filter.start()
    join_filter.close()
    return 0


if __name__ == "__main__":
    main()