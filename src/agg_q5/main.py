import os
import logging
import signal

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
Q5_FILTER_AMOUNT = int(os.environ["Q5_FILTER_AMOUNT"])
Q5_FILTER_PREFIX = os.environ["Q5_FILTER_PREFIX"]


class AggregatorQ5:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.count = {}
        self.worker_finished_with_client = {}

    def _process_data(self, transaction: dict):
        client_id = transaction.pop("client_id")
        self.count[client_id] = self.count.get(client_id, 0) + 1

    def _process_eof(self, eof_message):
        client_id = eof_message["client_id"]
        nodo_id = eof_message["nodo_id"]
        self.worker_finished_with_client.setdefault(client_id, set()).add(nodo_id)
        if len(self.worker_finished_with_client[client_id]) == Q5_FILTER_AMOUNT:
            count = self.count.pop(client_id, 0)
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, "q5", {"count": count}])
            )
            del self.worker_finished_with_client[client_id]

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        if len(deserialized_message) == 2:
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
        aggregator = AggregatorQ5()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: aggregator.stop(),
        )
        aggregator.start()
        aggregator.close()
        return 0
    except Exception:
        logging.exception(f"An error occurred while running the {Q5_FILTER_PREFIX} aggregator")


if __name__ == "__main__":
    main()
