import os
import logging
import signal

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
QUERY_AMOUNT = int(os.environ["QUERY_AMOUNT"])


class JoinNode:
    def __init__(self):
        self.input_queue = middleware.MultiQueueConsumer(MOM_HOST)
        for queue_name in os.environ["INPUT_QUEUES"].split(","):
            queue_name = queue_name.strip()
            self.input_queue.add_queue(queue_name, self._on_message)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_results = {}

    def _on_message(self, message, ack, nack):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            client_id = deserialized[0]
            query_id = deserialized[1]
            results = deserialized[2]
            logging.info(f"received {query_id} results for client {client_id}")
            if client_id not in self.client_results:
                self.client_results[client_id] = {}
            self.client_results[client_id][query_id] = results
            if len(self.client_results[client_id]) == QUERY_AMOUNT:
                consolidated = self.client_results.pop(client_id)
                self.output_queue.send(
                    message_protocol.internal.serialize([client_id, consolidated])
                )
                logging.info(f"sent consolidated results for client {client_id}")
            ack()
        except Exception as e:
            logging.error(f"error processing message: {e}")
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
        join = JoinNode()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: join.stop(),
        )
        join.start()
        join.close()
        return 0
    except Exception as e:
        logging.error(f"error: {e}")


if __name__ == "__main__":
    main()
