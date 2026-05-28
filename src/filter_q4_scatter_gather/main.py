import os
import logging
import signal
from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SCATTER_DETECTOR_PREFIX = os.environ["SCATTER_DETECTOR_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
SCATTER_VALUE = int(os.environ["SCATTER_VALUE"])
Q4_GRAPH_AMOUNT = int(os.environ["Q4_GRAPH_AMOUNT"])
SCATTER_DETECTOR_STORAGE = "/output/q4_scatter_"

class ScatterGatherDetector:
    def __init__(self):

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SCATTER_DETECTOR_PREFIX,
            [SCATTER_DETECTOR_PREFIX, SCATTER_DETECTOR_PREFIX + str(ID)]
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_PREFIX
        )
        self.eof_count = {}
        self.results = {}

    def _process_data(self, result):
        client_id = result.get("client_id")
        if client_id is None:
            return
        if client_id not in self.results:
            self.results[client_id] = []
        self.results[client_id].append(result)

    def _process_eof(self, message):
        client_id = message.get("client_id")
        if client_id is None:
            return

        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        logging.info(f"EOF recibido de GraphFilter para client {client_id} ({self.eof_count[client_id]}/{Q4_GRAPH_AMOUNT})")

        if self.eof_count[client_id] < Q4_GRAPH_AMOUNT:
            return

        self._print_and_send(client_id)
        self.results.pop(client_id, None)
        self.eof_count.pop(client_id, None)

    def _print_and_send(self, client_id):
        logging.info(f"Scatter-Gather results for client {client_id}")

        for result in self.results.get(client_id, []):
            destinations = result.get("destinations", {})
            valid_destinations = {dest: count for dest, count in destinations.items() if count >= SCATTER_VALUE}
            if not valid_destinations:
                continue

            print(f"* bank={result['origin_bank']} account={result['origin_account']} destinations={valid_destinations}")

            filtered_result = {
                "client_id": client_id,
                "origin_bank": result["origin_bank"],
                "origin_account": result["origin_account"],
                "destinations": valid_destinations
            }
            self.output_queue.send(message_protocol.internal.serialize(filtered_result))

            # EOF
            self.output_queue.send( message_protocol.internal.serialize(
                {"nodo_id": ID, "client_id": client_id}
            ))

    def process_message(self, message, ack, nack):
        deserialized = message_protocol.internal.deserialize(message)
        logging.info(f"MESSAGE {deserialized}")
        if len(deserialized) == 2:
            self._process_eof(deserialized)
        else:
            self._process_data(deserialized)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        self.output_queue.close()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        try:
            if self.input_exchange:
                self.input_exchange.close()
        except Exception as e:
            logging.warning(f"Input exchange ya estaba cerrado: {e}")

        try:
            if self.output_queue:
                self.output_queue.close()
        except Exception as e:
            logging.warning(f"Output queue ya estaba cerrada: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    scatter_detector = ScatterGatherDetector()
    signal.signal(signal.SIGTERM, lambda signum, frame: scatter_detector.stop())
    scatter_detector.start()
    scatter_detector.close()
    return 0

if __name__ == "__main__":
    main()