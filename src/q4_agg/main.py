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

    def _process_data(self, result):
        try:
            client_id = result.get("client_id")
            if client_id is None:
                return

            path = RESULTS_STORAGE + f"{client_id}.csv"
            write_header = not os.path.exists(path)

            with open(path, "a", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["origin_bank","origin_account","destinations"])
                if write_header:
                    writer.writeheader()
                writer.writerow({
                    "origin_bank": result.get("origin_bank"),
                    "origin_account": result.get("origin_account"),
                    "destinations": result.get("destinations", {})
                })
            logging.info(f"writing result for client {client_id} down")
        except Exception as e:
            logging.error(f"ERROR: {e}")

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
                with open(path, "r", newline="") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        values = {
                            "origin_bank": row["origin_bank"],
                            "origin_account": row["origin_account"],
                            "destinations": eval(row["destinations"])
                        }
                        results.append(values)
                        logging.info(f"sending result: {values}")

                self.output_queue.send(message_protocol.internal.serialize([client_id, results]))
                os.remove(path)
                logging.info(f"Q4 RESULTS SENT for client {client_id}")
        except Exception as e:
            logging.error(f"ERROR: {e}")

    def process_message(self, message, ack, nack):
        deserialized = message_protocol.internal.deserialize(message)
        logging.info(f"MESSAGE: {deserialized}")
        if len(deserialized) == 2:  # EOF
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