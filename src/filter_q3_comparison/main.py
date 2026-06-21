import os
import logging
import signal

from common import middleware, message_protocol, heartbeat

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
AVG_CALC_AMOUNT = int(os.environ["AVG_CALC_AMOUNT"])
DATE_FILTER_AMOUNT = int(os.environ["DATE_FILTER_AMOUNT"])
NODO_ID = "nodo_id"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]

class AvgFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.avg_worker_finished_with_client = {}
        self.date_filter_finished_with_client = {}
        self.payment_formats_averages = {}
        self.transactions_per_client = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))

    def _process_data(self, data):
        client_id = data.pop("client_id")
        payment_format = data.get("payment_format", "")
        if client_id not in self.transactions_per_client:
            self.transactions_per_client[client_id] = {}
        if payment_format not in self.transactions_per_client[client_id]:
            self.transactions_per_client[client_id][payment_format] = []
        self.transactions_per_client[client_id][payment_format].append({
            "client_id": client_id,
            "from_bank": data.get("from_bank", ""),
            "account": data.get("account", ""),
            "amount_paid": data.get("amount_paid", 0),
            "payment_format": payment_format,
        })

    def _process_eof(self, deserialized_message):
        try:
            client_id = deserialized_message["client_id"]
            nodo_id = deserialized_message["nodo_id"]
            if client_id not in self.avg_worker_finished_with_client:
                self.avg_worker_finished_with_client[client_id] = set()
                self.date_filter_finished_with_client[client_id] = set()
            if "avg" in deserialized_message:
                self.avg_worker_finished_with_client[client_id].add(nodo_id)
                self.payment_formats_averages.setdefault(client_id, {}).update(deserialized_message["avg"])
            else:
                self.date_filter_finished_with_client[client_id].add(nodo_id)
            if len(self.date_filter_finished_with_client[client_id]) < DATE_FILTER_AMOUNT or len(self.avg_worker_finished_with_client[client_id]) < AVG_CALC_AMOUNT:
                return
            payment_formats_averages = self.payment_formats_averages.get(client_id, {})
            client_transactions = self.transactions_per_client.get(client_id, {})
            for payment_format, average in payment_formats_averages.items():
                avg_threshold = float(average) / 100
                for transaction in client_transactions.get(payment_format, []):
                    try:
                        if float(transaction["amount_paid"]) < avg_threshold:
                            self.output_queue.send(message_protocol.internal.serialize({"results": transaction}))
                    except (TypeError, ValueError):
                        continue
            self.output_queue.send(message_protocol.internal.serialize({"nodo_id": ID, "client_id": client_id}))
            self.transactions_per_client.pop(client_id, None)
            self.payment_formats_averages.pop(client_id, None)
            self.avg_worker_finished_with_client.pop(client_id, None)
            self.date_filter_finished_with_client.pop(client_id, None)
        except Exception as e:
            logging.warning(f"ERROR: {e}")

    def process_messsage(self, message, ack, nack):
        try:
            deserialized_message = message_protocol.internal.deserialize(message)
            if NODO_ID in deserialized_message:
                self._process_eof(deserialized_message)
            else:
                self._process_data(deserialized_message)
        except Exception as e:
            logging.warning(f"ERROR: {e}")
        ack()

    def start(self):
        for heartbeat in self.heartbeats:
            heartbeat.start()
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()

    def stop(self):
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

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
