import os
import logging
import bisect
import signal
import socket
import time

from common import middleware, message_protocol,heartbeat

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
DONE = True
WORKING = False
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME =  os.environ["NODE_NAME"]


class DollarAmtFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.eof_count = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))

    def _process_data(self, transaction):
        if transaction["amount_paid"] < 50:
            output = {
                "client_id": transaction["client_id"],
                "from_bank":transaction.get("from_bank", ""),
                "account": transaction["account"],
                "to_bank":transaction.get("to_bank", ""),
                "to_account": transaction["to_account"],
                "amount_paid": transaction["amount_paid"],
            }
            self.output_queue.send(message_protocol.internal.serialize(output))

    def _process_eof(self, deserialized_message):
        client_id = deserialized_message["client_id"]
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        self.output_queue.send(
            message_protocol.internal.serialize(
                {"nodo_id": ID, "client_id": client_id}
            )
        )
        del self.eof_count[client_id]

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        logging.debug(f"MESSAGE {deserialized_message}")
        if len(deserialized_message) == 2:
            self._process_eof(deserialized_message)
        else:
            self._process_data(deserialized_message)
        ack()


    def start(self):
        try:
            for heartbeat in self.heartbeats:
                heartbeat.start()
            self.input_exchange.start_consuming(self.process_messsage)
        except Exception as e:
            logging.exception(f"Error consuming messages: {e}")

    def stop(self):
        logging.info(f"signal.SIGTERM recived stopping {FILTER_PREFIX}_{ID}")
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()



def main():
    logging.basicConfig(level=logging.INFO)
    dollar_amt_filter = DollarAmtFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: dollar_amt_filter.stop(),
    )
    dollar_amt_filter.start()
    dollar_amt_filter.close()
    return 0


if __name__ == "__main__":
    main()
