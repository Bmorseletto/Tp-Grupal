import os
import logging
import bisect
import signal

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
DONE = True
WORKING = False
CLIENT_ID_KEY = "client_id"
BANK_KEY = "from_bank"
AMMOUNT_PAID_KEY = "amount_paid"

class MaxTransactionFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}",FILTER_PREFIX+f"{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.max_transaction_per_bank = {}
        self.eof_count = {}

    def _process_data(self, transaction):
        client_id = transaction.pop(CLIENT_ID_KEY)
        bank_id = transaction[BANK_KEY]
        if client_id not in self.max_transaction_per_bank.keys():
            self.max_transaction_per_bank[client_id] = {}
        if bank_id in self.max_transaction_per_bank[client_id].keys():
            if self.max_transaction_per_bank[client_id][bank_id][AMMOUNT_PAID_KEY] >= transaction[AMMOUNT_PAID_KEY]:
                return
        logging.debug(f"TRANSDACTION {transaction}")
        self.max_transaction_per_bank[client_id][bank_id] = transaction
    

    def _process_eof(self, deserialized_message):
        client_id = deserialized_message["client_id"]
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        results = list(self.max_transaction_per_bank[deserialized_message[CLIENT_ID_KEY]].values())
        logging.info(f"Sending max values {results}, to {OUTPUT_QUEUE}")
        self.output_queue.send(message_protocol.internal.serialize({"nodo_id":ID, CLIENT_ID_KEY:deserialized_message[CLIENT_ID_KEY], "results": results} ))

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        logging.debug(f"MESSAGE {deserialized_message}")
        if len(deserialized_message) == 2:
            self._process_eof(deserialized_message)
        else:    
            self._process_data(deserialized_message)
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
       

def main():
    logging.basicConfig(level=logging.INFO)
    dollar_amt_filter = MaxTransactionFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: dollar_amt_filter.stop(),
    )
    dollar_amt_filter.start()
    dollar_amt_filter.close()
    return 0


if __name__ == "__main__":
    main()