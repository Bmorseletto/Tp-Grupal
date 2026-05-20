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
DONE = True
WORKING = False

class DollarAmtFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}",f"{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _process_data(self, transaction):
        if transaction["Amt Paid"] < 50:
            output = {
                "Account" : transaction["Account"],
                "To Account" : transaction["To Account"],
                "Amt Paid":  transaction["Amt Paid"]
            }
            self.output_queue.send(message_protocol.internal.serialize(output))
        

    def _process_eof(self, desiriized_message):
        self.output_queue.send(message_protocol.internal.serialize({"nodo_id":ID, "client_id":desiriized_message["client_id"]}))

    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        if len(desiriized_message) == 4:
            self._process_data(desiriized_message)
        elif len(desiriized_message) == 2:
            self._process_eof(desiriized_message)
        else:
            logging.info(f"message does not comply with required format: {desiriized_message}")
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