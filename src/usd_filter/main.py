import os
import logging
import bisect
import signal

from common import middleware, message_protocol
import zlib

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_Q1_AMOUNT = int(os.environ["FILTER_Q1_AMOUNT"])
FILTER_Q1_PREFIX = os.environ["FILTER_Q1_PREFIX"]
DONE = True
WORKING = False

class CurrencyFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}",f"{ID}"]
        )
        self.output_exchange_q1 = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, FILTER_Q1_PREFIX,  [FILTER_Q1_PREFIX] + [str(i) for i in range(FILTER_Q1_AMOUNT)]
        )
        #TODO: Agregar resto de los exchanges o routing keys paralas otras queries

    def _process_data(self, transaction): #TODO: se puede refactorizar esta funciona peuqeñas funciones para cada query
        if transaction["Payment Currency"] == "US Dollar":
            output = {
                "Account" : transaction["Account"],
                "To Account" : transaction["To Account"],
                "Amt Paid":  transaction["Amt Paid"]
            }
            routing_key = zlib.crc32(output["Account"].encode('utf-8')) % FILTER_Q1_AMOUNT #Usamos la account de origen y la cantidad de filtros Q1 para routear el mensaje
            self.output_exchange_q1.send_by_key(message_protocol.internal.serialize(output), routing_key)
        

    def _process_eof(self, desiriized_message):
        self.output_exchange_q1.send(message_protocol.internal.serialize({"nodo_id":ID, "client_id":desiriized_message["client_id"]}))

    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        if len(desiriized_message) == 2:
            self._process_eof(desiriized_message)
        else:
            self._process_data(desiriized_message)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_exchange_q1.close()

    
    def stop(self):
        self.input_exchange.stop_consuming()
    def close(self):
        self.input_exchange.close()
        self.output_exchange_q1.close()
       

def main():
    logging.basicConfig(level=logging.INFO)
    dollar_amt_filter = CurrencyFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: dollar_amt_filter.stop(),
    )
    dollar_amt_filter.start()
    dollar_amt_filter.close()
    return 0


if __name__ == "__main__":
    main()