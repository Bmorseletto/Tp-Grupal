import os
import logging
import bisect
import signal

from common import middleware, message_protocol
import zlib

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
#OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
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
        self.output_exchange_q1 = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_Q1_PREFIX,  [FILTER_Q1_PREFIX] + [str(i) for i in range(FILTER_Q1_AMOUNT)]
        )
        #TODO: Agregar resto de los exchanges o routing keys paralas otras queries

    def _process_data(self, transaction): #TODO: se puede refactorizar esta funciona a pequeñas funciones para cada query
        if transaction["payment_currency"] == "US Dollar":
            output = {
                "client_id": transaction["client_id"],
                "account" : transaction["account"],
                "to_account" : transaction["to_account"],
                "amount_paid":  transaction["amount_paid"]
            }
            routing_key = zlib.crc32(output["account"].encode('utf-8')) % FILTER_Q1_AMOUNT #Usamos la account de origen y la cantidad de filtros Q1 para routear el mensaje
            self.output_exchange_q1.send_by_key(message_protocol.internal.serialize(output), str(routing_key))
        

    def _process_eof(self, desiriized_message):
        logging.info("sending eof to next node")
        self.output_exchange_q1.send_by_key(message_protocol.internal.serialize({"nodo_id":ID, "client_id":desiriized_message[0]}), FILTER_Q1_PREFIX)

    def process_messsage(self, message, ack, nack):
        desiriized_message = message_protocol.internal.deserialize(message)
        logging.info(f"MESSAGE: {desiriized_message}")
        if len(desiriized_message) == 1:
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