from datetime import datetime
import os
import logging
import bisect
import signal
import zlib

from common import middleware, message_protocol
from graph_router import GraphRouterCSV

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUTS_PREFIX = os.environ["OUTPUTS_PREFIX"] # FORMATO: prefix1,prefix2,prefix (usamos split de python para hacer la lista de prefix)
OUTPUTS_AMOUNTS = os.environ["OUTPUTS_AMOUNTS"]  # FORMATO: 1,2,3 (usamos split de python para hacer la lista de amounts)
ROUTING_HASH_TARGET = os.environ["ROUTING_HASH_TARGET"] # columna del csv de input data que se usa para routear el input EJ: from_bank,to_account,
INITIAL_DATE = os.environ["INITIAL_DATE"]
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
END_DATE =  os.environ["END_DATE"]
DONE = True
WORKING = False

class DateFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}",FILTER_PREFIX+f"{ID}"]
        )
        logging.info(f"PERSONAL ROUTING KEY: {FILTER_PREFIX+f"{ID}"}")
        self.outputs_prefix = OUTPUTS_PREFIX.split(",")
        self.outputs_amounts = list(map(int,OUTPUTS_AMOUNTS.split(",")))
        self.routing_hash_targets = ROUTING_HASH_TARGET.split(",")
        self.output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                self.outputs_prefix[i],
                [self.outputs_prefix[i]]
                + [
                    self.outputs_prefix[i] + str(j)
                    for j in range(self.outputs_amounts[i])
                ],
            )
            for i in range(len(self.outputs_prefix))
        ]

        self.eof_count = {}
        logging.info(f"OUTPUTS EXCHANGE AMOUNT: {len(self.output_exchanges)}")
        logging.info(f"OUTPUTS EXCHANGE ROUTING KEYS: {self.output_exchanges[0]._routing_keys}")
        logging.info(f"ROUTING_HASH_TARGET: {ROUTING_HASH_TARGET}")

    def _process_data(self, transaction):
        
        transaction_timestamp=datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M")
        initial_date = datetime.strptime(INITIAL_DATE, "%Y/%m/%d")
        end_date = datetime.strptime(END_DATE, "%Y/%m/%d")
        logging.debug(f"transaction_timestamp {transaction_timestamp}, initial_date {initial_date}, end_date {end_date} ")
        logging.debug(f"date comp: {initial_date <= transaction_timestamp <= end_date}")

        if initial_date <= transaction_timestamp <= end_date:
            for i in range(len(self.output_exchanges)):
                logging.info(f"ROUTING_HASH_TARGET I: {self.routing_hash_targets[i]}")
                if '+' in self.routing_hash_targets[i]:
                    self.graph_router = GraphRouterCSV(self.outputs_amounts[i])
                     # QUERY 4
                    routing_key_q4 = self.graph_router.get_node(
                        transaction.get("to_bank", ""),
                        transaction.get("to_account", ""),
                        transaction.get("from_bank", ""),
                        transaction.get("account", "")
                    )
                    self.output_exchanges[i].send_by_key(
                        message_protocol.internal.serialize(transaction), routing_key_q4
                    )
                else:
                    routing_key = (
                    self.outputs_prefix[i]
                    + str(
                        zlib.crc32(transaction[self.routing_hash_targets[i]].encode("utf-8"))
                        % self.outputs_amounts[i]
                        )
                    )
                    logging.info(f"SENDING transaction {transaction}")
                    logging.debug(f"Routing transaction {transaction} to output exchange with routing key {routing_key}")
                    self.output_exchanges[i].send_by_key(
                        message_protocol.internal.serialize(transaction), routing_key
                        )
           
           
    def _process_eof(self, deserialized_message):
        client_id = deserialized_message["client_id"]
        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        for i, output_exchange in enumerate(self.output_exchanges):
            output_exchange.send_by_key(
                message_protocol.internal.serialize(
                    {"nodo_id": ID, "client_id": deserialized_message["client_id"]}
                ),
                self.outputs_prefix[i],
            )

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        logging.info(f"MESSAGE {deserialized_message}")
        if len(deserialized_message) == 2:
            self._process_eof(deserialized_message)
        else:    
            self._process_data(deserialized_message)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    
    def stop(self):
        logging.info(f"signal.SIGTERM recived stopping {FILTER_PREFIX}_{ID}")
        self.input_exchange.stop_consuming()
    def close(self):
        self.input_exchange.close()
        for exchange in self.output_exchanges:
            exchange.close()
       

def main():
    logging.basicConfig(level=logging.INFO)
    date_filter = DateFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: date_filter.stop(),
    )
    date_filter.start()
    date_filter.close()
    return 0


if __name__ == "__main__":
    main()