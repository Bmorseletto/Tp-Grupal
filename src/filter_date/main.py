from datetime import datetime
import os
import logging
import bisect
import signal
import zlib

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUTS_PREFIX = os.environ["OUTPUTS_PREFIX"] # FORMATO: prefix1,prefix2,prefix (usamos split de python para hacer la lista de prefix)
OUTPUTS_AMOUNTS = os.environ["OUTPUTS_AMOUNTS"]  # FORMATO: 1,2,3 (usamos split de python para hacer la lista de amounts)
ROUTING_HASH_TARGET = os.environ["ROUTING_HASH_TARGET"] # columna del csv de input data que se usa para routear el input EJ: from_bank,to_account,
INITIAL_DATE = os.environ["INITIAL_DATE"]
END_DATE =  os.environ["END_DATE"]
DONE = True
WORKING = False

class GraphRouter:
    def __init__(self, num_nodes):
        self.parent = {} # El padre sería la cuenta que inicia el scatter gather
        self.num_nodes = num_nodes
        self.component_id = {} # ID de grupo de banco-cuentas
        self.next_id = 0

    def find(self, x):
        if self.parent.get(x, x) != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent.get(x, x)

    def union(self, a, b):
        # se unen las cuentas conectadas.
        # Ej.: A->B y B->C, por ende A, B y C son del mismo grupo => van a la misma routing key
        pa, pb = self.find(a), self.find(b)
        if pa != pb:
            # unimos pb en pa
            self.parent[pb] = pa
            # propagación comp_id
            if pb in self.component_id:
                self.component_id[pa] = self.component_id[pb]
            elif pa in self.component_id:
                self.component_id[pb] = self.component_id[pa]

    def get_node(self, to_bank, to_account, from_bank, from_account):
        to = f"{to_bank}:{to_account}"
        fr = f"{from_bank}:{from_account}"
        self.union(to, fr)
        rep = self.find(to)

        # asignar id al componente (si es que no existia de antes)
        if rep not in self.component_id:
            self.component_id[rep] = self.next_id
            self.next_id += 1

        comp_id = self.component_id[rep]
        routing_key = "Q4Graph" + str(comp_id % self.num_nodes)
        logging.info(
            f"GRAPH GET NODE: {to_bank}, {to_account}, {from_bank}, {from_account} "
            f"| rep={rep} | comp_id={comp_id} | routing key={routing_key}"
        )
        return routing_key

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

        self.graph_router = GraphRouter(self.outputs_amounts[1])
        logging.info(f"OUTPUTS EXCHANGE AMOUNT: {len(self.output_exchanges)}")
        logging.info(f"OUTPUTS EXCHANGE ROUTING KEYS: {self.output_exchanges[0]._routing_keys}")

    def _process_data(self, transaction):
        
        transaction_timestamp=datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M")
        initial_date = datetime.strptime(INITIAL_DATE, "%Y/%m/%d")
        end_date = datetime.strptime(END_DATE, "%Y/%m/%d")
        logging.info(f"transaction_timestamp {transaction_timestamp}, initial_date {initial_date}, end_date {end_date} ")
        logging.info(f"date comp: {initial_date <= transaction_timestamp <= end_date}")

        if initial_date <= transaction_timestamp <= end_date:
            # for i in range(len(self.output_exchanges)):
            #     routing_key = (
            #     self.outputs_prefix[i]
            #     + str(
            #         zlib.crc32(transaction[self.routing_hash_targets[i]].encode("utf-8"))
            #         % self.outputs_amounts[i]
            #         )
            #     )
            #     logging.info(f"SENDING transaction {transaction}")
            #     self.output_exchanges[i].send_by_key(
            #     message_protocol.internal.serialize(transaction), routing_key
            #     )

            # QUERY 3
            composite_key_avg = transaction.get("payment_format", "")
            routing_key_avg = "AvgCalc" + str(
                zlib.crc32(composite_key_avg.encode("utf-8")) % self.outputs_amounts[0]
            )
            logging.info(f"SENDING AvgCalc transaction {transaction} -> {routing_key_avg}")
            self.output_exchanges[0].send_by_key(
                message_protocol.internal.serialize(transaction), routing_key_avg
            )

            # QUERY 4
            routing_key_q4 = self.graph_router.get_node(
                transaction.get("to_bank", ""),
                transaction.get("to_account", ""),
                transaction.get("from_bank", ""),
                transaction.get("account", "")
            )
            self.output_exchanges[1].send_by_key(
                message_protocol.internal.serialize(transaction), routing_key_q4
            )
           
        

    def _process_eof(self, deserialized_message):
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
        self.input_exchange.close()
        for exchange in self.output_exchanges:
            exchange.close()

    
    def stop(self):
        self.input_exchange.stop_consuming()
    def close(self):
        self.input_exchange.close()
        for exchange in self.output_exchanges():
            exchange.close()
       

def main():
    logging.basicConfig(level=logging.INFO)
    dollar_amt_filter = DateFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: dollar_amt_filter.stop(),
    )
    dollar_amt_filter.start()
    dollar_amt_filter.close()
    return 0


if __name__ == "__main__":
    main()