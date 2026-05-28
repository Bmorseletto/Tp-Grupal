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

import os, csv, fcntl

COMPONENTS_FILE = "/output/q4_graph_components.csv"


class GraphRouterCSV:
    def __init__(self, num_nodes):
        self.num_nodes = num_nodes
        os.makedirs(os.path.dirname(COMPONENTS_FILE), exist_ok=True)
        if not os.path.exists(COMPONENTS_FILE):
            with open(COMPONENTS_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["rep", "comp_id"])
                writer.writeheader()

    def _load_components(self):
        components = {}
        with open(COMPONENTS_FILE, "r", newline="") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            reader = csv.DictReader(f)
            for row in reader:
                components[row["rep"]] = int(row["comp_id"])
            fcntl.flock(f, fcntl.LOCK_UN)
        return components

    def _rewrite_components(self, components):
        """Reescribe todo el CSV con el dict actualizado"""
        with open(COMPONENTS_FILE, "w", newline="") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            writer = csv.DictWriter(f, fieldnames=["rep", "comp_id"])
            writer.writeheader()
            for rep, comp_id in components.items():
                writer.writerow({"rep": rep, "comp_id": comp_id})
            fcntl.flock(f, fcntl.LOCK_UN)

    def get_node(self, to_bank, to_account, from_bank, from_account):
        rep_to = f"{to_bank}:{to_account}"
        rep_fr = f"{from_bank}:{from_account}"
        components = self._load_components()

        if rep_to not in components and rep_fr not in components:
            # nuevo componente
            comp_id = len(components)
            components[rep_to] = comp_id
            components[rep_fr] = comp_id
            self._rewrite_components(components)
        elif rep_to in components and rep_fr not in components:
            comp_id = components[rep_to]
            components[rep_fr] = comp_id
            self._rewrite_components(components)
        elif rep_fr in components and rep_to not in components:
            comp_id = components[rep_fr]
            components[rep_to] = comp_id
            self._rewrite_components(components)
        else:
            # ambos existen: si tienen comp_id distinto, unificar
            comp_id_to = components[rep_to]
            comp_id_fr = components[rep_fr]
            if comp_id_to != comp_id_fr:
                # normalizar: todos los reps con comp_id_fr pasan a comp_id_to
                for rep, cid in components.items():
                    if cid == comp_id_fr:
                        components[rep] = comp_id_to
                comp_id = comp_id_to
                self._rewrite_components(components)
            else:
                comp_id = comp_id_to

        routing_key = "Q4Graph" + str(comp_id % self.num_nodes)
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

        self.graph_router = GraphRouterCSV(self.outputs_amounts[1])
        logging.info(f"OUTPUTS EXCHANGE AMOUNT: {len(self.output_exchanges)}")
        logging.info(f"OUTPUTS EXCHANGE ROUTING KEYS: {self.output_exchanges[0]._routing_keys}")

    def _process_data(self, transaction):
        
        transaction_timestamp=datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M")
        initial_date = datetime.strptime(INITIAL_DATE, "%Y/%m/%d")
        end_date = datetime.strptime(END_DATE, "%Y/%m/%d")
        logging.info(f"transaction_timestamp {transaction_timestamp}, initial_date {initial_date}, end_date {end_date} ")
        logging.info(f"date comp: {initial_date <= transaction_timestamp <= end_date}")

        if initial_date <= transaction_timestamp <= end_date:
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
                transaction.get("account", "")   # ojo: aquí es 'account', no 'from_account'
            )
            logging.info(f"SENDING Q4Graph transaction {transaction} -> {routing_key_q4}")
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