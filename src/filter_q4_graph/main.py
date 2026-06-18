import os
import logging
import signal
import zlib
from collections import defaultdict

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_DATE_AMOUNT = int(os.environ["FILTER_DATE_AMOUNT"])
SCATTER_VALUE = int(os.environ["SCATTER_VALUE"])


class GraphFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            FILTER_PREFIX,
            [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"],
            ID
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            OUTPUT_PREFIX,
            [OUTPUT_PREFIX] + [OUTPUT_PREFIX + str(j) for j in range(OUTPUT_AMOUNT)],
            ID
        )
        self.eof_count = {}
        self.origin_groups = {}
        # {client_id: {
        #   (origin_bank, origin_account): {
        #       "destinations": {
        #           (dest_bank, dest_account):
        #               account, from_bank, to_account, to_bank
        #           }
        #       }
        #   }
        # }
        self.destination_groups = {}
        # {client_id: {
        #   (dest_bank, dest_account): {
        #       "transactions": {
        #           (dest_bank, dest_account):
        #               account, from_bank, to_account, to_bank
        #           }
        #       }
        #   }
        # }

    def _process_data(self, transaction):
        client_id = transaction.get("client_id")
        if client_id is None:
            return

        origin_account = transaction.get("account")
        origin_bank = transaction.get("from_bank")
        destination_account = transaction.get("to_account")
        destination_bank = transaction.get("to_bank")

        origin_key = f"{origin_bank},{origin_account}"
        destination_key = f"{destination_bank},{destination_account}"

        if client_id not in self.origin_groups:
            self.origin_groups[client_id] = defaultdict(
                lambda: {"destinations": {}}
            )
            self.destination_groups[client_id] = defaultdict(
                lambda: {"transactions": {}}
            )

        origin_data = self.origin_groups[client_id][origin_key]["destinations"]
        destination_data =  self.destination_groups[client_id][destination_key]["transactions"]
        if destination_account is not None or destination_bank is not None:
            if destination_key not in origin_data.keys():
                # No se admiten repetidos
                origin_data[destination_key] = (
                    {"account": origin_account, "from_bank": origin_bank, "to_account": destination_account, "to_bank": destination_bank}
                )
            if origin_key not in destination_data.keys():
                # No se admiten repetidos
                destination_data[origin_key] = (
                    {"account": origin_account, "from_bank": origin_bank, "to_account": destination_account, "to_bank": destination_bank}
                )
            

    def _process_eof(self, deserialized_message):
        client_id = deserialized_message.get("client_id")
        if client_id is None:
            return

        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < FILTER_DATE_AMOUNT:
            return
        if client_id in self.origin_groups.keys():
            for  origin_key, data2 in self.origin_groups[client_id].items():
                if len(data2["destinations"].keys()) >= SCATTER_VALUE:
                    # Se hace un broadcast de todas las cuentas sospechosas (las que le enviaron
                    # dinero a >=5 cuentas distintas)
                    self.output_exchange.send_by_key(
                        message_protocol.internal.serialize(
                            {"client_id": client_id, "origin_account": origin_key, "transactions": data2["destinations"]}
                        ),
                        OUTPUT_PREFIX,
                    )
            for destination_key, data2 in self.destination_groups[client_id].items():
                # Se rutea en base al destino (to_bank y to_account)
                routing_key=self._get_output_routing_key(destination_key[0], destination_key[1])
                self.output_exchange.send_by_key(
                    message_protocol.internal.serialize(
                        {"client_id": client_id, "destination_account": destination_key, "transactions": data2["transactions"]}
                    ),
                    routing_key,
                )
        self.output_exchange.send_by_key(
            message_protocol.internal.serialize(
                {"nodo_id": ID, "client_id": client_id}
            ),
            OUTPUT_PREFIX,
        )
        self.origin_groups.pop(client_id, None)
        self.eof_count.pop(client_id, None)

    def _format_node(self, node_key):
        bank, account = node_key
        return f"bank={bank or 'unknown'} account={account or 'unknown'}"

    def _format_edges(self, edges):
        return {
            self._format_node(node): count for node, count in edges.items()
        }

    def _get_output_routing_key(self, bank, account):
        origin_hash = zlib.crc32(f"{bank}:{account}".encode("utf-8"))
        return OUTPUT_PREFIX + str(origin_hash % OUTPUT_AMOUNT)

    def _send_result(self, result, bank, account):
        routing_key = self._get_output_routing_key(bank, account)
        self.output_exchange.send_by_key(
            message_protocol.internal.serialize(result), routing_key
        )

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        if len(deserialized_message) == 2:
            self._process_eof(deserialized_message)
        else:
            self._process_data(deserialized_message)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_exchange.close()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    graph_filter = GraphFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: graph_filter.stop())
    graph_filter.start()
    graph_filter.close()
    return 0


if __name__ == "__main__":
    main()
