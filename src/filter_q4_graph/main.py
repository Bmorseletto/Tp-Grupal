import os
import logging
import signal
from collections import defaultdict

from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_DATE_AMOUNT = int(os.environ["FILTER_DATE_AMOUNT"])


class GraphFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            FILTER_PREFIX,
            [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"]
        )
        self.eof_count = {}
        self.origin_groups = {}
        self.destination_groups = {}

    def _process_data(self, transaction):
        client_id = transaction.get("client_id")
        if client_id is None:
            return

        origin_account = transaction.get("account")
        origin_bank = transaction.get("from_bank")
        destination_account = transaction.get("to_account")
        destination_bank = transaction.get("to_bank")
        try:
            amount = float(transaction.get("amount_paid", 0))
        except (TypeError, ValueError):
            amount = 0.0

        origin_key = (origin_bank, origin_account)
        destination_key = (destination_bank, destination_account)

        if client_id not in self.origin_groups:
            self.origin_groups[client_id] = defaultdict(
                lambda: {"transactions": 0, "total_amount": 0.0, "destinations": {}}
            )
            self.destination_groups[client_id] = defaultdict(
                lambda: {"transactions": 0, "total_amount": 0.0, "origins": {}}
            )

        origin_data = self.origin_groups[client_id][origin_key]
        origin_data["transactions"] += 1
        origin_data["total_amount"] += amount
        if destination_account is not None or destination_bank is not None:
            self.origin_groups[client_id][origin_key]["destinations"][destination_key] = (
                origin_data["destinations"].get(destination_key, 0) + 1
            )

    def _process_eof(self, deserialized_message):
        client_id = deserialized_message.get("client_id")
        if client_id is None:
            return

        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1
        if self.eof_count[client_id] < FILTER_DATE_AMOUNT:
            return

        self._print_results(client_id)
        self.origin_groups.pop(client_id, None)
        self.eof_count.pop(client_id, None)

    def _format_node(self, node_key):
        bank, account = node_key
        return f"bank={bank or 'unknown'} account={account or 'unknown'}"

    def _format_edges(self, edges):
        return {
            self._format_node(node): count for node, count in edges.items()
        }

    def _get_second_level_destinations(self, client_id, origin_key):
        direct_destinations = self.origin_groups[client_id][origin_key]["destinations"]
        second_level = {}
        for dest_key, direct_count in direct_destinations.items():
            if dest_key == origin_key:
                continue
            dest_origin_data = self.origin_groups[client_id].get(dest_key)
            if not dest_origin_data:
                continue
            for next_dest_key, next_count in dest_origin_data["destinations"].items():
                if next_dest_key == origin_key:
                    continue
                if next_dest_key == dest_key:
                    continue
                second_level[next_dest_key] = second_level.get(next_dest_key, 0) + (
                    direct_count * next_count
                )
        return second_level

    def _print_results(self, client_id):
        origins = self.origin_groups.get(client_id, {})

        logging.info(f"Q4 Graph results for client {client_id}")
        print(f"Q4 Graph results for client {client_id}")
        print("Accounts with second level transactions:")
        for origin, data in origins.items():
            second_level_destinations = self._get_second_level_destinations(client_id, origin)
            if not second_level_destinations:
                continue
            print(
                f"  {self._format_node(origin)} transactions={data['transactions']} total_amount={data['total_amount']} destinations={self._format_edges(second_level_destinations)}"
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

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    graph_filter = GraphFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: graph_filter.stop())
    graph_filter.start()
    graph_filter.close()
    return 0


if __name__ == "__main__":
    main()
