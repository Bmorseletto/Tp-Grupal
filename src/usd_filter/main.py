import os
import logging
import bisect
import signal

from common import middleware, message_protocol
import zlib

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
# OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
# FILTER_Q1_AMOUNT = int(os.environ["FILTER_Q1_AMOUNT"])
# FILTER_Q1_PREFIX = os.environ["FILTER_Q1_PREFIX"]
# FILTER_Q2_AMOUNT = int(os.environ["FILTER_Q2_AMOUNT"])
# FILTER_Q2_PREFIX = os.environ["FILTER_Q2_PREFIX"]
DONE = True
WORKING = False
TOTAL_QUERIES = 2


class CurrencyFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", f"{ID}"]
        )
        self.filter_q_prefixes = [
            os.environ[f"FILTER_Q{i}_PREFIX"] for i in range(1, TOTAL_QUERIES + 1)
        ]
        self.filter_q_amounts = [
            int(os.environ[f"FILTER_Q{i}_AMOUNT"]) for i in range(1, TOTAL_QUERIES + 1)
        ]
        self.output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                self.filter_q_prefixes[i],
                [self.filter_q_prefixes[i]]
                + [
                    self.filter_q_prefixes[i] + str(j)
                    for j in range(self.filter_q_amounts[i])
                ],
            )
            for i in range(TOTAL_QUERIES)
        ]

    def _process_data(
        self, transaction
    ):
        for i in range(TOTAL_QUERIES):
            send_to_query_i = getattr(self, f"_send_to_query_{i+1}")
            send_to_query_i(transaction)

    def _send_to_query_1(self, transaction):
        if transaction["payment_currency"] == "US Dollar":
            output = {
                "client_id": transaction["client_id"],
                "account": transaction["account"],
                "to_account": transaction["to_account"],
                "amount_paid": transaction["amount_paid"],
            }
            routing_key = (
                self.filter_q_prefixes[0]
                + str(
                    zlib.crc32(output["account"].encode("utf-8"))
                    % self.filter_q_amounts[0]
                )
            )  # Usamos la account de origen y la cantidad de filtros Q1 para routear el mensaje
            logging.info(f"routing key for Q1 {routing_key}")
            self.output_exchanges[0].send_by_key(
                message_protocol.internal.serialize(output), str(routing_key)
            )

    def _send_to_query_2(self, transaction):
        if transaction["payment_currency"] == "US Dollar":
            output = {
                "client_id": transaction["client_id"],
                "account": transaction["account"],
                "amount_paid": transaction["amount_paid"],
                "from_bank": transaction["from_bank"],
            }
            routing_key = (
                self.filter_q_prefixes[1]
                + str(
                    zlib.crc32(output["from_bank"].encode("utf-8"))
                    % self.filter_q_amounts[1]
                )
            )  # Usamos el banco y la cantidad de filtros Q2 para routear  las transacciones del mismo banco siempre al mismo nodo
            logging.info(f"routing key for Q2 {routing_key}")
            self.output_exchanges[1].send_by_key(
                message_protocol.internal.serialize(output), routing_key
            )

    def _process_eof(self, deserialized_message):
        logging.info("sending eof to next node")
        for i, output_exchange in enumerate(self.output_exchanges):
            output_exchange.send_by_key(
                message_protocol.internal.serialize(
                    {"nodo_id": ID, "client_id": deserialized_message[0]}
                ),
                self.filter_q_prefixes[i],
            )

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        logging.debug(f"MESSAGE: {deserialized_message}")
        if len(deserialized_message) == 1:
            self._process_eof(deserialized_message)
        else:
            self._process_data(deserialized_message)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_exchanges[0].close()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_exchanges[0].close()


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
