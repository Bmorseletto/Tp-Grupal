import os
import logging
import csv
import socket
import signal

from common import message_protocol

TRANSACTIONS_INPUT_FILE = os.environ["TRANSACTIONS_INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


class Client:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def send_transaction_records(self, input_file):
        logging.info("Sending transaction records")
        with open(input_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            for row in csv_reader:
                logging.info(f"ROW: {row}")
                (
                    timestamp,
                    og_bank,
                    og_account,
                    dest_bank,
                    dest_account,
                    amt_recv,
                    recv_currency,
                    amt_paid,
                    payment_currency,
                    payment_format,
                    is_laundering,
                ) = row
                transaction = message_protocol.types.TransactionRecord(
                    timestamp,
                    og_bank,
                    og_account,
                    dest_bank,
                    dest_account,
                    float(amt_paid),
                    payment_currency,
                    payment_format,
                )
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.TRANSACTION_RECORD,
                    transaction
                )
                logging.info("awating Transaction ACK")
                message_protocol.external.recv_msg(self.server_socket)
        logging.info("Sending transaction EOF")
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECODS
        )
        logging.info("awating ACK")
        message_protocol.external.recv_msg(self.server_socket)

    def recv_results(self, output_file):
        logging.info("Receiving query result")
        results = message_protocol.external.recv_msg(self.server_socket)
        logging.info("Received query result")
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.ACK
        )
        logging.info(f"RESULTS: {results}")
        if results[0] != message_protocol.external.MsgType.RESULTS:
            raise TypeError("Expected a RESULTS message")

        with open(output_file, "w") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
            for query_item in results[1]:
                csv_writer.writerow(query_item)


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        logging.info("Connected to server")
        client.send_transaction_records(TRANSACTIONS_INPUT_FILE)
        logging.info("transacftions sent")
        client.recv_results(OUTPUT_FILE)
        logging.info("results recived")
    except socket.error as e:
        if not client.closed:
            logging.error(f"The connection with the server was lost {e}")
            return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
