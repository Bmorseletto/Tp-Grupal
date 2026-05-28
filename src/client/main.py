import os
import logging
import csv
import socket
import signal
import threading
from pathlib import Path
from common import message_protocol

TRANSACTIONS_INPUT_FILE = os.environ["TRANSACTIONS_INPUT_FILE"]
ACCOUNTS_INPUT_FILE = os.environ["ACCOUNTS_INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


class Client:

    def __init__(self):
        self.closed = False
        self._socket_lock = threading.Lock()
        self._send_error = None
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

    def _send_and_wait_ack(self, msg_type, *args):
        with self._socket_lock:
            if self._send_error:
                raise self._send_error
            message_protocol.external.send_msg(
                self.server_socket, msg_type, *args
            )
            message_protocol.external.recv_msg(self.server_socket)

    def _send_account_records(self, accounts_file):
        try:
            logging.info("Sending Account records")
            with open(accounts_file, newline="\n") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                _headers = next(csv_reader)
                for row in csv_reader:
                    logging.info(f"ROW: {row}")
                    (
                        bank_name,
                        bank_id,
                        accout_number,
                        entity_id,
                        entity_name,
                    ) = row
                    accounts = message_protocol.types.AccountRecord(
                        bank_name,
                        bank_id,
                        accout_number,
                        entity_id,
                        entity_name,
                    )
                    self._send_and_wait_ack(
                        message_protocol.external.MsgType.ACCOUNT_RECORD,
                        accounts
                    )
            logging.info("Sending Accounts EOF")
            self._send_and_wait_ack(
                message_protocol.external.MsgType.END_OF_ACCOUNTS
            )
            logging.info("accounts sent")
        except Exception as e:
            self._send_error = e

    def send_transaction_records(self, transactions_file):
        logging.info("Sending transaction records")
        with open(transactions_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            _headers = next(csv_reader)
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
                self._send_and_wait_ack(
                    message_protocol.external.MsgType.TRANSACTION_RECORD,
                    transaction
                )
        logging.info("Sending transaction EOF")
        self._send_and_wait_ack(
            message_protocol.external.MsgType.END_OF_TRANSACTIONS
        )
        logging.info("transactions sent")

    def recv_results(self, output_file):
        filepath = Path(output_file)
        name = filepath.name
        while True:
            logging.info("Receiving query result")
            msg_type, payload = message_protocol.external.recv_msg(self.server_socket)
            logging.info(f"Received message type: {msg_type}")
            if msg_type == message_protocol.external.MsgType.RESULTS:
                message_protocol.external.send_msg(
                    self.server_socket, message_protocol.external.MsgType.ACK
                )
                for query_id, query_results in payload.items():
                    with open(filepath.with_name(f"{query_id}_{name}"), "w") as csvfile:
                        csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
                        for row in query_results:
                            csv_writer.writerow(row)
            elif msg_type == message_protocol.external.MsgType.END_OF_RESULTS:
                message_protocol.external.send_msg(
                    self.server_socket, message_protocol.external.MsgType.ACK
                )
                break
            else:
                raise TypeError(f"Unexpected message type: {msg_type}")


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        logging.info("Connected to server")

        account_thread = threading.Thread(
            target=client._send_account_records,
            args=(ACCOUNTS_INPUT_FILE,),
            daemon=True,
        )
        account_thread.start()

        client.send_transaction_records(TRANSACTIONS_INPUT_FILE)

        account_thread.join()
        if client._send_error:
            raise client._send_error

        client.recv_results(OUTPUT_FILE)
        logging.info("results received")
    except socket.error as e:
        if not client.closed:
            logging.error(f"The connection with the server was lost {e}")
            return 1
    except Exception:
        logging.exception("An error occurred while running the client")
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
