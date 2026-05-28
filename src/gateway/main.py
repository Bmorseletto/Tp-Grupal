import os
import logging
import socket
import signal
import multiprocessing
import zlib
import message_handler
from common import middleware, message_protocol
from asyncio import IncompleteReadError

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AMOUNT_CURRENCY_FILTERS = int(os.environ["AMOUNT_CURRENCY"])
CURRENCY_PREFIX = os.environ["CURRENCY_PREFIX"]
FILTER_DATE_AMOUNT = int(os.environ["FILTER_DATE_AMOUNT"])
FILTER_DATE_PREFIX = os.environ["FILTER_DATE_PREFIX"]

# AMOUNT_RESULTS = 2


def handle_client_request(client_socket, message_handler):
    routing_keys = [CURRENCY_PREFIX] + [str(i) for i in range(AMOUNT_CURRENCY_FILTERS)]
    data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CURRENCY_PREFIX, routing_keys
        )
    accounts_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
    date_filter_exchange =  middleware.MessageMiddlewareExchangeRabbitMQ(
        MOM_HOST,
        FILTER_DATE_PREFIX,
        [FILTER_DATE_PREFIX]
        + [
            FILTER_DATE_PREFIX + str(j)
            for j in range(FILTER_DATE_AMOUNT)
        ],
    )
    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)
            # print(f"{message}", flush=True)
            # logging.info(f"Message: {message}")
            if message[0] == message_protocol.external.MsgType.TRANSACTION_RECORD:
                logging.info(f"Processing Transaction Record")
                serialized_message = message_handler.serialize_transaction_message(message[1])
                routing_key = str(zlib.crc32(message[1].account.encode('utf-8')) % AMOUNT_CURRENCY_FILTERS)
                data_output_exchange.send_by_key(serialized_message, routing_key)
                routing_key = (
                    FILTER_DATE_PREFIX
                    + str(
                        zlib.crc32(message[1].account.encode("utf-8"))
                        % FILTER_DATE_AMOUNT
                    )
                ) 
                logging.info(f"routing key for date {routing_key}")
                date_filter_exchange.send_by_key(serialized_message, routing_key)

            elif message[0] == message_protocol.external.MsgType.ACCOUNT_RECORD:
                logging.info(f"Processing Account Record")
                serialized_message = message_handler.serialize_account_message(message[1])
                accounts_output_queue.send(serialized_message)

            elif message[0] == message_protocol.external.MsgType.END_OF_TRANSACTIONS:
                logging.info("Processing Transactions EOF")
                serialized_message = message_handler.serialize_eof_message()
                data_output_exchange.send_by_key(serialized_message, CURRENCY_PREFIX)

            elif message[0] == message_protocol.external.MsgType.END_OF_ACCOUNTS:
                logging.info("Processing Accounts EOF")
                serialized_message = message_handler.serialize_eof_message()
                accounts_output_queue.send(serialized_message)
                
            # ACK
            message_protocol.external.send_msg(
                client_socket, message_protocol.external.MsgType.ACK
            )
    except socket.error:
        logging.error("The connection with the server was lost")
    except IncompleteReadError:
        logging.info("The client has closed the connection")
    except Exception:
        logging.exception(f"An error occurred while processing the client's request: {message}")
    finally:
        data_output_exchange.close()
        accounts_output_queue.close()


def handle_client_response(client_list, results_count):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        client_index = -1
        try:
            for i, [message_handler_instance, client_socket] in enumerate(client_list):
                result = message_handler_instance.deserialize_result_message(message)
                if not result:
                    continue
                client_index = i
                client_id, deserialized_message = result
                logging.info(f"Received results for {client_id}")

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.RESULTS,
                    deserialized_message,
                )
                # message_protocol.external.recv_msg(client_socket)
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.END_OF_RESULTS,
                )
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception:
            logging.exception("An error occurred while processing the client's response")
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        results_count = {}
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list, results_count))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception:
                        logging.exception("An error occurred while accepting a new client connection")
                        return 2
    return 0


if __name__ == "__main__":
    main()
