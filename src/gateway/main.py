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
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AMOUNT_CURRENCY_FILTERS = int(os.environ["AMOUNT_CURRENCY"])
CURRENCY_PREFIX = os.environ["CURRENCY_PREFIX"]
Q5_PREFIX = os.environ["Q5_PREFIX"]
AMOUNT_Q5 = int(os.environ["AMOUNT_Q5"])
QUERY_AMOUNT = int(os.environ["QUERY_AMOUNT"])
RESULT_QUEUES = os.environ["RESULT_QUEUES"]


def handle_client_request(client_socket, message_handler):
    routing_keys = [CURRENCY_PREFIX] + [str(i) for i in range(AMOUNT_CURRENCY_FILTERS)]
    routing_keys_converter = [Q5_PREFIX]
    routing_keys_converter.extend(f"{Q5_PREFIX}{i}" for i in range(AMOUNT_Q5))
    data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
        MOM_HOST, CURRENCY_PREFIX, routing_keys
    )
    data_output_exchange_converter = middleware.MessageMiddlewareExchangeRabbitMQ(
        MOM_HOST, Q5_PREFIX, routing_keys_converter
    )
    accounts_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
        MOM_HOST, OUTPUT_QUEUE
    )
    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)
            if message[0] == message_protocol.external.MsgType.TRANSACTION_RECORD:
                serialized_message = message_handler.serialize_transaction_message(message[1])
                routing_key = str(zlib.crc32(message[1].account.encode('utf-8')) % AMOUNT_CURRENCY_FILTERS)
                data_output_exchange.send_by_key(serialized_message, routing_key)
                routing_key_converter = Q5_PREFIX+str(zlib.crc32(message[1].account.encode('utf-8')) % AMOUNT_Q5)
                data_output_exchange_converter.send_by_key(serialized_message, routing_key_converter)

            elif message[0] == message_protocol.external.MsgType.ACCOUNT_RECORD:
                serialized_message = message_handler.serialize_account_message(message[1])
                accounts_output_queue.send(serialized_message)

            elif message[0] == message_protocol.external.MsgType.END_OF_TRANSACTIONS:
                logging.info("Processing Transactions EOF")
                serialized_message = message_handler.serialize_eof_message()
                data_output_exchange.send_by_key(serialized_message, CURRENCY_PREFIX)
                deserialized_message = message_protocol.internal.deserialize(serialized_message)
                serialized_message=message_protocol.internal.serialize(
                    {"nodo_id": 0, "client_id": deserialized_message[0]}
                )
                data_output_exchange_converter.send_by_key(serialized_message, Q5_PREFIX)

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
        logging.exception("An error occurred while processing the client's request")
    finally:
        data_output_exchange.close()
        accounts_output_queue.close()


def handle_client_response(client_list, queries_remaining):
    logging.basicConfig(level=logging.INFO)
    consumer = middleware.MultiQueueConsumer(MOM_HOST)
    for queue_name in RESULT_QUEUES.split(","):
        queue_name = queue_name.strip()
        if queue_name:
            consumer.add_queue(queue_name, _make_result_callback(client_list, queries_remaining))
    consumer.start_consuming()
    consumer.close()


def _make_result_callback(client_list, queries_remaining):
    def _consume_result(message, ack, nack):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            client_id = deserialized[0]
            query_id = deserialized[1]
            results = deserialized[2]
            
            logging.info(f"Received {query_id} results for client {client_id}")

            target_index = None
            target_socket = None
            for i, [mh, sock] in enumerate(client_list):
                if mh.client_id == client_id:
                    target_index = i
                    target_socket = sock
                    break

            if target_index is None:
                logging.warning(f"no matching client for result {client_id}")
                nack()
                return

            message_protocol.external.send_msg(
                target_socket,
                message_protocol.external.MsgType.RESULTS,
                {query_id: results},
            )

            queries_remaining[client_id] = queries_remaining.get(client_id, QUERY_AMOUNT) - 1
            if queries_remaining[client_id] <= 0:
                message_protocol.external.send_msg(
                    target_socket,
                    message_protocol.external.MsgType.END_OF_RESULTS,
                )
                client_list.pop(target_index)
                del queries_remaining[client_id]
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            if target_index is not None:
                client_list.pop(target_index)
            ack()
        except Exception:
            logging.exception("An error occurred while processing the client's response")
            nack()

    return _consume_result


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        queries_remaining = manager.dict()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list, queries_remaining))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
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
