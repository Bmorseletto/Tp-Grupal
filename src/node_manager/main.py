import json
import multiprocessing
import os
import logging
import shutil
import signal
import socket
import subprocess
import uuid

from common import middleware, message_protocol
from manager_intercomm import NodeManagerIntercomm, WORKING, VOTING

MOM_HOST = os.environ["MOM_HOST"]
MANAGER_HOST = os.environ["MANAGER_HOST"]
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
SOCKET_TIMEOUT = 6
ID = int(os.environ["ID"])
   

class NodeManager:
    def __init__(self):
        pass
    def start(self):
        with multiprocessing.Manager() as manager:
            sigterm_received = manager.Value("c_short", 0)
            status = manager.Value(int, WORKING)
            is_leader = manager.Value(bool,  ID == 0)
            status_gate = manager.Event()
            status_gate.set()
            intercomm = NodeManagerIntercomm(ID)
            intercomm_process = multiprocessing.Process(
                target=intercomm.start,
                args=(status, status_gate, is_leader, MOM_HOST),
                daemon=True
            )
            intercomm_process.start()
            with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                    server_socket.bind((MANAGER_HOST, MANAGER_PORT))
                    server_socket.listen()
                    node_dict =  manager.dict()
                    signal.signal(
                        signal.SIGTERM,
                        lambda signum, frame: handle_sigterm(
                            server_socket, node_dict, sigterm_received
                        ),
                    )
                    lock = manager.Lock()
                    while True:
                        try:
                            node_socket, _ = server_socket.accept()
                            node_socket.settimeout(SOCKET_TIMEOUT)
                            node_id = uuid.uuid4().int
                            with lock:
                                 node_dict[node_id] = node_socket
                            processes_pool.apply_async(
                                _handle_node,
                                [node_socket, node_id, node_dict, lock, status_gate, is_leader],
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

def handle_sigterm(server_socket, node_dict, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for socket in node_dict.values():
        socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1

def _handle_node(node_socket, node_uuid, node_dict, lock, status_gate, is_leader):
    logging.basicConfig(level=logging.INFO)
    node_id = ""
    timeout_counter = 0
    node_socket.settimeout(SOCKET_TIMEOUT)
    while True:
        try:
            message = message_protocol.external.recv_msg(node_socket)
            if node_id == "":
                logging.info(f"Heartbeat recived: {node_id}")
            node_id = message[1]
        except socket.timeout:
            logging.info(f"Heartbeat timeout for node: {node_id}")
            if timeout_counter == 3:
                logging.info(f"retries finished node is down")
                break
            else:
                timeout_counter+=1
        except Exception as e:
            logging.exception(f"Error handling node {node_id}: {e}")
            break
    logging.info(f"This node is leader?: {is_leader}")
    if node_id != "" and is_leader.value:
        status_gate.wait()
        logging.info(f"restarting container {node_id}")
        resultado = subprocess.run(
            ['docker', 'restart', node_id], 
            check=False, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        logging.info(f"restart subprocess for {node_id} run, result: {resultado.returncode}, stdout:{resultado.stdout}")
        if resultado.returncode == 0:
            logging.info(f"Servicio {node_id} levantado.")
            with lock:
                node_socket.shutdown(socket.SHUT_RDWR)
                del node_dict[node_uuid]
        else:
            logging.info(f"Compose falló para {node_id}. Stderr: {resultado.stderr}")
        


def main():
    try:
        logging.basicConfig(level=logging.INFO)
        manager = NodeManager()
        manager.start()
        return 0
    except Exception:
        logging.exception("An error occurred while running the join node")


if __name__ == "__main__":
    main()
