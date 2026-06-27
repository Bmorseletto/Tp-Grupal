import json
import multiprocessing
import os
import logging
import shutil
import signal
import socket
import subprocess
import time
import uuid
from asyncio import IncompleteReadError

from common import middleware, message_protocol
from manager_intercomm import NodeManagerIntercomm, WORKING, VOTING

MOM_HOST = os.environ["MOM_HOST"]
MANAGER_HOST = os.environ["MANAGER_HOST"]
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
SOCKET_TIMEOUT = 6
ID = int(os.environ["ID"])
NODES = [
    "currency_filter_0", "currency_filter_1", "currency_filter_2",
    "dollar_amount_filter_0", "dollar_amount_filter_1", "dollar_amount_filter_2",
    "query_1_aggregator",
    "query_2_aggregator",
    "max_transaction_filter_0", "max_transaction_filter_1", "max_transaction_filter_2",
    "query_3_aggregator",
    "query_3_avg_filter_0", "query_3_avg_filter_1", "query_3_avg_filter_2",
    "query_3_avg_calc_0", "query_3_avg_calc_1", "query_3_avg_calc_2",
    "query_3_date_filter_0", "query_3_date_filter_1", "query_3_date_filter_2",
    "date_filter_0", "date_filter_1", "date_filter_2",
    "query_4_graph_0", "query_4_graph_1", "query_4_graph_2",
    "query_4_scatter_detector_0", "query_4_scatter_detector_1", "query_4_scatter_detector_2",
    "query_4_agg",
    "usd_converter_0", "usd_converter_1", "usd_converter_2",
    "query_5_aggregator",
    "query_5_date_filter_0", "query_5_date_filter_1", "query_5_date_filter_2",
]
   

class NodeManager:
    def __init__(self):
        self.processes = {}
    def start(self):
        with multiprocessing.Manager() as manager:
            sigterm_received = manager.Value("c_short", 0)
            status = manager.Value(int, WORKING)
            is_leader = manager.Value(bool,  ID == 0)
            is_leader_barrier = manager.Event()
            status_gate = manager.Event()
            status_gate.set()
            down_containers = manager.list()
            for i in NODES:
                down_containers.append(i)
            lock = manager.Lock()
            intercomm = NodeManagerIntercomm(ID)
            intercomm_process = multiprocessing.Process(
                target=intercomm.start,
                args=(status, status_gate, is_leader, MOM_HOST, is_leader_barrier),
                daemon=True
            )
            start_containers_process = multiprocessing.Process(
                target=start_containers,
                args=(down_containers, is_leader_barrier, lock),
                daemon=True
            )
            intercomm_process.start()
            start_containers_process.start()
            return_value = 0
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
                while True:
                    to_pop = []
                    for node_uuid, process in self.processes.items():
                        if process.is_alive() == False:
                            process.join()
                            to_pop.append(node_uuid)
                    for node_uuid in to_pop:
                        del self.processes[node_uuid]
                    try:
                        node_socket, _ = server_socket.accept()
                        node_socket.settimeout(SOCKET_TIMEOUT)
                        node_id = uuid.uuid4().int
                        with lock:
                                node_dict[node_id] = node_socket
                        process = multiprocessing.Process(
                            target=_handle_node,
                            args=(node_socket, node_id, node_dict, lock, status_gate, is_leader, sigterm_received, down_containers),
                        )
                        process.start()
                        self.processes[node_id] = process
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return_value =  1
                            break
                        else:
                            return_value =  0
                            break
                    except Exception:
                        logging.exception("An error occurred while accepting a new client connection")
                        return_value = 2
                        break
                to_pop = []
                for node_uuid, process in self.processes.items():
                        if process.is_alive():
                            process.terminate()
                        process.join()
                        to_pop.append(node_uuid)
                for node_uuid in to_pop:
                    del self.processes[node_uuid]
                if intercomm_process.is_alive():
                    intercomm_process.terminate()
                    intercomm_process.join()
                if start_containers_process.is_alive():
                    start_containers_process.terminate()
                    start_containers_process.join()
                return return_value
                

def handle_sigterm(server_socket, node_dict, sigterm_received):
    sigterm_received.value = 1
    try:
        server_socket.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    for node_socket in node_dict.values():
        try:
            node_socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

def _handle_node(node_socket, node_uuid, node_dict, lock, status_gate, is_leader, sigterm_received, down_containers):
    logging.basicConfig(level=logging.INFO)
    node_id = ""
    timeout_counter = 0
    node_socket.settimeout(SOCKET_TIMEOUT)
    try:
        while True:
            try:
                test = False
                message = message_protocol.external.recv_msg(node_socket)
                if message[1] == "":
                    logging.info(f"{node_id} is closing gracefully")
                    node_id = ""
                    break
                if node_id == "":
                    with lock:
                        if message[1] in down_containers:
                            down_containers.remove(message[1])
                    if is_leader.value:
                        logging.info(f"Heartbeat recived: {message[1]}")
                node_id = message[1]                    
                timeout_counter = 0
            except socket.timeout:
                logging.info(f"Heartbeat timeout for node: {node_id}")
                if timeout_counter == 3:
                    logging.info(f"retries finished node is down")
                    break
                else:
                    timeout_counter+=1
            except (IncompleteReadError, socket.error, ConnectionResetError, OSError):
                if sigterm_received.value != 1:
                    logging.warning(f"Conexión perdida abruptamente con el nodo '{node_id}'")
                break
    except Exception as e:
        logging.exception(f"Error handling node {node_id}: {e}")
    finally:
        with lock:
            try:
                node_socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            node_socket.close()
            
            if node_uuid in node_dict:
                del node_dict[node_uuid]
    
    if node_id != ""  and sigterm_received.value == 0:
        status_gate.wait()
        with lock:
                if node_id not in down_containers:
                    down_containers.append(node_id)
            

    
def start_containers(down_containers, is_leader_barrier, lock):
    while True:
        is_leader_barrier.wait()
        with lock:
            for container in down_containers:
                logging.info(f"restarting container {container}")
                resultado = subprocess.run(
                    ['docker', 'start', container], 
                    check=False, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    text=True
                )
                if resultado.returncode == 0:
                    logging.info(f"Servicio {container} levantado.")
                else:
                    logging.info(f"Compose falló para {container}. Stderr: {resultado.stderr}")
            down_containers.clear()
        time.sleep(3)


def main():
    try:
        logging.basicConfig(level=logging.INFO)
        manager = NodeManager()
        return manager.start()
    except Exception:
        logging.exception("An error occurred while running the join node")


if __name__ == "__main__":
    main()
