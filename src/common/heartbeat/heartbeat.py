import multiprocessing
import socket
import time

from common import message_protocol

class Heartbeat:
    def __init__(self, node_id, manager_host,manager_port):
        self.node_id = node_id
        self.manager_host= manager_host
        self.manager_port = manager_port
        self.heartbeat_process = None

    def start(self):
            self.heartbeat_process = multiprocessing.Process(
                target=_heartbeat, 
                args=(self.manager_host, self.manager_port, self.node_id,), daemon=True
            )
            self.heartbeat_process.start()

    def stop(self):
        if self.heartbeat_process != None:
           self.heartbeat_process.terminate()
           self.heartbeat_process.join()

def _heartbeat(manager_host, manager_port, node_id):
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((manager_host, manager_port))
                while True:
                    message_protocol.external.send_msg(
                        s, message_protocol.external.MsgType.HEARTBEAT, node_id
                    )
                    time.sleep(5)
        except Exception as e:
            time.sleep(5)