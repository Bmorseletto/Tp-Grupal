import multiprocessing
import signal
import socket
import threading
import time

from common import message_protocol
HEARTBEAT_SLEEP = 2

class Heartbeat:
    def __init__(self, node_id, manager_host,manager_port):
        self.node_id = node_id
        self.manager_host= manager_host
        self.manager_port = manager_port
        self.heartbeat_thread = None
        self.beat = False
        self.stop_event = threading.Event()
        self.beat = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.heartbeat_thread.start()

    def start(self):
            pass
            self.beat = True
            self.heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
            self.heartbeat_thread.start()

    def stop(self):
        self.stop_event.set()
        if self.heartbeat_thread:
           self.heartbeat_thread.join()

    def _heartbeat(self):
            while not self.stop_event.is_set():
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as manager_socket:
                        manager_socket.settimeout(5)
                        manager_socket.connect((self.manager_host, self.manager_port))
                        
                        while not self.stop_event.is_set():
                            message_protocol.external.send_msg(
                                manager_socket, message_protocol.external.MsgType.HEARTBEAT, self.node_id
                            )
                            self.stop_event.wait(timeout=5)
                        try:
                            message_protocol.external.send_msg(
                                manager_socket, message_protocol.external.MsgType.HEARTBEAT, ""
                            )
                        except Exception:
                            pass 
                            
                except Exception as e:
                    self.stop_event.wait(timeout=HEARTBEAT_SLEEP)