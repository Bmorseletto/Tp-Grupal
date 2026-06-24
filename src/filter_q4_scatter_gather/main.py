from collections import defaultdict
import os
import logging
import signal
import time
from common import middleware, message_protocol, heartbeat
from common.client_state_ttl import ClientStateTTL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SCATTER_DETECTOR_PREFIX = os.environ["SCATTER_DETECTOR_PREFIX"]
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]
OUTPUT_AMOUNT = int(os.environ["OUTPUT_AMOUNT"])
Q4_GRAPH_AMOUNT = int(os.environ["Q4_GRAPH_AMOUNT"])
SCATTER_DETECTOR_STORAGE = "/output/q4_scatter_"
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])

NODE_NAME =  os.environ["NODE_NAME"]


class ScatterGatherDetector:
    def __init__(self):

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SCATTER_DETECTOR_PREFIX,
            [SCATTER_DETECTOR_PREFIX, SCATTER_DETECTOR_PREFIX + str(ID)],
            ID
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_PREFIX
        )
        self.suspicious_accounts = {}
        self.accounts = {}
        self.eof_count = {}
        self.results = {}
        self.client_state_ttl = ClientStateTTL()
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))

    def _process_data(self, result):
        client_id = result.get("client_id")
        if client_id is None:
            return
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)
        self.client_state_ttl.update_last_seen(client_id)
        if client_id not in self.results:
            self.results[client_id] = []
        self.results[client_id].append(result)

    def _process_eof(self, message):
        client_id = message.get("client_id")
        if client_id is None:
            return

        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)
        self.client_state_ttl.update_last_seen(client_id)

        self.eof_count[client_id] = self.eof_count.get(client_id, 0) + 1

        if self.eof_count[client_id] < Q4_GRAPH_AMOUNT:
            return

        # final_dict = {
        #   (origin_bank, origin_account): {
        #       (destination_bank_1, destination_account_1): <amount of transactions>,
        #       (destination_bank_2, destination_account_2): <amount of transactions>
        #   }
        # }
        final_dict = {}
        suspicious = self.suspicious_accounts.get(client_id, {})
        accounts = self.accounts.get(client_id, {})
        if suspicious and accounts:
            for account, transactions in suspicious.items():
                for middle_man, transaction in transactions.items():
                    for final_account, final_transactions in accounts.items():
                        if middle_man in final_transactions.keys():
                            if account not in final_dict:
                                final_dict[account] = {}
                            if final_account not in final_dict[account]:
                                final_dict[account][final_account] = 0
                            final_dict[account][final_account] += 1
        self.output_queue.send(message_protocol.internal.serialize({"client_id": client_id, "suspicious_accounts": final_dict}))
        self.output_queue.send( message_protocol.internal.serialize(
            {"nodo_id": ID, "client_id": client_id}
        ))
        self.results.pop(client_id, None)
        self.eof_count.pop(client_id, None)
        self.suspicious_accounts.pop(client_id, None)
        self.accounts.pop(client_id, None)
        self.client_state_ttl.remove(client_id)

    def _expire_client_state(self, client_id):
        self.results.pop(client_id, None)
        self.eof_count.pop(client_id, None)
        self.suspicious_accounts.pop(client_id, None)
        self.accounts.pop(client_id, None)

    def _cleanup_expired_clients(self):
        self.client_state_ttl.cleanup_expired_clients(self._expire_client_state)

    def _update_last_seen(self, client_id):
        self.client_state_ttl.update_last_seen(client_id)

    def process_message(self, message, ack, nack):
        deserialized = message_protocol.internal.deserialize(message)
        if len(deserialized) == 2:
            self._process_eof(deserialized)
        elif "origin_account" in deserialized.keys():
            client_id = deserialized.pop("client_id")
            self._cleanup_expired_clients()
            self._update_last_seen(client_id)
            if client_id not in self.suspicious_accounts:
                self.suspicious_accounts[client_id] = {}
            self.suspicious_accounts[client_id][deserialized["origin_account"]] = deserialized["transactions"]
        elif "destination_account" in deserialized.keys():
            client_id = deserialized.pop("client_id")
            self._cleanup_expired_clients()
            self._update_last_seen(client_id)
            if client_id not in self.accounts:
                self.accounts[client_id] = defaultdict(dict)
            self.accounts[client_id][deserialized["destination_account"]].update(deserialized["transactions"])
        ack()

    def start(self):
        for heartbeat in self.heartbeats:
                heartbeat.start()
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        self.output_queue.close()

    def stop(self):
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()
        self.client_state_ttl.clear()

    def close(self):
        try:
            if self.input_exchange:
                self.input_exchange.close()
        except Exception as e:
            logging.warning(f"Input exchange ya estaba cerrado: {e}")

        try:
            if self.output_queue:
                self.output_queue.close()
        except Exception as e:
            logging.warning(f"Output queue ya estaba cerrada: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    scatter_detector = ScatterGatherDetector()
    signal.signal(signal.SIGTERM, lambda signum, frame: scatter_detector.stop())
    scatter_detector.start()
    scatter_detector.close()
    return 0

if __name__ == "__main__":
    main()