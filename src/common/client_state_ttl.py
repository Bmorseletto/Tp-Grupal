import os
import time


class ClientStateTTL:
    def __init__(self, ttl_env_name: str = "CLIENT_STATE_TTL_SECONDS"):
        self.ttl_seconds = int(os.environ.get(ttl_env_name, "1800"))
        self.last_seen = {}

    def update_last_seen(self, client_id):
        if client_id is not None:
            self.last_seen[client_id] = time.time()

    def cleanup_expired_clients(self, cleanup_func):
        now = time.time()
        expired_clients = [client_id for client_id, last_seen in self.last_seen.items() if now - last_seen > self.ttl_seconds]
        for client_id in expired_clients:
            cleanup_func(client_id)
            self.last_seen.pop(client_id, None)

    def remove(self, client_id):
        self.last_seen.pop(client_id, None)

    def clear(self):
        self.last_seen.clear()
