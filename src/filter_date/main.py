from datetime import datetime
import os
import logging
import signal
import zlib

from common import middleware, message_protocol, heartbeat
from common.wal import WAL

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
FILTER_PREFIX = os.environ["FILTER_PREFIX"]
OUTPUTS_PREFIX = os.environ["OUTPUTS_PREFIX"]
OUTPUTS_AMOUNTS = os.environ["OUTPUTS_AMOUNTS"]
ROUTING_HASH_TARGET = os.environ["ROUTING_HASH_TARGET"]
INITIAL_DATE = os.environ["INITIAL_DATE"]
UPSTREAM_AMOUNT = int(os.environ["UPSTREAM_AMOUNT"])
END_DATE = os.environ["END_DATE"]
MANAGER_HOSTS = os.environ["MANAGER_HOSTS"].split(",")
MANAGER_PORT = int(os.environ["MANAGER_PORT"])
NODE_NAME = os.environ["NODE_NAME"]
WAL_DIR = os.environ.get("WAL_DIR", f"/wal/{FILTER_PREFIX}_{ID}")


class DateFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"], ID
        )
        logging.info(f"PERSONAL ROUTING KEY: {FILTER_PREFIX + f'{ID}'}")
        self.outputs_prefix = OUTPUTS_PREFIX.split(",")
        self.outputs_amounts = list(map(int, OUTPUTS_AMOUNTS.split(",")))
        self.routing_hash_targets = ROUTING_HASH_TARGET.split(",")
        self.output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                self.outputs_prefix[i],
                [],
                ID,
                publish_only=True,
                source_id=f"{FILTER_PREFIX}_{ID}",
            )
            for i in range(len(self.outputs_prefix))
        ]
        self.eof_count = {}
        self.heartbeats = []
        for manager_host in MANAGER_HOSTS:
            self.heartbeats.append(heartbeat.Heartbeat(NODE_NAME, manager_host, MANAGER_PORT))
        self.wal = WAL(WAL_DIR)
        state, _, _ = self.wal.backup_load(default=({"eof": {}, "__msg_counters": {}}, 0, set()))
        self.eof_count = {str(k): v for k, v in state["eof"].items()}
        state["eof"] = self.eof_count
        middleware._init_msg_id_counters(state.get("__msg_counters", {}))
        self._orphans = self.wal.recover(self._wal_apply, state)
        for cid in list(self.eof_count):
            self._try_send_eof(cid)

    @staticmethod
    def _wal_apply(entry, state):
        cid = str(entry["client_id"])
        if entry["type"] == "eof_count":
            state["eof"][cid] = entry["count"]

    def _process_data(self, transaction):
        transaction_timestamp = datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M").replace(hour=0, minute=0, second=0, microsecond=0)
        initial_date = datetime.strptime(INITIAL_DATE, "%Y/%m/%d")
        end_date = datetime.strptime(END_DATE, "%Y/%m/%d")

        if initial_date <= transaction_timestamp <= end_date:
            for i in range(len(self.output_exchanges)):
                if '+' in self.routing_hash_targets[i]:
                    routing_key = (
                        self.outputs_prefix[i]
                        + str(
                            zlib.crc32(f"{transaction['from_bank']}:{transaction['account']}".encode("utf-8"))
                            % self.outputs_amounts[i]
                        )
                    )
                    self.output_exchanges[i].send_by_key(
                        message_protocol.internal.serialize(transaction), routing_key
                    )
                else:
                    routing_key = (
                        self.outputs_prefix[i]
                        + str(
                            zlib.crc32(transaction[self.routing_hash_targets[i]].encode("utf-8"))
                            % self.outputs_amounts[i]
                        )
                    )
                    self.output_exchanges[i].send_by_key(
                        message_protocol.internal.serialize(transaction), routing_key
                    )

    def _try_send_eof(self, client_id):
        if self.eof_count.get(client_id, 0) < UPSTREAM_AMOUNT:
            return
        for i, output_exchange in enumerate(self.output_exchanges):
            output_exchange.send_by_key(
                message_protocol.internal.serialize(
                    {"nodo_id": ID, "client_id": int(client_id)}
                ),
                self.outputs_prefix[i],
            )

    def _process_eof(self, deserialized_message, msg_id=None):
        client_id = str(deserialized_message["client_id"])
        current_count = self.eof_count.get(client_id, 0)
        if current_count >= UPSTREAM_AMOUNT:
            return
        self.eof_count[client_id] = current_count + 1
        self.wal.append(msg_id, {"type": "eof_count", "client_id": client_id, "count": self.eof_count[client_id]})
        if self.eof_count[client_id] < UPSTREAM_AMOUNT:
            return
        self._try_send_eof(client_id)

    def process_messsage(self, message, ack, nack, ctx):
        msg_id = ctx.get("msg_id")
        deserialized_message = message_protocol.internal.deserialize(message)
        is_eof = len(deserialized_message) == 2
        if msg_id and msg_id in self.wal.processed_ids:
            ack()
            return
        try:
            if is_eof:
                self._process_eof(deserialized_message, msg_id)
            else:
                self._process_data(deserialized_message)
            if is_eof and msg_id:
                self.wal.processed_ids.add(msg_id)
            if self.wal.is_checkpoint_necessary():
                self.wal.checkpoint({"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()})
            ack()
        except Exception:
            logging.exception("error processing message")
            nack()

    def start(self):
        for heartbeat in self.heartbeats:
            heartbeat.start()
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        self.wal.backup_save({"eof": self.eof_count, "__msg_counters": middleware.get_msg_id_counters()}, self.wal.last_seq())
        self.input_exchange.stop_consuming()
        for heartbeat in self.heartbeats:
            heartbeat.stop()

    def close(self):
        self.wal.close()
        self.input_exchange.close()
        for exchange in self.output_exchanges:
            exchange.close()


def main():
    logging.basicConfig(level=logging.INFO)
    date_filter = DateFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: date_filter.stop(),
    )
    date_filter.start()
    date_filter.close()
    return 0


if __name__ == "__main__":
    main()
