import json
import os
import logging
import shutil
import signal

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
QUERY_AMOUNT = int(os.environ["QUERY_AMOUNT"])
STATE_DIR = os.environ.get("JOIN_STATE_DIR", "/output/join_state")


class JoinNode:
    def __init__(self):
        self.input_queue = middleware.MultiQueueConsumer(MOM_HOST)
        for queue_name in os.environ["INPUT_QUEUES"].split(","):
            queue_name = queue_name.strip()
            self.input_queue.add_queue(queue_name, self._on_message)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.received_queries = {}
        self._recover_state()

    def _client_dir(self, client_id):
        return os.path.join(STATE_DIR, str(client_id))

    def _query_filepath(self, client_id, query_id):
        return os.path.join(self._client_dir(client_id), f"{query_id}.json")

    def _persist_result(self, client_id, query_id, results):
        client_dir = self._client_dir(client_id)
        os.makedirs(client_dir, exist_ok=True)
        with open(self._query_filepath(client_id, query_id), "w") as f:
            json.dump(results, f)

    def _load_consolidated(self, client_id):
        consolidated = {}
        client_dir = self._client_dir(client_id)
        for query_id in self.received_queries[client_id]:
            with open(self._query_filepath(client_id, query_id), "r") as f:
                consolidated[query_id] = json.load(f)
        return consolidated

    def _cleanup_client(self, client_id):
        client_dir = self._client_dir(client_id)
        shutil.rmtree(client_dir, ignore_errors=True)

    def _recover_state(self):
        if not os.path.exists(STATE_DIR):
            return
        for client_id_str in os.listdir(STATE_DIR):
            client_id = int(client_id_str)
            client_dir = os.path.join(STATE_DIR, client_id_str)
            if not os.path.isdir(client_dir):
                continue
            query_ids = set()
            for fname in os.listdir(client_dir):
                if fname.endswith(".json"):
                    query_ids.add(fname[:-5])
            if query_ids:
                self.received_queries[client_id] = query_ids
                logging.info(
                    f"recovered state for client {client_id}: {query_ids}"
                )
                if len(query_ids) == QUERY_AMOUNT:
                    logging.warning(
                        f"client {client_id} had all results on disk but was not sent; will resend"
                    )
                    consolidated = self._load_consolidated(client_id)
                    self.output_queue.send(
                        message_protocol.internal.serialize(
                            [client_id, consolidated]
                        )
                    )
                    self._cleanup_client(client_id)
                    self.received_queries.pop(client_id, None)

    def _on_message(self, message, ack, nack):
        try:
            deserialized = message_protocol.internal.deserialize(message)
            logging.debug(f"Received message: {deserialized}")
            client_id = deserialized[0]
            query_id = deserialized[1]
            results = deserialized[2]
            logging.info(f"received {query_id} results for client {client_id}")
            self._persist_result(client_id, query_id, results)
            if client_id not in self.received_queries:
                self.received_queries[client_id] = set()
            self.received_queries[client_id].add(query_id)
            if len(self.received_queries[client_id]) == QUERY_AMOUNT:
                consolidated = self._load_consolidated(client_id)
                self.output_queue.send(
                    message_protocol.internal.serialize(
                        [client_id, consolidated]
                    )
                )
                self._cleanup_client(client_id)
                self.received_queries.pop(client_id)
                logging.info(f"sent consolidated results for client {client_id}")
            ack()
        except Exception as e:
            logging.exception(f"error processing message: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming()

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        self.input_queue.close()
        self.output_queue.close()


def main():
    try:
        logging.basicConfig(level=logging.DEBUG)
        join = JoinNode()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: join.stop(),
        )
        join.start()
        join.close()
        return 0
    except Exception:
        logging.exception("An error occurred while running the join node")


if __name__ == "__main__":
    main()
