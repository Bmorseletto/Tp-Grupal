import pika
import logging
import threading
import time
import uuid
import json
import base64

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

CHUNK_SIZE = 98304
_CHUNK_MARKER = "__chunked"
_MSGID_KEY = "__msgid"


def _generate_msg_id(source_id):
    if not hasattr(_generate_msg_id, "_counters"):
        _generate_msg_id._counters = {}
    seq = _generate_msg_id._counters.get(source_id, 0)
    _generate_msg_id._counters[source_id] = seq + 1
    return f"{source_id}_{seq}"


def _build_properties(msg_id, delivery_mode=2):
    return pika.BasicProperties(
        delivery_mode=delivery_mode,
        message_id=msg_id,
    )


def _publish_chunked(channel, exchange, routing_key, body, msg_id, properties=None):
    properties = properties or _build_properties(msg_id)
    if len(body) <= CHUNK_SIZE:
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
        )
        return
    chunk_group_id = uuid.uuid4().hex
    total = (len(body) + CHUNK_SIZE - 1) // CHUNK_SIZE
    logging.info(f"Publishing chunked message: msg_id={msg_id}, group={chunk_group_id}, total_chunks={total}, size={len(body)}")
    try:
        for idx in range(total):
            start = idx * CHUNK_SIZE
            end = min(start + CHUNK_SIZE, len(body))
            chunk_data = base64.b64encode(body[start:end]).decode("ascii")
            logging.info(f"Publishing chunk {idx + 1}/{total} for message id={msg_id}, chunk_size={end - start}")
            chunk_msg = json.dumps({
                _CHUNK_MARKER: True,
                "chunk_group_id": chunk_group_id,
                _MSGID_KEY: msg_id,
                "idx": idx,
                "total": total,
                "data": chunk_data,
            }).encode("utf-8")
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=chunk_msg,
                properties=properties,
            )
    except Exception as e:
        logging.exception("Error publishing chunked message")
        raise e


class _ChunkReassembler:
    def __init__(self):
        self._buffers = {}
        self._msg_ids = {}

    def process(self, body, ack, nack, deliver):
        try:
            parsed = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            deliver(body, ack, nack, None)
            return

        if not isinstance(parsed, dict) or not parsed.get(_CHUNK_MARKER):
            deliver(body, ack, nack, None)
            return

        logging.info(f"Received chunked message: id={parsed.get('msg_id')}, idx={parsed.get('idx')}, total={parsed.get('total')}")
        
        chunk_group_id = parsed["chunk_group_id"]
        original_msg_id = parsed.get(_MSGID_KEY)
        idx = parsed["idx"]
        total = parsed["total"]
        chunk_data = base64.b64decode(parsed["data"])

        if chunk_group_id not in self._buffers:
            self._buffers[chunk_group_id] = [None] * total
            self._msg_ids[chunk_group_id] = original_msg_id

        self._buffers[chunk_group_id][idx] = chunk_data

        if all(c is not None for c in self._buffers[chunk_group_id]):
            full_body = b"".join(self._buffers.pop(chunk_group_id))
            msg_id = self._msg_ids.pop(chunk_group_id)
            deliver(full_body, lambda: None, nack, msg_id)
        ack()


def _create_connection(host):
    retries = 50
    for i in range(retries):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=host, heartbeat=0)
            )
        except pika.exceptions.AMQPConnectionError:
            if i == retries - 1:
                raise MessageMiddlewareDisconnectedError()
            time.sleep(1)
        except pika.exceptions.AMQPError:
            if i == retries - 1:
                raise MessageMiddlewareDisconnectedError()
            time.sleep(1)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name, source_id=None):
        self._host = host
        self._queue_name = queue_name
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._consumer_tag = None
        self._consuming = False
        self._lock = threading.Lock()
        self._reassembler = _ChunkReassembler()
        self._source_id = source_id if source_id else queue_name

    def start_consuming(self, on_message_callback):
        self._consuming = True

        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            msg_id = None
            if properties and properties.message_id:
                msg_id = properties.message_id

            def deliver(body, ack_fn, nack_fn, reassembled_msg_id):
                resolved_msg_id = reassembled_msg_id if reassembled_msg_id else msg_id
                ctx = {"msg_id": resolved_msg_id}
                on_message_callback(body, ack_fn, nack_fn, ctx)

            self._reassembler.process(body, ack, nack, deliver)

        self._channel.basic_qos(prefetch_count=100)
        self._consumer_tag = self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=_internal_callback,
        )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            pass

    def stop_consuming(self):
        if self._consuming:
            self._consuming = False
            try:
                self._connection.add_callback_threadsafe(
                    lambda: self._channel.stop_consuming()
                )
            except Exception:
                try:
                    self._channel.stop_consuming()
                except Exception:
                    pass

    def send(self, message):
        with self._lock:
            try:
                msg_id = _generate_msg_id(self._source_id)
                _publish_chunked(
                    self._channel,
                    exchange="",
                    routing_key=self._queue_name,
                    body=message,
                    msg_id=msg_id,
                    properties=pika.BasicProperties(delivery_mode=2, message_id=msg_id),
                )
            except pika.exceptions.AMQPConnectionError:
                raise MessageMiddlewareDisconnectedError()
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            if self._channel.is_open:
                self.stop_consuming()
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys, consumer_id, exchange_type="topic", publish_only=False, source_id=None):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, heartbeat=0))
        self._channel = self._conn.channel()
        self._exchange_name = exchange_name
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type=exchange_type, durable=True)
        self._publish_only = publish_only
        self._routing_keys = routing_keys
        self._delivery_tag = None
        self._consumer_tag = None
        self._channel.confirm_delivery()
        self._source_id = source_id if source_id else f"{exchange_name}_{consumer_id}"
        if publish_only:
            self._queue_name = None
            self._reassembler = None
            self._consumer_id = consumer_id
        else:
            self._queue_name = f"{exchange_name}_consumer_{consumer_id}"
            self._channel.queue_declare(queue=self._queue_name, durable=True)
            for key in routing_keys:
                self._channel.queue_bind(exchange=self._exchange_name, queue=self._queue_name, routing_key=key)
            self._reassembler = _ChunkReassembler()
            self._consumer_id = consumer_id
        logging.basicConfig(level=logging.WARNING)
        logging.info(f"Instanciando nodo con la cola: {self._queue_name}")

    def call_later(self,time, function):
        return self._conn.call_later(time, function)

    def remove_timeout(self, timer):
        self._conn.remove_timeout(timer)

    def send(self, message):
        try:
            keys = ".".join(self._routing_keys)
            msg_id = _generate_msg_id(self._source_id)
            _publish_chunked(
                self._channel,
                exchange=self._exchange_name,
                routing_key=keys,
                body=message,
                msg_id=msg_id,
            )
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)

    def send_by_key(self, message, key):
        #if key not in self._routing_keys:
        #    raise KeyError(f"{key} not in routing keys")
        try:
            msg_id = _generate_msg_id(self._source_id)
            _publish_chunked(
                self._channel,
                exchange=self._exchange_name,
                routing_key=key,
                body=message,
                msg_id=msg_id,
            )
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)

    def close(self):
        try:
            _close(self)
        except Exception as e:
            raise MessageMiddlewareCloseError(e)

    def start_consuming(self, on_message_callback):
        if self._publish_only:
            raise RuntimeError("Cannot consume from publish_only exchange")
        try:
            _start_consuming(self, on_message_callback=on_message_callback)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self):
        try:
            self._channel.stop_consuming(self._consumer_tag)
            self._consumer_tag = None
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)

    def ack(self):
        if self._publish_only:
            return
        self._channel.basic_ack(delivery_tag=self._delivery_tag)

    def set_delivery_tag(self, delivery_tag):
        self._delivery_tag = delivery_tag

    def set_consumer_tag(self, consumer_tag):
        self._consumer_tag = consumer_tag

    def bind(self, routing_keys=[]):
        if self._publish_only:
            raise RuntimeError("Cannot bind on publish_only exchange")
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name, queue=self._queue_name, routing_key=key)
            self._routing_keys.append(key)


def _start_consuming(message_middleware, on_message_callback):
    reassembler = message_middleware._reassembler

    def callback(ch, method, properties, body):
        message_middleware.set_delivery_tag(method.delivery_tag)
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)

        msg_id = None
        if properties and properties.message_id:
            msg_id = properties.message_id

        def deliver(body, ack_fn, nack_fn, reassembled_msg_id):
            resolved_msg_id = reassembled_msg_id if reassembled_msg_id else msg_id
            ctx = {"msg_id": resolved_msg_id}
            on_message_callback(body, ack_fn, nack_fn, ctx)

        reassembler.process(body, ack, ch.basic_nack, deliver)
        message_middleware.set_delivery_tag(method.delivery_tag)

    message_middleware._channel.basic_qos(prefetch_count=100)
    consumer_tag = message_middleware._channel.basic_consume(
        queue=message_middleware._queue_name,
        on_message_callback=callback,
    )
    message_middleware.set_consumer_tag(consumer_tag)
    message_middleware._channel.start_consuming()


def _close(message_middleware):
    if message_middleware._channel.is_open:
        if message_middleware._consumer_tag is not None:
            message_middleware.stop_consuming()
        message_middleware._channel.close()
    if message_middleware._conn.is_open:
        message_middleware._conn.close()


class MultiQueueConsumer:
    """Consumer that listens on multiple queues over a single RabbitMQ connection.

    Used by services that need to consume from several queues concurrently
    (e.g., the Join service consumes from 5 result queues, one per query).
    Each queue is registered with its own callback via add_queue(), then
    all are consumed in a single blocking loop with fair dispatch.
    """
    def __init__(self, host):
        self._host = host
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._queues = {}
        self._consuming = False
        self._reassemblers = {}

    def add_queue(self, queue_name, callback):
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._queues[queue_name] = callback
        self._reassemblers[queue_name] = _ChunkReassembler()

    def start_consuming(self):
        self._consuming = True
        self._channel.basic_qos(prefetch_count=1)
        for queue_name, callback in self._queues.items():
            reassembler = self._reassemblers[queue_name]

            def _internal_callback(ch, method, properties, body, cb=callback, reasm=reassembler):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)

                msg_id = None
                if properties and properties.message_id:
                    msg_id = properties.message_id

                def deliver(body, ack_fn, nack_fn, reassembled_msg_id):
                    resolved_msg_id = reassembled_msg_id if reassembled_msg_id else msg_id
                    ctx = {"msg_id": resolved_msg_id}
                    cb(body, ack_fn, nack_fn, ctx)

                reasm.process(body, ack, nack, deliver)

            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=_internal_callback,
            )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            pass

    def stop_consuming(self):
        if self._consuming:
            self._consuming = False
            try:
                self._connection.add_callback_threadsafe(
                    lambda: self._channel.stop_consuming()
                )
            except Exception:
                try:
                    self._channel.stop_consuming()
                except Exception:
                    pass

    def close(self):
        try:
            self.stop_consuming()
            self._connection.close()
        except Exception:
            pass
