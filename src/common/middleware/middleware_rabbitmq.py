import pika
import logging
import threading
import time

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


def _create_connection(host):
    """Creates a blocking connection to RabbitMQ with retry logic.

    Retries up to 50 times with 1-second intervals on AMQP connection
    or protocol errors, allowing the service to wait for the broker
    to become available during startup.
    """
    retries = 50
    for i in range(retries):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
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
    """RabbitMQ implementation of point-to-point queue communication.

    Declares a durable queue and publishes messages to it via the default
    exchange (exchange=""). Messages are persisted (delivery_mode=2) and
    consumed with manual ack/nack. Thread-safe sending via a lock.
    Supports graceful shutdown through stop_consuming() callable from
    SIGTERM handlers via add_callback_threadsafe.
    """

    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._consumer_tag = None
        self._consuming = False
        self._lock = threading.Lock()

    def start_consuming(self, on_message_callback):
        self._consuming = True

        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        self._channel.basic_qos(prefetch_count=1)
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
                self._channel.basic_publish(
                    exchange="",
                    routing_key=self._queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
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
    def __init__(self, host, exchange_name, routing_keys, exchange_type = "topic"):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._exchange_name = exchange_name
        self._channel.exchange_declare(exchange= self._exchange_name,exchange_type=exchange_type)
        result = self._channel.queue_declare(queue="")
        self._queue_name = result.method.queue
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name,queue=self._queue_name,  routing_key=key)  
        self._routing_keys = routing_keys
        self._delivery_tag = None
        self._consumer_tag = None
        self._channel.confirm_delivery()

    def send(self,message):
        try:
            keys = ".".join(self._routing_keys)
            self._channel.basic_publish(exchange=self._exchange_name,
                        routing_key=keys,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            self.close()
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(e)
    def send_by_key(self,message, key):
        if key not in self._routing_keys:
            raise KeyError(f"{key} not in routing keys")
        try:
            self._channel.basic_publish(exchange=self._exchange_name,
                        routing_key=key,
                        body=message)
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
        self._channel.basic_ack(delivery_tag=self._delivery_tag)
    def set_delivery_tag(self, delivery_tag):
        self._delivery_tag = delivery_tag
    def set_consumer_tag(self, consumer_tag):
        self._consumer_tag = consumer_tag
    def bind(self, routing_keys=[]):
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name,queue=self._queue_name,  routing_key=key) 
            self._routing_keys.append(key)

### Auxiliary function ###
def _start_consuming(message_middleware, on_message_callback):
    def callback(ch, method, properties, body):
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)
        message_middleware.set_delivery_tag(method.delivery_tag)
        on_message_callback(body,ack, ch.basic_nack)
    message_middleware._channel.basic_qos(prefetch_count=1)
    consumer_tag = message_middleware._channel.basic_consume(queue=message_middleware._queue_name,on_message_callback= callback)
    message_middleware.set_consumer_tag(consumer_tag)
    message_middleware._channel.start_consuming()

def _close(message_middleware):
    if message_middleware._channel.is_open:
        if message_middleware._consumer_tag != None:
            message_middleware.stop_consuming()
        message_middleware._channel.close()
    if message_middleware._conn.is_open:
        message_middleware._conn.close()


class DirectExchangeBcast:
    """Broadcast communication using a direct exchange.

    Implements one-to-all messaging among a group of peer instances using
    only a direct exchange. Each instance gets its own queue bound with its
    instance_id as the routing key. broadcast() publishes the same message
    to every known peer's routing key on the direct exchange, achieving the
    same fan-out effect without requiring a fanout exchange type.

    Used for EOF synchronization among horizontally-scaled filter instances:
    when one instance receives an EOF via round-robin, it broadcasts it to
    all peers so every instance can flush its state.
    """

    def __init__(self, host, exchange_name, instance_id, peer_ids=None):
        self._host = host
        self._exchange_name = exchange_name
        self._instance_id = instance_id
        self._peer_ids = peer_ids or [instance_id]
        self._connection = _create_connection(host)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=exchange_name, exchange_type="direct", durable=True
        )
        self._queue_name = f"{exchange_name}_{instance_id}"
        self._channel.queue_declare(queue=self._queue_name, durable=True)
        self._channel.queue_bind(
            exchange=exchange_name,
            queue=self._queue_name,
            routing_key=instance_id,
        )

    def add_peer(self, peer_id):
        if peer_id not in self._peer_ids:
            self._peer_ids.append(peer_id)
        peer_queue = f"{self._exchange_name}_{peer_id}"
        self._channel.queue_declare(queue=peer_queue, durable=True)
        self._channel.queue_bind(
            exchange=self._exchange_name,
            queue=peer_queue,
            routing_key=peer_id,
        )

    def broadcast(self, message):
        try:
            for peer_id in self._peer_ids:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=peer_id,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def start_consuming(self, on_message_callback):
        def _internal_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
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
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


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

    def add_queue(self, queue_name, callback):
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._queues[queue_name] = callback

    def start_consuming(self):
        self._consuming = True
        self._channel.basic_qos(prefetch_count=1)
        for queue_name, callback in self._queues.items():
            def _internal_callback(ch, method, properties, body, cb=callback):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)

                cb(body, ack, nack)

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
