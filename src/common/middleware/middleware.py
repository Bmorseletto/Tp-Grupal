from abc import ABC, abstractmethod


class MessageMiddlewareMessageError(Exception):
    """Raised when an internal error occurs while sending or processing a message."""
    pass


class MessageMiddlewareDisconnectedError(Exception):
    """Raised when the connection to the message broker is lost."""
    pass


class MessageMiddlewareCloseError(Exception):
    """Raised when an error occurs while closing the middleware connection."""
    pass


class MessageMiddlewareDeleteError(Exception):
    """Raised when an error occurs while deleting a queue or exchange."""
    pass


class MessageMiddleware(ABC):
    """Abstract base class for message middleware.

    Defines the common interface for both queue-based and exchange-based
    communication: consuming, sending, and connection lifecycle.
    """

    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    @abstractmethod
    def stop_consuming(self):
        pass

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    """Abstract specialization for exchange-based communication.

    Routes messages to queues based on routing keys bound to a direct exchange.
    A single exchange can publish to multiple routing keys, and a consumer
    listens on exactly one routing key.
    """
    @abstractmethod
    def __init__(self, host, exchange_name, route_keys):
        pass


class MessageMiddlewareQueue(MessageMiddleware):
    """Abstract specialization for point-to-point queue communication.

    Publishes and consumes directly from a named queue without an exchange
    (uses RabbitMQ's default exchange).
    """
    @abstractmethod
    def __init__(self, host, queue_name):
        pass
