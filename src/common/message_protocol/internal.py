import json
from dataclasses import is_dataclass
from .types import AccountRecord, TransactionRecord


def serialize(message):
    if is_dataclass(message):
        message = message.__dict__
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

def deserialize_transaction_record(message):
    deserialized_message = deserialize(message)
    return TransactionRecord(**deserialized_message)

def deserialize_account_record(message):
    deserialized_message = deserialize(message)
    return AccountRecord(**deserialized_message)