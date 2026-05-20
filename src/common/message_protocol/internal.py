import json
from dataclasses import is_dataclass


def serialize(message):
    if is_dataclass(message):
        message = message.__dict__
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
