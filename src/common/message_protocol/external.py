from asyncio import IncompleteReadError

from . import external_serializer
from .types import AccountRecord, MsgType, TransactionRecord 

def _recv_sized(socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def _recv_transaction_record(socket):
    timestamp_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    timestamp = external_serializer.deserialize_string(_recv_sized(socket, timestamp_size))
    from_bank_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    from_bank = external_serializer.deserialize_string(_recv_sized(socket, from_bank_size))
    account_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account = external_serializer.deserialize_string(_recv_sized(socket, account_size))
    to_bank_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    to_bank = external_serializer.deserialize_string(_recv_sized(socket, to_bank_size))
    to_account_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    to_account = external_serializer.deserialize_string(_recv_sized(socket, to_account_size))
    amount_received = external_serializer.deserialize_float(
        _recv_sized(socket, external_serializer.FLOAT_SIZE)
    )
    receiving_currency_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    receiving_currency = external_serializer.deserialize_string(_recv_sized(socket, receiving_currency_size))
    amount_paid = external_serializer.deserialize_float(
        _recv_sized(socket, external_serializer.FLOAT_SIZE)
    )
    payment_currency_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    payment_currency = external_serializer.deserialize_string(_recv_sized(socket, payment_currency_size))
    payment_format_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    payment_format = external_serializer.deserialize_string(_recv_sized(socket, payment_format_size))
    is_laundering = external_serializer.deserialize_bool(
        _recv_sized(socket, external_serializer.BOOL_SIZE)
    )
    return TransactionRecord(
        timestamp=timestamp,
        from_bank=from_bank,
        account=account,
        to_bank=to_bank,
        to_account=to_account,
        amount_received=amount_received,
        receiving_currency=receiving_currency,
        amount_paid=amount_paid,
        payment_currency=payment_currency,
        payment_format=payment_format,
        is_laundering=is_laundering,
    )

def _recv_account_record(socket):
    record_count = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    records = []
    for _ in range(record_count):
        bank_name_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        bank_name = external_serializer.deserialize_string(_recv_sized(socket, bank_name_size))
        bank_id_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        bank_id = external_serializer.deserialize_string(_recv_sized(socket, bank_id_size))
        account_number_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        account_number = external_serializer.deserialize_string(_recv_sized(socket, account_number_size))
        entity_id_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        entity_id = external_serializer.deserialize_string(_recv_sized(socket, entity_id_size))
        entity_name_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        entity_name = external_serializer.deserialize_string(_recv_sized(socket, entity_name_size))
        records.append(AccountRecord(
            bank_name=bank_name,
            bank_id=bank_id,
            account_number=account_number,
            entity_id=entity_id,
            entity_name=entity_name,
        ))
    return records


def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.TRANSACTION_RECORD: _recv_transaction_record,
    MsgType.ACCOUNT_RECORD: _recv_account_record,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECODS: _recv_empty,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


def _serialize_transaction_record(record: TransactionRecord):
    return b"".join(
        [
            external_serializer.serialize_uint32(len(record.timestamp)),
            external_serializer.serialize_string(record.timestamp),
            external_serializer.serialize_uint32(len(record.from_bank)),
            external_serializer.serialize_string(record.from_bank),
            external_serializer.serialize_uint32(len(record.account)),
            external_serializer.serialize_string(record.account),
            external_serializer.serialize_uint32(len(record.to_bank)),
            external_serializer.serialize_string(record.to_bank),
            external_serializer.serialize_uint32(len(record.to_account)),
            external_serializer.serialize_string(record.to_account),
            external_serializer.serialize_float(record.amount_received),
            external_serializer.serialize_uint32(len(record.receiving_currency)),
            external_serializer.serialize_string(record.receiving_currency),
            external_serializer.serialize_float(record.amount_paid),
            external_serializer.serialize_uint32(len(record.payment_currency)),
            external_serializer.serialize_string(record.payment_currency),
            external_serializer.serialize_uint32(len(record.payment_format)),
            external_serializer.serialize_string(record.payment_format),
            external_serializer.serialize_bool(record.is_laundering),
        ]
    )


def _send_transaction_record(socket, record):
    msg = external_serializer.serialize_uint32(MsgType.TRANSACTION_RECORD)
    msg += _serialize_transaction_record(record)
    socket.sendall(msg)


def _serialize_account_record(record: AccountRecord):
    return b"".join(
        [
            external_serializer.serialize_uint32(len(record.bank_name)),
            external_serializer.serialize_string(record.bank_name),
            external_serializer.serialize_uint32(len(record.bank_id)),
            external_serializer.serialize_string(record.bank_id),
            external_serializer.serialize_uint32(len(record.account_number)),
            external_serializer.serialize_string(record.account_number),
            external_serializer.serialize_uint32(len(record.entity_id)),
            external_serializer.serialize_string(record.entity_id),
            external_serializer.serialize_uint32(len(record.entity_name)),
            external_serializer.serialize_string(record.entity_name),
        ]
    )


def _send_account_record(socket, records):
    msg = external_serializer.serialize_uint32(MsgType.ACCOUNT_RECORD)
    msg += external_serializer.serialize_uint32(len(records))
    for record in records:
        msg += _serialize_account_record(record)
    socket.sendall(msg)


def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))


def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECODS))


SEND_MSG_HANDLERS = {
    MsgType.TRANSACTION_RECORD: _send_transaction_record,
    MsgType.ACCOUNT_RECORD: _send_account_record,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECODS: _send_end_of_records,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)
