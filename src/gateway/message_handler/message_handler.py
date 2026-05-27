import logging
from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().int

    def serialize_transaction_message(self, message):
        return message_protocol.internal.serialize(
            message_protocol.types.Transaction(
                self.client_id,
                message.timestamp,
                message.from_bank,
                message.account,
                message.to_bank,
                message.to_account,
                message.amount_paid,
                message.payment_currency,
                message.payment_format,
            )
        )

    def serialize_account_message(self, message):
        return message_protocol.internal.serialize(
            message_protocol.types.AccountInfo(
                self.client_id,
                message.bank_name,
                message.bank_id,
                message.account_number,
                message.entity_id,
                message.entity_name,
            )
        )

    def serialize_eof_message(self):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        logging.basicConfig(level=logging.INFO)
       
        fields = message_protocol.internal.deserialize(message)
        logging.info(f"FIELDS: {fields}")
        if fields[0] == self.client_id:
            return self.client_id, fields[1]
        return None
