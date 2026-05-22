import logging
from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().int

    def serialize_data_message(self, message):
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

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        logging.basicConfig(level=logging.INFO)
       
        fields = message_protocol.internal.deserialize(message)
        logging.info(f"FIELDS: {fields}")
        if fields[0] == self.client_id:
            return fields[1]
        return None
