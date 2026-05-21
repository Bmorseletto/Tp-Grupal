from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().int

    def serialize_data_message(self, message):
        (
            timestamp,
            og_bank,
            og_account,
            dest_bank,
            dest_account,
            amt_paid,
            payment_currency,
            payment_format,
        ) = message

        return message_protocol.internal.serialize(
            [
                self.client_id,
                timestamp,
                og_bank,
                og_account,
                dest_bank,
                dest_account,
                amt_paid,
                payment_currency,
                payment_format,
            ]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == self.client_id:
            return fields[1]
        return None
