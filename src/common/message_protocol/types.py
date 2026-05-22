from dataclasses import dataclass

@dataclass
class AccountRecord:
    bank_name: str
    bank_id: str
    account_number: str
    entity_id: str
    entity_name: str
    

@dataclass
class TransactionRecord:
    timestamp: str
    from_bank: str
    account: str
    to_bank: str
    to_account: str
    amount_paid: float
    payment_currency: str
    payment_format: str

@dataclass
class Transaction:
    client_id : str
    timestamp: str
    from_bank: str
    account: str
    to_bank: str
    to_account: str
    amount_paid: float
    payment_currency: str
    payment_format: str

class MsgType:
    TRANSACTION_RECORD = 1
    ACCOUNT_RECORD = 2
    ACK = 3
    END_OF_RECODS = 4
    RESULTS = 5