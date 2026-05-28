import os
import logging
import signal
import requests
import json
from datetime import datetime
from common import middleware, message_protocol

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"]

CONVERSION_API_URL = (
    "https://api.frankfurter.dev/v2/rates?from=2022-09-01&to=2022-09-05&base=USD"
)
CURRENCIES_API_URL = "https://api.frankfurter.dev/v2/currencies"
BITCOIN_CONVERSION_RATES = {
    "2022-09-01": 19793.1,
    "2022-09-02": 19999.9,
    "2022-09-03": 19831.4,
    "2022-09-04": 19952.7,
    "2022-09-05": 20126.1,
}
ISO_TO_DATASET_NAME = {
    "BRL": "Brazil Real",
    "RUB": "Ruble",
    "INR": "Rupee",
    "ILS": "Shekel",
    "GBP": "UK Pound",
    "JPY": "Yen",
    "CNY": "Yuan",
}
STATE_FILE = "/output/conversion_rates.json"
US_DOLLAR = "US Dollar"


class USDConverter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, FILTER_PREFIX, [f"{FILTER_PREFIX}", FILTER_PREFIX + f"{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.conversion_rates = {}
        self._fetch_conversion_rates()

    def _save_conversion_rates(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.conversion_rates, f)
        except IOError as e:
            logging.exception(f"Error saving conversion rates: {e}")

    def _fetch_conversion_rates(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    self.conversion_rates = json.load(f)
                logging.info("Loaded conversion rates from state file.")
                return
            except IOError as e:
                logging.exception(f"Error loading conversion rates: {e}")
        try:
            response = requests.get(CURRENCIES_API_URL)
            response.raise_for_status()
            data = response.json()
            currencies = {currency["iso_code"]: currency["name"] for currency in data}
            currencies.update(ISO_TO_DATASET_NAME)

            response = requests.get(CONVERSION_API_URL)
            response.raise_for_status()
            data = response.json()
            for rate in data:
                day_rate = self.conversion_rates.setdefault(rate["date"], {})
                if rate["quote"] in currencies:
                    day_rate[currencies[rate["quote"]]] = rate["rate"]
            for date, rate in BITCOIN_CONVERSION_RATES.items():
                self.conversion_rates[date]["Bitcoin"] = rate
        except requests.RequestException as e:
            logging.exception(f"Error fetching conversion rates: {e}, retrying later.")

            self._save_conversion_rates()

    def _convert_to_usd(self, amount, currency, date):
        if not self.conversion_rates:
            self._fetch_conversion_rates()
        if currency == US_DOLLAR:
            return amount
        if date not in self.conversion_rates:
            logging.info(f"No conversion rates found for date {date}.")
            return None
        day_rates = self.conversion_rates.get(date)
        rate = day_rates.get(currency)
        if not rate:
            logging.info(
                f"No conversion rate found for currency {currency} on date {date}."
            )
            return None
        return amount / rate

    def _process_data(self, transaction):
        amount = transaction.get("amount_paid")
        currency = transaction.get("payment_currency")
        date = str(datetime.strptime(transaction["timestamp"], "%Y/%m/%d %H:%M").date())
        # logging.debug(f"Processing transaction: {transaction}")
        if amount is None or currency is None or date is None:
            logging.info(f"Message missing required fields: {transaction}")
            return

        converted_amount = self._convert_to_usd(amount, currency, date)
        logging.debug(f"Converted {amount} {currency} to {converted_amount} USD")
        if converted_amount is not None and currency != US_DOLLAR:
            transaction["amount_paid"] = converted_amount
            transaction["payment_currency"] = US_DOLLAR

        if (
            transaction["payment_format"] == "Wire"
            or transaction["payment_format"] == "ACH"
        ) and transaction["amount_paid"] < 1:
            self.output_queue.send(message_protocol.internal.serialize(transaction))

    def _process_eof(self, deserialized_message):
        # Just forward the EOF message
        self.output_queue.send(
            message_protocol.internal.serialize(deserialized_message)
        )

    def process_messsage(self, message, ack, nack):
        deserialized_message = message_protocol.internal.deserialize(message)
        logging.debug(f"MESSAGE {deserialized_message}")
        if len(deserialized_message) == 2:
            self._process_eof(deserialized_message)
        else:
            self._process_data(deserialized_message)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.input_exchange.close()
        self.output_queue.close()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    usd_converter_filter = USDConverter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: usd_converter_filter.stop(),
    )
    usd_converter_filter.start()
    usd_converter_filter.close()
    return 0


if __name__ == "__main__":
    main()
