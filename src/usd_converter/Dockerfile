FROM python:3-alpine

RUN pip3 install pika requests

COPY common ./common
COPY usd_converter .

ENTRYPOINT ["python3", "main.py"]
