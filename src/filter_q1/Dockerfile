FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY filter_q1 .

ENTRYPOINT ["python3", "main.py"]
