FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY agg_q1 .

ENTRYPOINT ["python3", "main.py"]
