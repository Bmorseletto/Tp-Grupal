FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY agg_q3 .

ENTRYPOINT ["python3", "main.py"]
