FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY filter_q4_scatter_gather .

ENTRYPOINT ["python3", "main.py"]