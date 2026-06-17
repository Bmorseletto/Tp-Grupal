FROM python:3-alpine

RUN apk add docker
RUN apk add --no-cache docker-cli

RUN pip3 install pika
RUN pip3 install docker

COPY common ./common
COPY node_manager .

ENTRYPOINT ["python3", "main.py"]