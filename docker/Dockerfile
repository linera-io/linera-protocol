FROM debian:11 as base

RUN apt-get update && apt-get install -y curl

RUN mkdir -p /opt/zefchain

COPY fetch-config-file.sh /opt/zefchain/

WORKDIR /opt/zefchain

FROM debian:11 as setup

RUN apt-get update && apt-get install -y mini-httpd

RUN mkdir -p /opt/zefchain

COPY server /opt/zefchain/
COPY client /opt/zefchain/
COPY setup.sh /opt/zefchain/

WORKDIR /opt/zefchain

FROM base as client

COPY client /opt/zefchain/
COPY run-client.sh /opt/zefchain/

FROM base as server

COPY server /opt/zefchain/
COPY run-server.sh /opt/zefchain/

FROM base as proxy

COPY proxy /opt/zefchain/
COPY run-proxy.sh /opt/zefchain/
