FROM ubuntu:22.04 as base

RUN apt-get update && apt-get install -y curl

RUN mkdir -p /opt/zefchain

COPY fetch-config-file.sh /opt/zefchain/

WORKDIR /opt/zefchain

FROM ubuntu:22.04 as setup

RUN apt-get update && apt-get install -y mini-httpd

RUN mkdir -p /opt/zefchain

COPY linera-server /opt/zefchain/
COPY linera /opt/zefchain/
COPY setup.sh /opt/zefchain/

WORKDIR /opt/zefchain

FROM base as client

COPY linera /opt/zefchain/
COPY run-client.sh /opt/zefchain/

FROM base as server

COPY linera-server /opt/zefchain/
COPY run-server.sh /opt/zefchain/

FROM base as proxy

COPY linera-proxy /opt/zefchain/
COPY run-proxy.sh /opt/zefchain/
