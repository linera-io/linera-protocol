FROM ubuntu:22.04 as base

RUN apt-get update && apt-get install -y curl

RUN mkdir -p /opt/linera

COPY fetch-config-file.sh /opt/linera/

WORKDIR /opt/linera

FROM ubuntu:22.04 as setup

RUN apt-get update && apt-get install -y mini-httpd

RUN mkdir -p /opt/linera

COPY linera-server /opt/linera/
COPY linera /opt/linera/
COPY setup.sh /opt/linera/

WORKDIR /opt/linera

FROM base as client

COPY linera /opt/linera/
COPY run-client.sh /opt/linera/

FROM base as server

COPY linera-server /opt/linera/
COPY run-server.sh /opt/linera/

FROM base as proxy

COPY linera-proxy /opt/linera/
COPY run-proxy.sh /opt/linera/
