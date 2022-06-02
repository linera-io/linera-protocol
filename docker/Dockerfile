FROM debian:11 AS builder

RUN apt-get update && apt-get install -y \
    curl \
    libclang-dev \
    g++ \
    git

ENV PATH=${PATH}:/root/.cargo/bin

RUN curl -sf -L https://sh.rustup.rs > /tmp/rustup.sh && \
    [ "$(sha256sum /tmp/rustup.sh | cut -f1 -d" ")" = "a3cb081f88a6789d104518b30d4aa410009cd08c3822a1226991d6cf0442a0f8" ] && \
    chmod +x /tmp/rustup.sh && \
    /tmp/rustup.sh -y && \
    rustc --version | grep -q '^rustc 1[.]61[.]0 ' && \
    rm /tmp/rustup.sh

COPY ssh-config /root/.ssh/config
COPY github_rsa /root/.ssh/github_rsa
COPY known_hosts /root/.ssh/known_hosts

RUN mkdir /opt/zefchain && \
    cd /opt/zefchain && \
    git init && \
    git remote add origin github:zefchain/zefchain-protocol.git && \
    git fetch && \
    git checkout main && \
    cargo build --release

FROM debian:11 as base

RUN apt-get update && apt-get install -y curl

RUN mkdir -p /opt/zefchain

COPY fetch-config-file.sh /opt/zefchain/

WORKDIR /opt/zefchain

FROM debian:11 as setup

RUN apt-get update && apt-get install -y mini-httpd

RUN mkdir -p /opt/zefchain

COPY --from=builder /opt/zefchain/target/release/server /opt/zefchain/
COPY --from=builder /opt/zefchain/target/release/client /opt/zefchain/
COPY setup.sh /opt/zefchain/

WORKDIR /opt/zefchain

FROM base as client

COPY --from=builder /opt/zefchain/target/release/client /opt/zefchain/
COPY run-client.sh /opt/zefchain/

FROM base as server

COPY --from=builder /opt/zefchain/target/release/server /opt/zefchain/
COPY run-server.sh /opt/zefchain/

FROM base as proxy

COPY --from=builder /opt/zefchain/target/release/proxy /opt/zefchain/
COPY run-proxy.sh /opt/zefchain/
