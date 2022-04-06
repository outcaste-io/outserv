# This file is used to add the nightly Dgraph binaries and assets to Dgraph base
# image.

# This gets built as part of release.sh. Must be run from /tmp/build, with the linux binaries
# already built and placed there.

# docker build --target builder -t outcaste/outserv:latest .
FROM golang:1.17.7-bullseye AS builder
WORKDIR /go/outserv
RUN apt-get update && apt-get install -y --no-install-recommends make gcc tar bzip2 && rm -rf /var/lib/apt/lists/*
COPY ./outserv/Makefile /tmp/Makefile
RUN make -C /tmp jemalloc
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
ARG BUILD_RACE
RUN make BUILD_RACE=$BUILD_RACE install

FROM ubuntu:20.04 as final
LABEL maintainer="Outcaste, Inc. <team@outcaste.io>"

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    htop \
    iputils-ping \
    jq \
    less \
    sysstat && \
  curl -fsSL https://deb.nodesource.com/setup_14.x | bash - && \
  apt-get install -y nodejs && \
  rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/bin/outserv /usr/local/bin/outserv

EXPOSE 8080

RUN mkdir /outserv
WORKDIR /outserv

CMD ["outserv"] # Shows the dgraph version and commands available.
