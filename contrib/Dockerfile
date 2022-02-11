# This file is used to add the nightly Dgraph binaries and assets to Dgraph base
# image.

# This gets built as part of release.sh. Must be run from /tmp/build, with the linux binaries
# already built and placed there.

# docker build --target builder -t outcaste/outserv:latest .
FROM golang:1.17.7-bullseye AS builder
WORKDIR /go/outserv
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -v -o /outserv ./outserv

FROM ubuntu:20.04 as final
LABEL maintainer="Dgraph Labs <contact@dgraph.io>"

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

COPY --from=builder /outserv /usr/local/bin/outserv

EXPOSE 8080
EXPOSE 9080

RUN mkdir /outserv
WORKDIR /outserv

CMD ["outserv"] # Shows the dgraph version and commands available.
