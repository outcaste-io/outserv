# Portions Copyright 2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
# Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

# Update BADGER_PB_VERSION when upgrading Badger major versions
BADGER_PB_VERSION = 3

BADGER_PATH := github.com/outcaste-io/outserv/badger/v${BADGER_PB_VERSION}
GOGO_PATH   := github.com/gogo/protobuf

TMPDIR     := $(shell mktemp -d)
PROTO_PATH := ${TMPDIR}/src:.
PROTO_PATH := ${PROTO_PATH}:${TMPDIR}/src/${BADGER_PATH}/pb

.PHONY: help
help:
	@echo Use \"make regenerate\" to proceed.

.PHONY: clean
clean:
	@mkdir -p pb && rm -f pb/pb.pb.go

.PHONY: tidy-deps
tidy-deps:
	@go mod tidy -v

.PHONY: check
check:
	@./depcheck.sh && \
		(echo "Installing proto libraries to versions in go.mod." ; \
		go install github.com/golang/protobuf/protoc-gen-go ; \
		go install github.com/gogo/protobuf/protoc-gen-gogofaster)

.PHONY: copy-protos
copy-protos:
	@mkdir -p ${TMPDIR}/src/${BADGER_PATH}/pb
	@mkdir -p ${TMPDIR}/src/${GOGO_PATH}/gogoproto
	@cp $(shell go list -m -f "{{.Dir}}" ${BADGER_PATH})/pb/badgerpb${BADGER_PB_VERSION}.proto \
	 ${TMPDIR}/src/${BADGER_PATH}/pb/pb.proto
	@cp $(shell go list -m -f "{{.Dir}}" ${GOGO_PATH})/gogoproto/gogo.proto \
	 ${TMPDIR}/src/${GOGO_PATH}/gogoproto/gogo.proto

.PHONY: regenerate
regenerate: tidy-deps copy-protos check clean
	@protoc \
		--proto_path=/usr/local/include \
		--proto_path=/usr/include \
		--proto_path=${PROTO_PATH} \
		--gogofaster_out=plugins=grpc:pb \
		pb.proto
	@rm -rf ${TMPDIR}
	@echo Done.
