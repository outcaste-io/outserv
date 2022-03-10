# Portions Copyright 2018 Dgraph Labs, Inc. are available under the Apache 2.0 license.
# Portions Copyright 2022 Outcaste, Inc. are available under the Smart license.

BUILD          ?= $(shell git rev-parse --short HEAD)
BUILD_CODENAME  = zion
BUILD_DATE     ?= $(shell git log -1 --format=%ci)
BUILD_BRANCH   ?= $(shell git rev-parse --abbrev-ref HEAD)
BUILD_VERSION  ?= $(shell git describe --always --tags)

MODIFIED = $(shell git diff-index --quiet HEAD || echo "-mod")

SUBDIRS = outserv

###############

.PHONY: $(SUBDIRS) all oss version install install_oss oss_install uninstall test help image
all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -w -C $@ all

version:
	@echo Dgraph ${BUILD_VERSION}
	@echo Build: ${BUILD}
	@echo Codename: ${BUILD_CODENAME}${MODIFIED}
	@echo Build date: ${BUILD_DATE}
	@echo Branch: ${BUILD_BRANCH}
	@echo Go version: $(shell go version)

install:
	@(set -e;for i in $(SUBDIRS); do \
		echo Installing $$i ...; \
		$(MAKE) -C $$i install; \
	done)

uninstall:
	@(set -e;for i in $(SUBDIRS); do \
		echo Uninstalling $$i ...; \
		$(MAKE) -C $$i uninstall; \
	done)

test:
	@echo Running ./test.sh
	./test.sh

image:
	@GOOS=linux $(MAKE) outserv
	@mkdir -p linux
	@mv ./outserv/outserv ./linux/outserv
	@docker build -f contrib/Dockerfile -t dgraph/dgraph:$(subst /,-,${BUILD_BRANCH}) .
	@rm -r linux

help:
	@echo
	@echo Build commands:
	@echo "  make [all]     - Build all targets [EE]"
	@echo "  make oss       - Build all targets [OSS]"
	@echo "  make dgraph    - Build dgraph binary"
	@echo "  make install   - Install all targets"
	@echo "  make uninstall - Uninstall known targets"
	@echo "  make version   - Show current build info"
	@echo "  make help      - This help"
	@echo
