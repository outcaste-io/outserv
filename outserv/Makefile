#
# Copyright 2018 Dgraph Labs, Inc. and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

USER_ID         = $(shell id -u)
BIN             = outserv
BUILD          ?= $(shell git rev-parse --short HEAD)
BUILD_CODENAME  = zion
BUILD_DATE     ?= $(shell git log -1 --format=%ci)
BUILD_BRANCH   ?= $(shell git rev-parse --abbrev-ref HEAD)
BUILD_VERSION ?= $(shell git describe --always --tags)

GOOS          ?= $(shell go env GOOS)
# Only build with jemalloc on Linux, mac
ifeq ($(GOOS),$(filter $(GOOS),linux darwin))
	BUILD_TAGS ?= jemalloc
endif
GOPATH        ?= $(shell go env GOPATH)
MODIFIED = $(shell git diff-index --quiet HEAD || echo "-mod")

export GO111MODULE := on

# Build-time Go variables
outservVersion   = github.com/outcaste-io/outserv/x.outservVersion
outservCodename  = github.com/outcaste-io/outserv/x.outservCodename
gitBranch       = github.com/outcaste-io/outserv/x.gitBranch
lastCommitSHA   = github.com/outcaste-io/outserv/x.lastCommitSHA
lastCommitTime  = github.com/outcaste-io/outserv/x.lastCommitTime

BUILD_FLAGS   ?= -ldflags '-X ${lastCommitSHA}=${BUILD} -X "${lastCommitTime}=${BUILD_DATE}" -X "${outservVersion}=${BUILD_VERSION}" -X "${outservCodename}=${BUILD_CODENAME}${MODIFIED}" -X ${gitBranch}=${BUILD_BRANCH}'

# Insert build tags if specified
ifneq ($(strip $(BUILD_TAGS)),)
	BUILD_FLAGS += -tags '$(BUILD_TAGS)'
endif

# Build with compiler optimizations disabled, which will help debugging with dlv.
ifneq ($(strip $(BUILD_DEBUG)),)
	BUILD_FLAGS += -gcflags="all=-N -l"
endif

# Build with race detector enabled.
ifneq ($(strip $(BUILD_RACE)),)
	BUILD_FLAGS += -race
endif

# jemalloc stuff
HAS_JEMALLOC = $(shell test -f /usr/local/lib/libjemalloc_outcaste.a && echo "jemalloc")
JEMALLOC_URL = "https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"

# nodejs
HAS_NODEJS = $(shell command -v node > /dev/null && echo "nodejs")

.PHONY: all $(BIN) jemalloc nodejs
all: $(BIN)

$(BIN): clean jemalloc nodejs
	@go build $(BUILD_FLAGS) -o $(BIN)

clean:
	@rm -f $(BIN)

uninstall:
	@go clean -i -x

install: jemalloc nodejs
	@echo "Commit SHA256: `git rev-parse HEAD`"
	@echo "Old SHA256:" `sha256sum $(GOPATH)/bin/$(BIN) 2>/dev/null | cut -c-64`
	@go install $(BUILD_FLAGS)
	@echo "New SHA256:" `sha256sum $(GOPATH)/bin/$(BIN) 2>/dev/null | cut -c-64`

jemalloc:
	@if [ -z "$(HAS_JEMALLOC)" ] ; then \
		mkdir -p /tmp/jemalloc-temp && cd /tmp/jemalloc-temp ; \
		echo "Downloading jemalloc" ; \
		curl -s -L ${JEMALLOC_URL} -o jemalloc.tar.bz2 ; \
		tar xjf ./jemalloc.tar.bz2 ; \
		cd jemalloc-5.2.1 ; \
		./configure --with-install-suffix='_outcaste' --with-jemalloc-prefix='je_' --with-malloc-conf='background_thread:true,metadata_thp:auto'; \
		make ; \
		if [ "$(USER_ID)" = "0" ]; then \
			make install_lib install_include ; \
		else \
			echo "==== Need sudo access to install jemalloc" ; \
			sudo make install_lib install_include ; \
		fi \
	fi

nodejs:
	@if [ -z "$(HAS_NODEJS)" ]; then \
		echo "Error: NodeJS not installed"; \
	fi
