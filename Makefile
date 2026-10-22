.PHONY: audit build build-all clean lint test

BINARY=plugin
PLATFORMS=linux/amd64 linux/arm64 darwin/arm64
VERSION ?= $(shell git describe --tags --always 2>/dev/null | sed 's/^v//')
MODULE=$(shell go list -m)
LDFLAGS=-s -w -X main.version=$(VERSION)

# A development build can sign in to a local PunchPlay instead of punchplay.tv:
#   make build PUNCHPLAY_BASE_URL=http://localhost:3000 PUNCHPLAY_CLIENT_ID=ppc_...
ifdef PUNCHPLAY_BASE_URL
LDFLAGS+= -X $(MODULE)/provider.defaultBaseURL=$(PUNCHPLAY_BASE_URL)
endif
ifdef PUNCHPLAY_CLIENT_ID
LDFLAGS+= -X $(MODULE)/provider.defaultClientID=$(PUNCHPLAY_CLIENT_ID)
endif

build:
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY) .

build-all:
	@mkdir -p dist
	@set -e; for platform in $(PLATFORMS); do \
		GOOS=$${platform%%/*} GOARCH=$${platform##*/} CGO_ENABLED=0 \
		go build -trimpath -ldflags="$(LDFLAGS)" -o dist/$(BINARY)-$${platform%%/*}-$${platform##*/} .; \
	done

test:
	go test ./...

lint:
	golangci-lint run ./...

audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

clean:
	rm -f $(BINARY)
	rm -rf dist
