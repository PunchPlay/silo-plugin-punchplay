.PHONY: audit build build-all clean lint test

BINARY=plugin
PLATFORMS=linux/amd64 linux/arm64 darwin/arm64
VERSION ?= $(shell git describe --tags --always 2>/dev/null | sed 's/^v//')
LDFLAGS=-s -w -X main.version=$(VERSION)

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
