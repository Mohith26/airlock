GO = go
BIN = bin/airlock

.PHONY: build test race e2e results wasm clean

build:
	$(GO) build -o $(BIN) ./cmd/airlock

test:
	$(GO) vet ./...
	$(GO) test ./...
	$(GO) test -race ./...

race:
	$(GO) test -race ./...

e2e: build
	./scripts/e2e.sh

results: build
	$(BIN) demo -out results/demo.json
	$(BIN) bench -out results/bench.json
	$(BIN) failover -trials 30 -out results/failover.json

clean:
	rm -rf bin

wasm:
	GOOS=js GOARCH=wasm $(GO) build -trimpath -ldflags="-s -w" -o bin/airlock.wasm ./cmd/airlock-wasm
	gzip -9 -k -f bin/airlock.wasm
