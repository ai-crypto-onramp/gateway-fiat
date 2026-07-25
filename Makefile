.PHONY: build test run lint fmt-check cover docker-build docker-run clean

build:
	go build -o bin/rail-connectors ./cmd/rail-connectors

test:
	go test ./internal/... -race -coverprofile=coverage.out -coverpkg=./internal/...

run:
	go run ./cmd/rail-connectors

lint:
	golangci-lint run

fmt-check:
	@out=$$(gofmt -l . 2>/dev/null); if [ -n "$$out" ]; then echo "gofmt would reformat:"; echo "$$out"; exit 1; fi

cover: test
	go tool cover -func=coverage.out | tail -1

docker-build:
	docker build -t ai-crypto-onramp/rail-connector .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/rail-connector

clean:
	rm -rf bin/ coverage.out
