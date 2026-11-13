.PHONY: all fmt vet test test-race bench sim ci fmt-check

all: fmt vet test-race sim

fmt:
	@echo "==> Formatting code..."
	gofmt -w -s .

fmt-check:
	@echo "==> Checking code formatting..."
	@test -z "$$(gofmt -l .)" || (echo "Unformatted files found:" && gofmt -l . && exit 1)

vet:
	@echo "==> Vetting code..."
	go vet ./...

test:
	@echo "==> Running tests..."
	go test -v ./...

test-race:
	@echo "==> Running tests with race detector..."
	go test -count=1 -race ./...

bench:
	@echo "==> Running benchmarks..."
	go test -bench=. -benchmem ./benchmarks

sim:
	@echo "==> Running banking simulation..."
	go run ./cmd/bank-sim

ci: fmt-check vet test-race sim
