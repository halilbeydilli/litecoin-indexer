.PHONY: build run dev clean rebuild-stats api-only

BINARY=chain-indexer
MAIN=./cmd/indexer

build:
	go build -o $(BINARY) $(MAIN)

run: build
	./$(BINARY)

dev:
	go run $(MAIN)

rebuild-stats:
	go run $(MAIN) --rebuild-stats

api-only:
	go run $(MAIN) --api-only

clean:
	rm -f $(BINARY)

# Cross-compile for Linux (VPS deployment)
build-linux:
	GOOS=linux GOARCH=amd64 go build -o $(BINARY)-linux-amd64 $(MAIN)
