.PHONY: build test mutants up down clean

build:
	go build -o bin/ ./cmd/...

up:
	scripts/pg.sh up

down:
	scripts/pg.sh down

clean:
	scripts/pg.sh destroy
	rm -rf bin .run

test:
	go vet ./...
	go test ./...

mutants:
	python3 scripts/mutants.py
