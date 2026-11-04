.PHONY: build test mutants up down clean run stop send smoke faults

build:
	go build -o bin/ ./cmd/...

# Databases only.
up:
	scripts/pg.sh up

down:
	scripts/pg.sh down

# The whole service, with one command each way.
run:
	scripts/run.sh start

stop:
	scripts/run.sh stop

send:
	scripts/run.sh send

# Correctness suite. Needs `make up`.
test:
	go vet ./...
	go test -race -count=1 ./...

mutants:
	python3 scripts/mutants.py

# About 90 seconds. Controls, the crash window and an outage, at low volume.
smoke: build up
	./bin/harness faults --scale 0.1 --out .run/smoke \
		--only control_naive,control_idempotent,kill_after_commit_before_ack,destination_offline

# The full fault suite, including the reference workload over HTTP.
faults: build up
	./bin/harness faults --out .run/faults

clean:
	scripts/pg.sh destroy
	rm -rf bin .run
