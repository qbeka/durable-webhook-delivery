.PHONY: build test mutants up down clean run stop send smoke faults spec report site

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

# The TLA+ model. Needs the TLA+ Toolbox: brew install --cask tla+-toolbox
spec:
	python3 scripts/check_spec.py "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"

# Rebuild the report and the website from the recorded runs.
report:
	python3 scripts/report.py results/final > /dev/null

site: report
	python3 scripts/build_site.py \
		--trace results/final/faults/kill_after_commit_before_ack/trace.json \
		--recovery results/final/recovery/slow-limits-on/rep-3/recovery.json \
		--stats results/site-stats.json --out site/index.html

clean:
	scripts/pg.sh destroy
	rm -rf bin .run
