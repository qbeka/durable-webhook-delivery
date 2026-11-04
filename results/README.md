# Results

Each number in the README, the report and the resume bullets comes from a file here.

| Folder | What it holds | Commit |
|---|---|---|
| `reference/` | Three local runs of the supplied benchmark | Not applicable |
| `published/` | The preregistered runs: faults, ladder, target, isolation, flush cost, generator limit | `3bbf3f6` |
| `failover/` | Primary failover, its control and the replication cost | `d8ba1ed` |
| `spec/` | TLC model checking results | See `git log` |
| `site-stats.json` | Text for the website, generated from the runs | Generated |

All runs happened on 2026-11-04. Each run folder has a `manifest.json` with the commit,
seeds, configuration, hardware, database settings, input hashes, background load and
hashes of its raw files.

## What is in Git

Each repetition keeps its `result.json`, its queue samples and `latency-*.csv.gz`, with
one row per request: arrival offset, latency, attempt and outcome.

Process logs and the generator's send records stay local, because they would add about
100 MB. Their hashes stay in the manifests. To regenerate them, run the commands in
`scripts/run-benchmarks.sh` and `scripts/run-failover.sh`.

## Regenerate the report

```bash
make site
```

This rewrites `docs/report.md`, `results/site-stats.json` and `site/index.html` from the
files here. It does not rerun anything.
