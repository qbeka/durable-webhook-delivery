# Results

Each number in the README, the report and the resume bullets comes from a file here.

| Folder | What it holds | Commit |
|---|---|---|
| `reference/` | Three local runs of the supplied benchmark | Not applicable |
| `final/` | **The run to quote.** Every experiment from one commit, as `scripts/run-final.sh` ran it. Start at `summary.json`. | `7ba5980` |
| `published/` | The first preregistered run. Superseded after review, kept for comparison. | `3bbf3f6` |
| `failover/` | The first failover run. Superseded by `final/failover`. | `d8ba1ed` |
| `spec/` | TLC model checking results, copied into `final/` | See `git log` |
| `exploratory/` | Comparisons and diagnosis. Not results. | See each folder |
| `site-stats.json` | Text for the website, generated from the runs | Generated |

All runs happened on 2026-11-04. Each run folder has a `manifest.json` with the commit,
seeds, configuration, hardware, database settings, input hashes, background load and
hashes of its raw files.

## What is in Git

Each repetition keeps its `result.json`, its queue samples and `latency-*.csv.gz`, with
one row per request: arrival offset, latency, attempt and outcome.

Process logs and the generator's send records stay local, because they would add about
100 MB. Their hashes stay in the manifests. To regenerate them, run
`scripts/run-final.sh`.

## Regenerate the report

```bash
make site
```

This rewrites `docs/report.md`, `results/site-stats.json` and `site/index.html` from the
files here. It does not rerun anything.
