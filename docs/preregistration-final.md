# Preregistration: the final run

Committed before the final run. It replaces the run at `3bbf3f6` as the source of every
quoted number. That first run is kept in `results/published` and `results/failover`.

## Why there is a second run

An independent review of the first run found a code defect and several claims stronger
than their evidence. No defect touched delivery safety, but the code changed, so a number
measured before the change should not be quoted after it. `docs/reviews.md` has the list.

## What changed from the earlier plans

Everything in `docs/preregistration.md` and `docs/preregistration-failover.md` still
applies, with these changes. All of them make a pass harder or a claim better supported.

| Item | Change | Why |
|---|---|---|
| H1b, new | 2,000 events per second, 4 workers, three 10 minute runs | The earlier claim rested on 60 second runs. |
| Sustained rule | Accepted rate must be within 1% of offered | The rule said "offered rate is met" but checked only the error rate. |
| H4 | The offline run must also keep the healthy p99 under 250 ms | The earlier check covered the slow run only. |
| Machine conditions | Background CPU is sampled every 5 seconds during a repetition. It is contended if the median is above 500%. | One reading before a repetition says little about the minutes after it. |
| Failover | A sync repetition is invalid unless the primary reports a synchronous standby | Earlier manifests described a different cluster. |
| Provenance | The failure run and generator have manifests. The mutation check and TLC results are recorded in the run. | They had no recorded run. |

## Decision rule for quoting

The sustained rate quoted is the highest ladder rung that passes the rule. If H1b fails, the
quoted claim says 60 second runs, or falls to a rate that passed for ten minutes. If any
repetition is contended, the report says so next to it.
