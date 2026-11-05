# Preregistration: the retest

Committed before the retest. It covers only the two tests that failed in the final run.

## Why

H1b and H4 failed in `results/final`, with one second stalls every 30 seconds for about an hour.
Two later exploratory runs did not show them. A failure that may come from the machine should be
tested once more, with an instrument that could explain it.

## What is the same

The commit's service code is unchanged from `7ba5980`. Commands, seeds, settings, the sustained
rule and the machine conditions are the ones in `docs/preregistration-final.md`.

## What is new

`scripts/flush_probe.py` measures a small write and a full drive flush four times a second, on the
same drive, for the whole retest. It runs beside the benchmark and adds a little load.

## Decision rules

- H1b and H4 use the same rules as the final run.
- This is one attempt. If a test fails here, it is not run again to get a better number.
- `results/final` stays on the record. A failed test there remains a failed test.
- If H1b passes all three repetitions, the README may say that 2,000 per second passed for ten
  minutes in this retest, and must say beside it that the same test failed in the final run.
- If H4 passes, the same applies to the 250 ms budget.
- If either fails, its claim stays as it is now.
- If stalls appear and the probe spikes at the same moments, the report says the drive flush was
  slow. It does not say why. If the probe stays flat, the drive flush is ruled out.
