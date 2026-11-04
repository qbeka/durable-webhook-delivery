#!/usr/bin/env python3
"""Model checks spec/DeliveryLease.tla with TLC and records what it found.

The design must pass. Each control removes one rule, and TLC must then find a
counterexample against the named property, or the model is too weak to mean anything.

  python3 scripts/check_spec.py [path to tla2tools.jar]
"""
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CHECKS = [
    ("DeliveryLease.cfg", "design, all safety properties", None),
    ("liveness.cfg", "design, every delivery finishes once faults stop", None),
    ("control-no-dedupe.cfg", "receiver does not check the key", "NoRepeatedEffect"),
    ("control-not-atomic.cfg", "key and effect commit separately", "SucceededHasEffect"),
    ("control-answer-first.cfg", "receiver answers before it commits", "SucceededHasEffect"),
    ("control-no-token.cfg", "finish ignores the lease token", "SuccessIsStable"),
]


def java_for(jar):
    """Uses the runtime bundled with the TLA+ Toolbox when there is one."""
    bundled = sorted(Path(jar).parent.glob("plugins/*openjdk*/Contents/Home/bin/java"))
    return str(bundled[0]) if bundled else "java"


def run(jar, config):
    with tempfile.TemporaryDirectory() as metadata:
        finished = subprocess.run(
            [java_for(jar), "-XX:+UseParallelGC", "-cp", jar, "tlc2.TLC", "-deadlock", "-workers", "auto",
             "-metadir", metadata, "-config", config, "DeliveryLease.tla"],
            cwd="spec", capture_output=True, text=True)
    return finished.stdout + finished.stderr


def main():
    jar = str(Path(sys.argv[1] if len(sys.argv) > 1 else ".run/tla2tools.jar").resolve())
    results, failed = [], 0
    for config, meaning, expected in CHECKS:
        started = time.time()
        output = run(jar, config)
        states = re.search(r"([\d,]+) states generated, ([\d,]+) distinct states found", output)
        depth = re.search(r"The depth of the complete state graph search is (\d+)", output)
        violated = re.search(r"(?:Invariant|Temporal property|Action property) (\w+) is violated|"
                             r"Temporal properties were violated", output)
        name = violated.group(1) or "temporal property" if violated else None
        clean = "Model checking completed. No error has been found." in output
        passed = clean if expected is None else name == expected or (violated and expected in output[violated.start():violated.end() + 200])
        results.append({
            "config": config, "meaning": meaning, "expected_violation": expected, "violated": name,
            "no_error": clean, "passed": bool(passed),
            "states_generated": int(states.group(1).replace(",", "")) if states else None,
            "distinct_states": int(states.group(2).replace(",", "")) if states else None,
            "depth": int(depth.group(1)) if depth else None,
            "seconds": round(time.time() - started, 1),
        })
        print(f"{'ok' if passed else 'FAILED':7} {config:28} {meaning}: "
              f"{'no error' if clean else name}, {results[-1]['distinct_states']} distinct states")
        if not passed:
            failed += 1
            Path(".run").mkdir(exist_ok=True)
            Path(f".run/tlc-{config}.out").write_text(output)
    Path("results/spec").mkdir(parents=True, exist_ok=True)
    Path("results/spec/tlc.json").write_text(json.dumps(results, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
