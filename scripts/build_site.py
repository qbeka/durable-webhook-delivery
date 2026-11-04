#!/usr/bin/env python3
"""Builds site/index.html from a recorded run. The page makes no network requests.

  python3 scripts/build_site.py --trace <trace.json> --recovery <recovery.json> \
      --stats <stats.json> --out site/index.html

stats.json holds the headline figures, the scope text and the footer, so every
sentence with a number on the page comes from a file a run produced or a person
reviewed.
"""
import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", required=True)
    parser.add_argument("--recovery", required=True)
    parser.add_argument("--stats", required=True)
    parser.add_argument("--out", default="site/index.html")
    arguments = parser.parse_args()

    trace = json.loads(Path(arguments.trace).read_text())
    recovery = json.loads(Path(arguments.recovery).read_text())
    stats = json.loads(Path(arguments.stats).read_text())
    config = recovery["config"]
    start = config["lead_ns"] / 1e9
    end = start + config["outage_ns"] / 1e9
    data = {
        "stats": stats["stats"],
        "trace": {"steps": trace["steps"]},
        "outage": {
            "intro": stats["outage_intro"],
            "start_seconds": start,
            "end_seconds": end,
            "timeline": [p for p in recovery["timeline"] if p["offset_seconds"] >= 0],
        },
        "scope": stats["scope"],
        "footer": stats["footer"],
    }
    template = (Path(__file__).parent.parent / "site/template.html").read_text()
    # "</" cannot appear inside a script element, so escape it in the JSON.
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    Path(arguments.out).write_text(template.replace("/*DATA*/", payload))
    print(f"wrote {arguments.out}")


if __name__ == "__main__":
    main()
