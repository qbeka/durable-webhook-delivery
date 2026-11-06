#!/usr/bin/env python3
"""Builds site/index.html from recorded runs. The page is one file and makes no network requests.

  python3 scripts/build_site.py --trace <trace.json> --naive <site-naive.json> \
      --recovery <recovery.json> --stats <stats.json> --out site/index.html

stats.json holds the results, the scope text and the method, so every sentence
with a number on the page comes from a file a run produced or a person reviewed.
"""
import argparse
import base64
import json
from datetime import datetime
from html import escape
from pathlib import Path

ROOT = Path(__file__).parent.parent


def font(name: str) -> str:
    return base64.b64encode((ROOT / "site/fonts" / name).read_bytes()).decode()


def clock(milliseconds: float) -> str:
    return f"+{milliseconds:.0f} ms" if milliseconds < 1000 else f"+{milliseconds / 1000:.2f} s"


def row(term: str, value: str, note: str = "") -> str:
    strong = f"<b>{escape(value)}</b>" if note else escape(value)
    return f"<div><dt>{escape(term)}</dt><dd>{strong}{escape(note)}</dd></div>"


def crash_steps(trace: dict) -> list:
    """The trace with each step's distance from the first one, in milliseconds."""
    start = datetime.fromisoformat(trace["steps"][0]["at"])
    return [
        {**step, "offset_ms": (datetime.fromisoformat(step["at"]) - start).total_seconds() * 1000}
        for step in trace["steps"]
    ]


def naive_rows(naive: dict) -> str:
    words = {"applied": "expense written", "dropped_response": "response dropped after the write"}
    return "".join(
        row(clock(m["offset_ms"]), f"attempt {m['attempt']}: {words[m['outcome']]}") for m in naive["moments"]
    )


def outage_rows(timeline: list) -> str:
    cells = ("<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>" for values in (
        (f"{p['offset_seconds']:.0f}", p["healthy_queue"], p["failing_queue"],
         f"{p['healthy_p50_ms']:.1f}", f"{p['healthy_p99_ms']:.1f}", p["healthy_count"]) for p in timeline))
    return "".join(cells)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", required=True)
    parser.add_argument("--naive", required=True)
    parser.add_argument("--recovery", required=True)
    parser.add_argument("--stats", required=True)
    parser.add_argument("--out", default="site/index.html")
    arguments = parser.parse_args()

    trace = json.loads(Path(arguments.trace).read_text())
    naive = json.loads(Path(arguments.naive).read_text())
    recovery = json.loads(Path(arguments.recovery).read_text())
    stats = json.loads(Path(arguments.stats).read_text())
    config = recovery["config"]
    start = config["lead_ns"] / 1e9
    timeline = [p for p in recovery["timeline"] if p["offset_seconds"] >= 0]
    steps = crash_steps(trace)
    data = {
        "trace": {
            "steps": steps,
            "event_id": trace["delivery"]["event_id"],
            "amount_cents": trace["ledger_amount_cents"],
            "ledger_rows": trace["ledger_rows"],
        },
        "naive": {**naive, "planted": stats["control_repeats"]},
        "outage": {"start_seconds": start, "end_seconds": start + config["outage_ns"] / 1e9, "timeline": timeline},
    }
    # "</" cannot appear inside a script element, so escape it in the JSON.
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    parts = {
        # The fonts are embedded so the page stays one file.
        "/*FONT_SERIF*/": font("GeistSerif-Regular.woff2"),
        "/*FONT_REGULAR*/": font("Geist-Regular.woff2"),
        "/*FONT_MEDIUM*/": font("Geist-Medium.woff2"),
        "/*DATA*/": payload,
        "<!--EVIDENCE_INTRO-->": escape(stats["evidence_intro"]),
        "<!--RESULTS-->": "".join(row(claim, result, control) for claim, result, control in stats["evidence"]["rows"]),
        "<!--OUTAGE_INTRO-->": escape(stats["outage_intro"]),
        "<!--DESIGN-->": "".join(row(item["key"], item["title"], f" {item['text']}") for item in stats["design"]),
        "<!--LIMITS-->": "".join(f"<li>{escape(text)}</li>" for text in [*stats["scope"], stats["provenance"]]),
        "<!--CRASH_STEPS-->": "".join(row(f"{clock(s['offset_ms'])}, {s['actor']}", s["detail"]) for s in steps),
        "<!--NAIVE_STEPS-->": naive_rows(naive),
        "<!--OUTAGE_ROWS-->": outage_rows(timeline),
    }
    page = (ROOT / "site/template.html").read_text()
    for marker, text in parts.items():
        page = page.replace(marker, text)
    Path(arguments.out).write_text(page)
    print(f"wrote {arguments.out}")


if __name__ == "__main__":
    main()
