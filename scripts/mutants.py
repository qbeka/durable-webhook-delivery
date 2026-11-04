#!/usr/bin/env python3
"""Breaks one invariant at a time in a temporary copy and checks a test catches it.

A test that cannot fail proves nothing. Each mutant below removes a rule the
contract depends on. The run passes only if every mutant is caught.

Run from the repository root with the databases up: python3 scripts/mutants.py
"""
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

STORE = "internal/store"
WORKER = "internal/worker"
RECEIVER = "internal/receiver"
SIGNING = "internal/signing"

# name, file, old text, new text, package, test name pattern
MUTANTS = [
    ("ingress does not lock the source",
     f"{STORE}/ingest.go",
     "if err := lockSourceShared(ctx, tx, event.TenantID, event.SourceID); err != nil {\n\t\t\treturn err\n\t\t}\n",
     "", STORE, "TestSubscriptionChangesAreOrderedAgainstIngress"),
    ("a conflicting body counts as a duplicate",
     f"{STORE}/ingest.go",
     "if storedType == event.EventType && bytes.Equal(result.StoredSHA256, event.PayloadSHA256) {",
     "if true || bytes.Equal(nil, nil) {", STORE, "TestConflictingBody"),
    ("a duplicate event is reported as accepted",
     f"{STORE}/ingest.go",
     "if eventRowID != nil {", "if eventRowID != nil || true {", STORE, "TestConcurrentIdenticalRequests"),
    ("finish ignores the lease token",
     f"{STORE}/finish.go",
     "WHERE d.id = r.id AND d.lease_token = r.token AND d.state = 'leased'",
     "WHERE d.id = r.id AND r.token IS NOT NULL AND d.state = 'leased'",
     STORE, "TestStaleFinish"),
    ("a stale finish is not marked stale",
     f"{STORE}/finish.go",
     "stale = NOT EXISTS (SELECT 1 FROM finished f WHERE f.id = r.id)", "stale = false", STORE, "TestStaleFinish"),
    ("claim ignores the in-flight limit",
     f"{STORE}/claim.go",
     "LIMIT LEAST($3::int, (SELECT free FROM room))", "LIMIT $3::int", STORE, "TestConcurrentClaims"),
    ("claim does not lock the destination",
     f"{STORE}/claim.go",
     "FOR NO KEY UPDATE OF d SKIP LOCKED", "", STORE, "TestConcurrentClaims"),
    ("an open breaker does not limit claims to one probe",
     f"{STORE}/claim.go",
     "maxInFlight, attemptCost = 1, 0", "attemptCost = 0", STORE, "TestBreaker"),
    ("a probe spends an attempt",
     f"{STORE}/claim.go",
     "maxInFlight, attemptCost = 1, 0", "maxInFlight, attemptCost = 1, 1", STORE, "TestProbesSpendNoAttempts"),
    ("the rate limit is ignored",
     f"{STORE}/claim.go",
     "budget = min(maxJobs, int(tokens))", "budget = maxJobs", STORE, "TestRateLimit"),
    ("claims always go to one destination",
     f"{STORE}/claim.go",
     "ORDER BY d.last_claimed_at", "ORDER BY d.id", STORE, "TestRoundRobin"),
    ("an expired lease never goes dead",
     f"{STORE}/finish.go",
     "state = CASE WHEN d.budget_used >= $2 OR",
     "state = CASE WHEN false OR",
     STORE, "TestExpiredLeaseBudget"),
    ("an expired lease ignores the age limit",
     f"{STORE}/finish.go",
     "state = CASE WHEN d.budget_used >= $2 OR clock_timestamp() - d.budget_started_at >= make_interval(secs => $5)",
     "state = CASE WHEN d.budget_used >= $2 OR false",
     STORE, "TestExpiredLeasePastTheAgeLimit"),
    ("a retried finish relabels an applied attempt",
     f"{STORE}/finish.go",
     "AND (a.outcome IS NULL OR a.outcome = 'lease_expired')", "", STORE, "TestRepeatedFinish"),
    ("pruned payloads use up the replay limit",
     f"{STORE}/ops.go",
     "` AND e.payload IS NOT NULL ORDER BY d.id LIMIT $4`", "` ORDER BY d.id LIMIT $4`", STORE, "TestExpiredPayloads"),
    ("replay changes the delivery key",
     f"{STORE}/ops.go",
     "state = 'pending', budget_used = 0,",
     "state = 'pending', budget_used = 0, delivery_key = d.delivery_key || 'r',",
     STORE, "TestReplayKeepsTheKey"),
    ("replay touches work in progress",
     f"{STORE}/ops.go",
     "WHERE d.state IN ('succeeded', 'dead')\n\t\t\t  AND (($1",
     "WHERE d.state IN ('succeeded', 'dead', 'leased')\n\t\t\t  AND (($1",
     STORE, "TestReplayKeepsTheKey"),
    ("prune removes unfinished work",
     f"{STORE}/ops.go",
     "WHERE d.event_row_id = e.id AND d.state IN ('pending', 'leased'))`",
     "WHERE d.event_row_id = e.id AND d.state IN ('none'))`",
     STORE, "TestPrune"),
    ("the receiver applies every request",
     f"{RECEIVER}/receiver.go",
     "if r.Mode == ModeNaive {", "if true {", RECEIVER, "TestIdempotentReceiver|TestConcurrentRequests"),
    ("the delivery key is not checked for a different body",
     f"{RECEIVER}/receiver.go",
     "if hex.EncodeToString(storedDigest) != hex.EncodeToString(incoming.digest) {",
     "if false && hex.EncodeToString(storedDigest) != hex.EncodeToString(incoming.digest) {",
     RECEIVER, "TestSameKeyWithDifferentBody"),
    ("the receiver skips signature verification",
     f"{RECEIVER}/receiver.go",
     'if key == "" || !signing.Equal(expected, header.Get("Hook-Signature")) {',
     'if key == "" || expected == "" {', RECEIVER, "TestBadSignature"),
    ("the worker follows redirects",
     f"{WORKER}/worker.go",
     "response, err := w.transport.RoundTrip(request)",
     "response, err := (&http.Client{Transport: w.transport}).Do(request)", WORKER, "TestRedirect"),
    ("the age limit is ignored",
     f"{WORKER}/policy.go",
     "if age >= policy.MaxAge {", "if false {", WORKER, "TestAgeLimit"),
    ("a stored event may have no effect",
     "internal/reconcile/reconcile.go",
     "(!event.optional || stored[key]) && seen[key] == 0", "!event.optional && seen[key] == 0",
     "internal/reconcile", "TestAStoredEvent"),
    ("a merchant mismatch goes unnoticed",
     "internal/reconcile/reconcile.go",
     "currency != event.currency || merchant != event.merchant", "currency != event.currency",
     "internal/reconcile", "TestEachKindOfDamage"),
    ("a run passes with rejected events",
     "internal/harness/scenario.go",
     "if summary.Rejected+summary.Conflicts+summary.TimedOut > 0 || len(summary.UnackedIndexes) > 0 {",
     "if false {", "internal/harness", "TestVerdict"),
    ("a clean exit counts as a crash",
     "internal/harness/process.go",
     "return ok && status.Signaled() && status.Signal() == syscall.SIGKILL",
     "return ok && (status.Exited() || status.Signaled())",
     "internal/harness", "TestACleanExit"),
    ("the delivery signature leaves out the tenant",
     f"{SIGNING}/signing.go",
     "deliveryKey, tenant, source, eventID, eventType, hex.EncodeToString(bodyDigest))",
     "deliveryKey, source, eventID, eventType, hex.EncodeToString(bodyDigest))",
     RECEIVER, "TestChangedIdentityHeader"),
    ("lease expiry overwrites the attempt outcome",
     f"{STORE}/finish.go",
     "outcome = COALESCE(a.outcome, 'lease_expired'),", "outcome = 'running',", WORKER, "TestAbandonedLease"),
    ("the signature leaves out the event ID",
     f"{SIGNING}/signing.go",
     "return sign(secret, strconv.FormatInt(unixSeconds, 10), tenant, source, eventID, eventType, hex.EncodeToString(bodyDigest))",
     "return sign(secret, strconv.FormatInt(unixSeconds, 10), tenant, source, eventType, hex.EncodeToString(bodyDigest))",
     SIGNING, "TestIngressSignatureBinds|TestDotsCannotMove"),
]


def run_mutant(root: Path, mutant) -> dict:
    name, path, old, new, package, pattern = mutant
    with tempfile.TemporaryDirectory() as folder:
        copy = Path(folder) / "repo"
        shutil.copytree(root, copy, ignore=shutil.ignore_patterns(".git", ".run", "bin", "results", "site"))
        target = copy / path
        source = target.read_text()
        if source.count(old) < 1:
            return {"mutant": name, "result": "NOT APPLIED: text not found"}
        target.write_text(source.replace(old, new))
        # `-count=3` repeats timing-dependent tests, so a mutant that only fails
        # under one interleaving still has several chances to be caught.
        finished = subprocess.run(
            ["go", "test", "-count=3", "-run", pattern, f"./{package}/"],
            cwd=copy, capture_output=True, text=True, timeout=300)
        output = finished.stdout + finished.stderr
        if "build failed" in output or "[setup failed]" in output:
            return {"mutant": name, "result": "NOT APPLIED: does not compile", "output": output[-500:]}
        return {"mutant": name, "test": pattern, "result": "caught" if finished.returncode != 0 else "SURVIVED"}


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    results = []
    for mutant in MUTANTS:
        result = run_mutant(root, mutant)
        print(f"{result['result']:<10} {result['mutant']}", flush=True)
        results.append(result)
    failed = [r for r in results if r["result"] != "caught"]
    print(f"\n{len(results) - len(failed)} of {len(results)} mutants caught")
    for result in failed:
        print(json.dumps(result))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
