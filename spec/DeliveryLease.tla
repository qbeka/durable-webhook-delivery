---------------------------- MODULE DeliveryLease ----------------------------
(***************************************************************************)
(* The delivery protocol for one event and one destination.               *)
(*                                                                         *)
(* This models the design in docs/contract.md, not the Go code. One        *)
(* delivery is enough: deliveries do not share state that matters to       *)
(* these properties, because each has its own key and lease.               *)
(*                                                                         *)
(* Time is not modelled. A lease may expire at any step, which covers      *)
(* every possible pause, slow request and clock.                           *)
(*                                                                         *)
(* Each constant named below is a rule of the design. Setting one to       *)
(* FALSE removes that rule, and TLC must then find a counterexample. That  *)
(* is the negative control: it shows the model can fail.                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Workers,            \* the set of workers
    MaxFaults,          \* crashes, losses and expiries allowed in one behaviour
    MaxReplays,         \* operator replays allowed in one behaviour
    ReceiverDedupes,    \* rule: the receiver checks the delivery key
    KeyAndEffectAtomic, \* rule: the key and the effect commit together
    CommitBeforeAnswer, \* rule: the receiver answers only after its commit
    FinishChecksToken   \* rule: a finish needs the current lease token

VARIABLES
    state,      \* the engine's view: pending, leased or succeeded
    token,      \* the current lease token, 0 when there is none
    nextToken,  \* the next unused token, so every claim gets a new one
    worker,     \* per worker: its phase and the token it holds
    requests,   \* tokens of requests in the network
    answers,    \* tokens of responses in the network
    keySeen,    \* the receiver has committed this delivery key
    ledger,     \* expense rows at the receiver
    unsaved,    \* work the receiver has started and not yet committed
    faults,     \* faults used so far
    replays     \* replays used so far

vars == <<state, token, nextToken, worker, requests, answers,
          keySeen, ledger, unsaved, faults, replays>>

Idle == [phase |-> "idle", tok |-> 0]

TypeOK ==
    /\ state \in {"pending", "leased", "succeeded"}
    /\ token \in Nat
    /\ \A w \in Workers :
         worker[w].phase \in {"idle", "claimed", "waiting", "answered", "failed"}
    /\ ledger \in Nat
    /\ unsaved \in {"none", "key", "effect"}

Init ==
    /\ state = "pending"
    /\ token = 0
    /\ nextToken = 1
    /\ worker = [w \in Workers |-> Idle]
    /\ requests = {}
    /\ answers = {}
    /\ keySeen = FALSE
    /\ ledger = 0
    /\ unsaved = "none"
    /\ faults = 0
    /\ replays = 0

(***************************************************************************)
(* Engine and worker steps.                                                *)
(***************************************************************************)

\* The claim commits before anything is sent. It always takes a new token.
Claim(w) ==
    /\ state = "pending"
    /\ worker[w].phase = "idle"
    /\ state' = "leased"
    /\ token' = nextToken
    /\ nextToken' = nextToken + 1
    /\ worker' = [worker EXCEPT ![w] = [phase |-> "claimed", tok |-> nextToken]]
    /\ UNCHANGED <<requests, answers, keySeen, ledger, unsaved, faults, replays>>

Send(w) ==
    /\ worker[w].phase = "claimed"
    /\ requests' = requests \cup {worker[w].tok}
    /\ worker' = [worker EXCEPT ![w].phase = "waiting"]
    /\ UNCHANGED <<state, token, nextToken, answers, keySeen, ledger, unsaved, faults, replays>>

ReadAnswer(w) ==
    /\ worker[w].phase = "waiting"
    /\ worker[w].tok \in answers
    /\ worker' = [worker EXCEPT ![w].phase = "answered"]
    /\ UNCHANGED <<state, token, nextToken, requests, answers, keySeen, ledger, unsaved, faults, replays>>

\* The sender gives up. The request may still arrive and commit later.
TimeOut(w) ==
    /\ worker[w].phase = "waiting"
    /\ worker[w].tok \notin answers
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ worker' = [worker EXCEPT ![w].phase = "failed"]
    /\ UNCHANGED <<state, token, nextToken, requests, answers, keySeen, ledger, unsaved, replays>>

HoldsLease(w) ==
    IF FinishChecksToken THEN state = "leased" /\ token = worker[w].tok
                         ELSE TRUE

\* A stale report changes nothing. It only clears the worker.
FinishSuccess(w) ==
    /\ worker[w].phase = "answered"
    /\ IF HoldsLease(w)
         THEN /\ state' = "succeeded"
              /\ token' = 0
         ELSE UNCHANGED <<state, token>>
    /\ worker' = [worker EXCEPT ![w] = Idle]
    /\ UNCHANGED <<nextToken, requests, answers, keySeen, ledger, unsaved, faults, replays>>

FinishFailure(w) ==
    /\ worker[w].phase = "failed"
    /\ IF HoldsLease(w)
         THEN /\ state' = "pending"
              /\ token' = 0
         ELSE UNCHANGED <<state, token>>
    /\ worker' = [worker EXCEPT ![w] = Idle]
    /\ UNCHANGED <<nextToken, requests, answers, keySeen, ledger, unsaved, faults, replays>>

\* The process dies at any point. What it knew is gone. Its lease stays until it expires.
WorkerCrash(w) ==
    /\ worker[w].phase # "idle"
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ worker' = [worker EXCEPT ![w] = Idle]
    /\ UNCHANGED <<state, token, nextToken, requests, answers, keySeen, ledger, unsaved, replays>>

\* A lease can expire at any step, including while its worker is alive and busy.
ExpireLease ==
    /\ state = "leased"
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ state' = "pending"
    /\ token' = 0
    /\ UNCHANGED <<nextToken, worker, requests, answers, keySeen, ledger, unsaved, replays>>

\* A lease whose worker is gone must end, and that is not a free choice of the
\* environment: this step has no fault budget, so recovery cannot run out.
ReapAbandonedLease ==
    /\ state = "leased"
    /\ \A w \in Workers : worker[w].tok # token \/ worker[w].phase = "idle"
    /\ state' = "pending"
    /\ token' = 0
    /\ UNCHANGED <<nextToken, worker, requests, answers, keySeen, ledger, unsaved, faults, replays>>

\* Replay requeues a finished delivery. The delivery key does not change.
Replay ==
    /\ state = "succeeded"
    /\ replays < MaxReplays
    /\ replays' = replays + 1
    /\ state' = "pending"
    /\ UNCHANGED <<token, nextToken, worker, requests, answers, keySeen, ledger, unsaved, faults>>

(***************************************************************************)
(* Network faults.                                                         *)
(***************************************************************************)

LoseRequest(r) ==
    /\ r \in requests
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ requests' = requests \ {r}
    /\ UNCHANGED <<state, token, nextToken, worker, answers, keySeen, ledger, unsaved, replays>>

\* The commit stands, and the sender never hears. This is the ambiguous case.
LoseAnswer(r) ==
    /\ r \in answers
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ answers' = answers \ {r}
    /\ UNCHANGED <<state, token, nextToken, worker, requests, keySeen, ledger, unsaved, replays>>

(***************************************************************************)
(* Receiver steps. They run in any order against any request, so a late    *)
(* request from an old worker can overlap a new one.                       *)
(***************************************************************************)

Answer(r) == answers \cup {r}

Receive(r) ==
    /\ r \in requests
    /\ unsaved = "none"
    /\ requests' = requests \ {r}
    /\ IF ReceiverDedupes /\ keySeen
         THEN \* Already applied. A compliant receiver answers duplicate.
              /\ answers' = Answer(r)
              /\ UNCHANGED <<keySeen, ledger, unsaved>>
         ELSE IF KeyAndEffectAtomic /\ CommitBeforeAnswer
           THEN \* One transaction: key and expense together, then the answer.
                /\ keySeen' = TRUE
                /\ ledger' = ledger + 1
                /\ answers' = Answer(r)
                /\ unsaved' = "none"
           ELSE IF ~CommitBeforeAnswer
             THEN \* Broken rule: it answers, and the commit is still to come.
                  /\ answers' = Answer(r)
                  /\ unsaved' = "effect"
                  /\ UNCHANGED <<keySeen, ledger>>
             ELSE \* Broken rule: the key commits first, the effect after.
                  /\ keySeen' = TRUE
                  /\ unsaved' = "key"
                  /\ answers' = answers
                  /\ UNCHANGED ledger
    /\ UNCHANGED <<state, token, nextToken, worker, faults, replays>>

\* Only reachable when a rule was removed.
CommitRest(r) ==
    /\ unsaved # "none"
    /\ keySeen' = TRUE
    /\ ledger' = ledger + 1
    /\ unsaved' = "none"
    /\ answers' = IF unsaved = "key" THEN answers \cup {r} ELSE answers
    /\ r \in 1..nextToken
    /\ UNCHANGED <<state, token, nextToken, worker, requests, faults, replays>>

\* The receiver dies between its two steps. Only reachable when a rule was removed.
ReceiverCrash ==
    /\ unsaved # "none"
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ unsaved' = "none"
    /\ UNCHANGED <<state, token, nextToken, worker, requests, answers, keySeen, ledger, replays>>

(***************************************************************************)
(* The whole system.                                                       *)
(***************************************************************************)

WorkerStep(w) ==
    \/ Claim(w) \/ Send(w) \/ ReadAnswer(w) \/ FinishSuccess(w) \/ FinishFailure(w)

Faulty ==
    \/ \E w \in Workers : TimeOut(w) \/ WorkerCrash(w)
    \/ ExpireLease
    \/ \E r \in requests : LoseRequest(r)
    \/ \E r \in answers : LoseAnswer(r)
    \/ ReceiverCrash

Next ==
    \/ \E w \in Workers : WorkerStep(w)
    \/ \E r \in requests : Receive(r)
    \/ \E r \in 1..nextToken : CommitRest(r)
    \/ ReapAbandonedLease
    \/ Replay
    \/ Faulty

\* Faults are unfair: they may simply stop happening. Progress steps cannot be
\* postponed forever, which is what a retrying system assumes.
Fairness ==
    /\ \A w \in Workers : WF_vars(WorkerStep(w))
    /\ \A w \in Workers : WF_vars(TimeOut(w))
    /\ WF_vars(\E r \in requests : Receive(r))
    /\ WF_vars(\E r \in 1..nextToken : CommitRest(r))
    /\ WF_vars(ReapAbandonedLease)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* What must be true.                                                      *)
(***************************************************************************)

\* The whole project in one line: no expense is ever recorded twice.
NoRepeatedEffect == ledger <= 1

\* The engine never believes in an effect that does not exist.
SucceededHasEffect == state = "succeeded" => ledger = 1

\* At most one worker can finish the delivery: a lease token is never shared.
TokensAreUnique ==
    \A a, b \in Workers :
        (a # b /\ worker[a].tok # 0) => worker[a].tok # worker[b].tok

\* Once succeeded, only a replay reopens it. Requires that a stale report change nothing.
SuccessIsStable ==
    [][state = "succeeded" => (state' = "succeeded" \/ replays' = replays + 1)]_vars

\* If faults stop, the delivery finishes. Requires retries to be able to reach an end.
EventuallyDelivered == <>[](state = "succeeded")

=============================================================================
