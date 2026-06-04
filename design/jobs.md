# Jobs — decoupled workflow orchestration

## Role

A workflow orchestrator that **holds the state of in-flight jobs and
emits events based on that state — but never executes a step itself.**
The flow through the steps (the FSM) is separated from the execution of
any given step (done by ordinary consumers).

This is a layer *on top of* streams and subscriptions. The orchestrator
advances a finite state machine; entering a state emits events to
streams; workers consume those events, do the real work, and report an
outcome; the outcome drives the next transition.

Definitions are described in `design/workflow-format.md`. This document
covers the runtime: instances, identity, context, observability, and how
it reuses the existing event bus.

## Vocabulary

- **Workflow** — a registered FSM definition (versioned).
- **Job** — a running instance of a workflow.
- **State** — a node in the FSM; emits events via its actions.
- **Action** — a declarative event-emitter (or context/log/timer helper).
- **Outcome** — a worker's reported result, injected as an event that
  drives a transition.

## It is mostly already built

The job runtime reuses the existing primitives almost wholesale:

| Job concept            | Existing primitive                          |
|------------------------|---------------------------------------------|
| A step to be done      | Event published to a stream                 |
| Worker that runs a step| Subscription + its workers                  |
| Step started           | Pull (lease)                                |
| Step done, with result | **Ack carrying an outcome**                 |
| Worker crashed mid-step| Lease timeout → redelivery                  |
| Step permanently failing| DLQ → automatic error transition           |
| Per-state deadline     | Scheduled sweep (`timer` action)            |

The only new API surface against the core bus is **ack-with-outcome**.

## Lifecycle

```
register workflow  →  create job (enters initial state)  →
  emit step events  →  workers report outcomes  →  transitions  →
  ... → terminal state (done / failed)
```

Creating a job runs the initial state's `enter` actions, which typically
emit the first step and then quiesce, waiting for an outcome.

## Job identity

Every job has a stable id (`job_...`, same format question as event ids
in `design/api.md`). The id is:

- **carried in every emitted event's payload** (`$job.id`), so a worker
  echoes it back on ack; and
- **mapped server-side** as `emitted_event_id → (job_id, expected_event)`
  so the runtime can route an ack-with-outcome to the right job *and*
  make the transition idempotent — a redelivered double-ack advances the
  FSM exactly once.

The job_id is also the correlation key for all logging (below).

## ack-with-outcome

The one extension to the core API. A worker reports a business result by
attaching a body to the existing ack:

```
POST /v1/subscriptions/payments-worker/ack/evt_01J...
{ "outcome": "declined", "data": { "reason": "insufficient_funds" } }

→ 204
```

- `outcome` is the event name matched against the current state's `on`
  handlers.
- `data` is merged into the job context (and available as `$event.data`
  in the handler's actions).
- For non-job events, the body is omitted and ack behaves exactly as
  today.

Business outcome (`success`/`declined`) and technical failure (worker
died → lease timeout → redelivery → DLQ) are different axes. Only the
former reaches the FSM as an event; the latter is handled by redelivery,
and a DLQ'd step trips an automatic `error` event so a job can't hang
forever.

## Context management

Each job carries a `context` blob, supplied at creation and evolved by
actions.

- **Patch-merge, not in-place.** `set` actions and ack `data` merge into
  context; each merge is recorded. This gives replayable history and
  idempotent merges. Default merge is shallow, last-write-wins.
- **Projected into payloads.** Emit payloads reference `$context.<path>`;
  for v1 the author decides what each step receives (no automatic
  whole-context dump). A declared per-step projection is deferred.
- **Redaction.** Context will hold secrets (payment tokens). The
  transition trace logs control-flow and context *deltas by key*, not
  raw values, unless a key is marked loggable. Logging raw context is
  opt-in.
- **Size.** Context is for control-flow data, not payloads. A soft cap +
  warning; hard limit TBD.

## Failure and safety

- **DLQ → error transition.** When a step event exhausts redelivery and
  lands in the DLQ, the runtime feeds an `error` event to the job's
  current state. Workflows handle it like any other event (e.g.,
  `error: { goto: failed }`).
- **Cascade budget.** Internal-event cascades (see evaluation semantics
  in `design/workflow-format.md`) are capped per external trigger.
  Exceeding the budget is a logged error transition, not a hang.
- **Idempotent transitions.** Guaranteed by the `event_id → job` map; a
  given step instance advances the FSM once.

## Observability and logging

A cascading FSM is undebuggable without a trace. Each macro-step appends
a record: triggering event, state-before, actions run, events emitted,
context delta (keys), state-after.

- **job_id threads every log line** — the jobs analogue of meander's
  `ConnectionId`. Logging and identity are the same mechanism.
- **Event-sourced (proposed).** The transition log is the source of
  truth; job state and context are a fold over it. This makes "the system
  holds state" literal and gives replay/audit for free. Decide early —
  it shapes storage.

## API surface (proposed)

To be folded into `design/api.md` once settled. Kept here while the jobs
feature stabilizes.

```
POST   /v1/workflows                  register a workflow definition
GET    /v1/workflows                  list
GET    /v1/workflows/{name}           get (?version=)
DELETE /v1/workflows/{name}           delete / deprecate

POST   /v1/jobs                       create  { workflow, version?, context }
GET    /v1/jobs                       list  (?workflow= ?state= ?status=)
GET    /v1/jobs/{id}                  inspect: state, context, status, history
DELETE /v1/jobs/{id}                  cancel
```

Plus the ack-with-outcome extension to the existing
`POST /v1/subscriptions/{sub}/ack/{id}`.

## Logic modules

Per `design/logic.md`, the work lives in logic, called identically by the
CLI and the meander server.

```python
# eventstream/logic/workflows.py
def register(definition: dict) -> dict: ...        # validates, versions
def list_() -> list[dict]: ...
def get(name: str, version: int | None = None) -> dict: ...
def delete(name: str) -> None: ...

# eventstream/logic/jobs.py
def create(workflow: str, context: dict, *,
           version: int | None = None) -> dict: ...   # {id, state}
def get(id: str) -> dict: ...                          # state, context, history
def list_(*, workflow: str | None = None,
          state: str | None = None,
          status: str | None = None) -> list[dict]: ...
def cancel(id: str) -> None: ...
def advance(id: str, event: str,
            data: dict | None = None) -> dict: ...     # feed one event to a job
```

`advance` is the engine entry point. The ack-with-outcome path resolves
the event's `job_id` and calls `jobs.advance(job_id, outcome, data)`;
`timer` firings and DLQ `error` events call it too.

## Design decisions

### The orchestrator never executes

Actions only emit events, patch context, log, or set timers. All real
work is in stream consumers. This is the founding constraint; it is why
payload templating is non-computational (`design/workflow-format.md`).

### Jobs reuse the bus, not a parallel mechanism

Steps are events, workers are subscriptions, completion is ack. No new
delivery/lease/retry machinery — only state-holding and event-emission
are added.

### Outcome is richer than ack

A bare ack means "processed." A job step's ack carries *which* outcome,
because the FSM branches on it. Backward compatible: no body = today's
ack.

## Open questions

- **Event-sourced vs. state + separate audit log.** (Storage-shaping.)
- **Job id format** — inherits the `design/api.md` decision.
- **Workflow versioning of in-flight jobs.** Does a running job pin its
  version (yes, proposed) — and can it be migrated to a new version?
- **Cancel semantics** — does cancel run a cleanup/exit path or hard-stop?
- **Job retention** — when are terminal jobs and their history GC'd?
