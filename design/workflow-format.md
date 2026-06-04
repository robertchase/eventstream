# Workflow definition format

The static format for workflow definitions consumed by the jobs
orchestrator. See `design/jobs.md` for the runtime that executes these.

A definition is structured data (JSON on the wire; YAML shown here for
readability). It describes a finite state machine: states, the events
each state reacts to, and the actions that fire. It contains **no
executable code** — only declarative bindings and field references.

## Top-level structure

```yaml
name: order-fulfillment      # workflow identity
version: 1                   # integer, server-assigned on register
initial: charging            # starting state

actions:                     # optional: reusable named actions
  notify-customer:
    emit:
      stream: notifications
      event: notify
      payload: { job: $job.id, type: email, to: $context.customer.email }

states:
  charging:
    enter: [ ... ]           # action sequence on entering the state
    exit:  [ ... ]           # action sequence on leaving the state
    on:                      # event handlers
      success: { do: [ ... ], goto: shipping }
      declined: { do: [ notify-customer ], goto: cancelled }
  done:   { terminal: true }
  failed: { terminal: true }
```

## States and handlers

A state has optional `enter` / `exit` action sequences and an `on` map of
event handlers. A handler is:

```yaml
<event>:
  do:   [ action, action, ... ]    # the action sequence
  goto: <target-state>             # optional transition (zero or one)
```

Per the execution rules, a handler is "one or more actions followed by
zero or one transition." `goto` may be omitted (internal handling, stays
in the state). `enter`/`exit` are bare action sequences — no `goto`.

A `terminal: true` state has no `on` handlers; reaching it ends the job.

## Actions

There is **one primitive — emit an event** — plus a few non-emitting
helpers. An action's meaning is supplied by the workflow, not fixed by
the system. The same action name in a different workflow can bind to a
different event, stream, and payload.

### emit

```yaml
emit:
  stream:  notifications     # which stream the event lands on
  event:   notify            # event type
  payload: { ... }           # templated (see below)
```

The event is published to its stream — available to any consumer
(worker) of that stream. It is *also* offered back to the FSM per the
evaluation rules below.

### Non-emitting actions

These produce no event (contribute no carry):

```yaml
set:   { txn_id: $event.data.txn_id }      # patch-merge into job context
log:   { level: info, message: "charged $context.order.total" }
timer: { after: 10m, event: timeout }      # schedule an event to this job
```

`timer` uses the same sweep as scheduled delivery (`design/api.md`), but
the event is delivered back to *this job* rather than to a stream — the
mechanism behind per-state deadlines.

### Inline vs named

An action is either written inline or referenced from the `actions:`
section by name. Named actions may be referenced with a shallow override:

```yaml
do:
  - notify-customer                              # by name
  - notify-customer: { payload: { type: sms } }  # name + shallow override
  - emit: { stream: audit, event: charged, payload: { job: $job.id } }  # inline
```

## Payload templating

Payloads are built from **field references and literals only — no
expressions, no conditionals, no arithmetic**. Keeping payloads
non-computational is what preserves "the system does not execute jobs."

References:

| Reference            | Resolves to                                    |
|----------------------|------------------------------------------------|
| `$job.id`            | the job instance id                            |
| `$job.workflow`      | workflow name                                  |
| `$job.version`       | workflow version                               |
| `$job.state`         | current state name                             |
| `$context.<path>`    | dotted path into the job context               |
| `$event.<path>`      | the triggering event's payload/data (in `do`)  |

Everything else is a literal (string, number, bool, object, array).
A reference to a missing path is a runtime error → the job's error
handling fires (see `design/jobs.md`). Default/optional-reference syntax
is an open question.

## Evaluation semantics

How one triggering event is processed (run-to-completion). Given event
`E` in state `S`, with handler `(do=A, goto=T)`:

```
run A in order
carry = event emitted by the LAST action of A   # only the last counts

if no T:
  if carry: re-process carry in S               # cascade in current state
  else:     quiesce in S
else:
  run S.exit, then T.enter                       # order: A → exit → enter
  carry = event emitted by the last action that ran (enter, else exit, else A)
  if carry: process carry in T
  else:     quiesce in T

repeat until a step yields no carry → persist, wait for next event
```

Consequences worth knowing when authoring:

- **Only the last action's event carries.** Every emit still reaches its
  stream, but to make the FSM react to an emission, that emission must be
  last in its sequence.
- **No handler for the carried event → quiesce.** This is the normal
  case: an `enter` action emits a step to a worker; the state has no
  handler for that event, so the job waits for the worker's outcome.
- **Cascades can loop.** `enter` emits → transition → `enter` emits → …
  is legal. The runtime caps internal events per external trigger (see
  `design/jobs.md`); overrun is an error transition, not a hang.

## Validation (at register time)

- `initial` names an existing state.
- Every `goto` target exists.
- Every named-action reference resolves in `actions:`.
- Terminal states have no `on` handlers.
- All `$context`/`$event` references are syntactically valid (path
  existence is necessarily a runtime check).

## Complete example

```yaml
name: order-fulfillment
version: 1
initial: charging
actions:
  notify-customer:
    emit:
      stream: notifications
      event: notify
      payload: { job: $job.id, type: email, to: $context.customer.email }
states:
  charging:
    enter:
      - emit:
          stream: payments
          event: charge
          payload: { job: $job.id, amount: $context.order.total }
    on:
      success:
        do: [ { set: { txn_id: $event.data.txn_id } } ]
        goto: shipping
      declined:
        do: [ notify-customer ]
        goto: cancelled
  shipping:
    enter:
      - emit:
          stream: fulfillment
          event: ship
          payload: { job: $job.id, address: $context.customer.address }
    on:
      success: { goto: done }
  cancelled:
    enter: [ notify-customer ]
    on:
      success: { goto: failed }
  done:   { terminal: true }
  failed: { terminal: true }
```

## Open questions / deferred

- **Bare transitions.** Rule 1 reads "one or more actions"; but
  `success: { goto: shipping }` with no `do` is extremely common.
  Recommend allowing empty `do`. Confirm.
- **Conditional guards.** v1 branches on event *name* only
  (`success` / `declined`). Context-conditional transitions
  (`amount > 100`) need an expression sublanguage — which conflicts with
  the no-expressions boundary. Deferred until that tradeoff is decided.
- **Missing-reference behavior.** Error vs. default-value syntax
  (`$context.foo ?? "x"`). Defaulting reintroduces a little computation;
  deferred.
- **Parallel regions / fan-out.** A single FSM is in one state at a time.
  Concurrent steps with a join are out of scope for v1 (the statechart
  cliff from earlier discussion).
- **Payload projection.** Workers currently receive whatever the payload
  template builds. A declared per-step "needs these context keys"
  projection is deferred.
