# Writing workflows

A **workflow** is a finite state machine that eventstream runs as a *job*. The
key idea: eventstream holds the state and emits events — it never does the work
itself. Each step is handed to an ordinary stream consumer (a worker), and the
worker's reply drives the machine to its next state.

You write a workflow as a plain-text `.flow` file, register it, and create jobs
from it. This guide is a practical walkthrough; for the full grammar, AST, and
validation rules see [design/workflow-format.md](design/workflow-format.md),
and for the runtime see [design/jobs.md](design/jobs.md).

## A complete example

```
# orders.flow — charge a card, then ship; fail on error.
NAME        orders
INITIAL     charging
DESCRIPTION Charge the card, ship the order.

# --- actions (must come before any STATE or DEFAULT) ---

ACTION charge-card
  EMIT payments charge
  PAYLOAD job    $job.id
  PAYLOAD amount $context.order.total

ACTION record-charge
  SET txn $event.data.txn_id

ACTION ship-order
  EMIT fulfillment ship
  PAYLOAD job     $job.id
  PAYLOAD address $context.customer.address

# --- workflow-wide fallback ---

DEFAULT error failed

# --- states ---

STATE charging
  ENTER
    ACTION charge-card
  EVENT charged shipping
    ACTION record-charge
  EVENT declined cancelled

STATE shipping
  ENTER
    ACTION ship-order
  EVENT shipped done

STATE cancelled TERMINAL
STATE done      TERMINAL
STATE failed    TERMINAL
```

Reading it: a new job starts in `charging`, whose `ENTER` emits a `charge`
event to the `payments` stream. The job then waits. When a payments worker
reports `charged`, the job records the transaction id into its context and
moves to `shipping`, which emits a `ship` event — and so on, until a terminal
state (`done`, `cancelled`, `failed`).

## The file, directive by directive

It's line-oriented: `DIRECTIVE args`. Blank lines and `#` comments are ignored.
Indentation is cosmetic — structure comes from directive order.

### Metadata (top of file)

```
NAME        orders        # required: the workflow's name
INITIAL     charging       # required: the starting state
DESCRIPTION Charge a card  # optional: shown in the admin UI
```

There is no `VERSION` — the server assigns one each time you register.

### Actions

An action is a named, declarative operation. **All action definitions must
come before the first `STATE` or `DEFAULT`.** Each block is exactly one of:

**`EMIT`** — publish an event to a stream (this is how work reaches a worker):

```
ACTION charge-card
  EMIT payments charge          # EMIT <stream> <event-type>
  PAYLOAD job    $job.id        # PAYLOAD <key> <value>, repeatable
  PAYLOAD amount $context.order.total
```

**`SET`** — write fields into the job's context (one or more lines):

```
ACTION record-charge
  SET txn        $event.data.txn_id
  SET shipped_to $context.customer.address
```

**`LOG`** — emit a log line (info level), with references interpolated:

```
ACTION note-it
  LOG charged order for $context.customer.email
```

**`TIMER`** — schedule an event back to this job after a delay:

```
ACTION arm-timeout
  TIMER 10m timeout             # TIMER <duration> <event-name>
```

Durations are `<n>s`, `<n>m`, `<n>h`, or `<n>d`. Timers fire only when a
sweeper is running (`eventstream jobs sweep`, or set `EVENTSTREAM_SWEEP_INTERVAL`
on the server).

### States

```
STATE charging
  ENTER                  # actions run on entering the state
    ACTION charge-card
  EXIT                   # actions run on leaving it (optional)
    ACTION note-it
  EVENT charged shipping # on event `charged`, run actions then go to `shipping`
    ACTION record-charge
  EVENT declined cancelled   # a bare transition (no actions) is fine

STATE done TERMINAL      # terminal states end the job; they take no events
```

Inside `ENTER`, `EXIT`, and `EVENT`, the only directive is `ACTION <name>` —
a reference to an action defined at the top. References only; no inline emits
and no per-reference overrides (define a second action if you need a variant).

### DEFAULT

A workflow-wide fallback for an event a state doesn't handle. Same shape as
`EVENT`. The common use is routing failures:

```
DEFAULT error failed
```

Now any state that doesn't explicitly handle `error` falls through to the
`failed` terminal state. (eventstream itself fires `error` at a job when one
of its steps lands in the dead-letter queue.)

## References

Values in `PAYLOAD`, `SET`, and `LOG` are either literals or `$`-references:

| Reference | Resolves to |
|---|---|
| `$job.id` | the job's id (the only job field exposed today) |
| `$context.<path>` | a dotted path into the job's context |
| `$event.<path>` | the triggering event's body — e.g. `$event.data.txn_id` |

Anything not starting with `$` is a literal string. There are **no
expressions** — no arithmetic, conditionals, or function calls; references and
literals only. A reference to a missing path fails the step (the job takes its
`error` transition).

Note `$event` only has meaning inside an `EVENT` handler. `ENTER`/`EXIT`
actions run without a triggering event, so they should reference `$context`
and `$job.id` only.

## How a job runs (the mental model)

1. **Create** a job → it enters `INITIAL` and runs that state's `ENTER`
   actions. Typically `ENTER` emits a step to a stream, then the job
   **quiesces** (waits).
2. A **worker** pulls that event from the stream, does the real work, and
   **acks with an outcome** (a workflow event name) plus optional data.
3. The bus routes that ack back to the job, which runs the matching `EVENT`
   handler — its actions, then the transition — entering the next state and
   running *its* `ENTER`, and so on.
4. This repeats until a **terminal** state.

One rule to keep in mind: within an action sequence, only the **last** action's
emitted event is offered back to the FSM. Every emit still reaches its stream;
but to make the machine react to one, put it last. The usual pattern — a single
`EMIT` in `ENTER`, then quiesce until the worker replies — falls out of this
naturally.

## Running one

```sh
# register (prints the assigned version)
eventstream workflow register orders.flow

# a worker needs a subscription on each stream the workflow emits to
eventstream sub create payments-worker --stream payments

# start a job with initial context
eventstream jobs create orders \
    --context '{"order":{"total":4200},"customer":{"address":"1 Main St"}}'

# a worker: pull the emitted step, do the work, ack with the outcome
eventstream pull payments-worker --json
eventstream ack payments-worker <event-id> --outcome charged --data '{"txn_id":"tx_1"}'

# inspect the job (state, context, transition history)
eventstream jobs show <job-id>

# fire due timers (run continuously in production)
eventstream jobs sweep --interval 1
```

The same actions are available over the HTTP API and in the admin UI
(`/workflows`, `/jobs`), which draws the FSM and highlights a running job's
current state.

## Gotchas

- **Action definitions go before `STATE`/`DEFAULT`.** Otherwise the parser
  can't tell an action *definition* from a *reference*; it reports the line.
- **Only the last action in a sequence carries an event** to the FSM.
- **Terminal states take no events** — listing one is a parse error.
- **`ENTER`/`EXIT` have no `$event`** — reference `$context` / `$job.id` only.
- **No expressions** — branching is by event name, not by condition.
