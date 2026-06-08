# Workflow definition format — the `.flow` DSL

A workflow definition is a plain-text `.flow` file in a line-oriented DSL.
Each line is `DIRECTIVE [arg1 [arg2 [...]]]`; comments start with `#`; blank
lines are ignored. Indentation is purely cosmetic — the parser tracks the
current open block from directive order, the same trick
[robertchase/fsm](https://github.com/robertchase/fsm) uses.

The file describes a finite state machine: states, the events each state
reacts to, and the actions that fire. It contains **no executable code** —
only declarative directives and field references.

The parser in `eventstream/logic/workflow_parser.py` converts a `.flow` file
into an AST (a JSON-serializable dict) that the runtime walks. Cross-
reference validation (target states exist, action refs resolve, etc.)
happens at register time; missing identifiers fail fast with line numbers.

## File order

The DSL is line-oriented, but it does impose one structural rule:

> **`ACTION` definitions must come before any `STATE` or `DEFAULT`
> directive.** Once the parser sees a `STATE` or `DEFAULT`, the `ACTION`
> keyword no longer opens a new top-level definition — inside the handler
> scopes those directives open, `ACTION <name>` is always a reference.

So the canonical order in a workflow file is:

1. Metadata (`NAME`, `INITIAL`, `DESCRIPTION`)
2. `ACTION` definitions
3. `DEFAULT` directives
4. `STATE` blocks

`DEFAULT` and `STATE` may be interleaved if you prefer (the validator
doesn't care), but actions must precede them all. This is the same shape
robertchase/fsm uses (where `HANDLER` lines sit at the bottom).

## Worked example

```
# Order fulfillment workflow
NAME        order-fulfillment
INITIAL     charging
DESCRIPTION Charges card, ships order, notifies on failure

# Reusable actions
ACTION charge-card
  EMIT payments charge
  PAYLOAD job    $job.id
  PAYLOAD amount $context.order.total

ACTION record-charge
  SET txn_id     $event.data.txn_id
  SET charged_at $job.now

ACTION ship-order
  EMIT fulfillment ship
  PAYLOAD job     $job.id
  PAYLOAD address $context.customer.address

ACTION notify-customer-decline
  EMIT notifications notify
  PAYLOAD job  $job.id
  PAYLOAD type email
  PAYLOAD to   $context.customer.email

# Workflow-wide error handling: any unhandled error → failed terminal state
DEFAULT error failed

# States
STATE charging
  ENTER
    ACTION charge-card
  EVENT success shipping
    ACTION record-charge
  EVENT declined cancelled

STATE shipping
  ENTER
    ACTION ship-order
  EVENT success done

STATE cancelled
  ENTER
    ACTION notify-customer-decline
  EVENT success failed

STATE done   TERMINAL
STATE failed TERMINAL
```

## Top-level directives

| Directive | Argument shape | Purpose |
|---|---|---|
| `NAME <name>` | one token | Workflow identity. Required, set once. |
| `INITIAL <state>` | one token | Starting state. Required, set once. |
| `DESCRIPTION <text>` | rest-of-line | Optional, admin display. |
| `STATE <name> [TERMINAL]` | one or two tokens | Open a state block. Trailing `TERMINAL` marks the state as final (no `EVENT` directives allowed). |
| `ACTION <name>` | one token | Open a named action definition block. The action's operation type is set by the first directive inside the block. |
| `DEFAULT <event> [<next-state>]` | event + optional state | Workflow-wide fallback handler for an event. Same shape as `EVENT`. Fires only when the current state has no `EVENT <event>`. |

There is **no `VERSION` directive in the file** — version is server-assigned
at registration time; each call to `workflows register` bumps it. Having a
version inside the file would make the same source un-re-registerable.

## State blocks

Inside `STATE name`, the following sub-directives open handler scopes:

| Directive | Argument shape | Purpose |
|---|---|---|
| `ENTER` | none | Open the enter-handler scope. Subsequent `ACTION` lines append to the enter sequence. |
| `EXIT` | none | Open the exit-handler scope. Same pattern as `ENTER`. |
| `EVENT <name> [<next-state>]` | event + optional state | Open a handler for `<name>`. Optional trailing state is the transition target. |

Within `ENTER` / `EXIT` / `EVENT`, the only valid sub-directive is `ACTION
<ref>` — a reference to a named action defined at top level. **No
overrides; no inline emits.** Variants of an action are separate
definitions.

Terminal states (`STATE done TERMINAL`) have no inner content — any
`ENTER`/`EXIT`/`EVENT` inside a terminal state is a parse error.

## Action blocks

Each `ACTION name` block contains exactly one operation type. The first
operation directive sets the type; subsequent directives must match.

### `EMIT` — publish an event to a stream

```
ACTION charge-card
  EMIT payments charge
  PAYLOAD job    $job.id
  PAYLOAD amount $context.order.total
```

- `EMIT <stream> <event>` — exactly two arguments.
- `PAYLOAD <key> <value>` — zero or more lines. Each line adds a field to
  the emit's payload. The value is everything after the second token (so
  values can contain spaces).

### `SET` — patch fields into the job context

```
ACTION record-charge
  SET txn_id     $event.data.txn_id
  SET charged_at $job.now
```

- One or more `SET <key> <value>` lines in the block. Each adds (or
  overwrites) a key in the job's context.
- Multiple `SET` lines are allowed in a single action block, so a logical
  "cluster of field updates" stays as one named action.

### `LOG` — emit a log entry

```
ACTION log-cancellation
  LOG order $job.id cancelled by $context.customer.email
```

- Exactly one `LOG <message>` line. Level is implicit (info). The message
  is rest-of-line, with `$`-references interpolated at runtime.

### `TIMER` — schedule a synthetic event back to this job

```
ACTION schedule-followup
  TIMER 24h followup-due
```

- Exactly one `TIMER <duration> <event>` line.
- Duration is a single token: an integer followed by a unit suffix —
  `s` seconds, `m` minutes, `h` hours, `d` days. Examples: `30s`, `10m`,
  `1h`, `7d`. No compound durations in v1.
- `<event>` is the synthetic event name that will be fired back to this
  job's current state when the timer expires.

## Reference syntax

`$`-prefix marks references; anything else is a literal string. Three
reference roots:

| Reference | Resolves to |
|---|---|
| `$job.<path>` | job metadata: `id`, `workflow`, `version`, `state`, `now` |
| `$context.<path>` | a dotted path into the job context |
| `$event.<path>` | the triggering event's body (in handler actions) |

The DSL has **no expressions, no conditionals, no arithmetic** — payload
construction is non-computational by design. The parser accepts any
dotted path syntactically; whether the path resolves at runtime is the
runtime's concern (a missing path triggers the job's error event).

There is no quoting and no escape for a literal `$` — values that start
with `$` are always treated as references. Acceptable for v1; revisit
when a real use case requires literal dollar signs.

## DEFAULT directive

A workflow-wide fallback handler for any event the current state doesn't
explicitly handle. Same shape as `EVENT`:

```
# bare transition, no actions
DEFAULT error failed

# with actions, no transition (stays in current state)
DEFAULT heartbeat
  ACTION record-heartbeat

# with actions and transition
DEFAULT error failed
  ACTION log-failure
```

The most common use is `DEFAULT error failed` — every state's unhandled
errors fall through to a terminal failed state.

## Carry rule (evaluation semantics)

These are runtime semantics, not parser concerns, but they shape how
workflows are authored. Given event `E` arriving in state `S` with handler
`(do=A, goto=T)`:

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
  the last in its sequence.
- **No handler for the carried event → quiesce.** The normal case: an
  `ENTER` action emits a step event to a worker; the state has no handler
  for the worker's event yet, so the job waits for the worker's outcome
  (which arrives as an `ack-with-outcome`).
- **Cascades can loop.** `enter` emits → transition → `enter` emits → … is
  legal. The runtime caps internal events per external trigger; overrun is
  an error transition, not a hang.

## Validation (at register time)

The parser raises with line numbers for syntactic errors. The validator
then walks the AST and rejects:

- Missing or duplicate `NAME` / `INITIAL`.
- `INITIAL` naming a non-existent state.
- `STATE` / `ACTION` / `DEFAULT event` redefined.
- Any `EVENT <name> <target>` or `DEFAULT <name> <target>` where `<target>`
  isn't a defined state.
- Any `ACTION <ref>` inside `ENTER`/`EXIT`/`EVENT` where `<ref>` isn't a
  defined action.
- `ACTION` blocks with no operation directive (`EMIT`/`SET`/`LOG`/`TIMER`).
- `ENTER`/`EXIT`/`EVENT` inside a terminal state.
- `EMIT` blocks with mixed operation types, `SET` mixed with non-SET ops,
  or `PAYLOAD` outside an `EMIT` action.
- References in `SET`/`LOG`/`PAYLOAD` whose path root is not `job`,
  `context`, or `event`.

Runtime checks (not validation): path existence on `$context.foo.bar`.

## AST shape (what the parser produces)

```json
{
  "name": "order-fulfillment",
  "description": "Charges card, ships order, notifies on failure",
  "initial": "charging",
  "defaults": {
    "error": {"do": [], "goto": "failed"}
  },
  "actions": {
    "charge-card": {
      "type": "emit",
      "stream": "payments",
      "event": "charge",
      "payload": {
        "job":    {"ref": "job.id"},
        "amount": {"ref": "context.order.total"}
      }
    },
    "record-charge": {
      "type": "set",
      "fields": {
        "txn_id":     {"ref": "event.data.txn_id"},
        "charged_at": {"ref": "job.now"}
      }
    },
    "schedule-followup": {
      "type": "timer",
      "delay_seconds": 86400,
      "event": "followup-due"
    }
  },
  "states": {
    "charging": {
      "enter": ["charge-card"],
      "exit":  [],
      "events": {
        "success":  {"do": ["record-charge"], "goto": "shipping"},
        "declined": {"do": [],                 "goto": "cancelled"}
      }
    },
    "done":   {"terminal": true},
    "failed": {"terminal": true}
  }
}
```

Values are tagged: a literal string is a bare string, a reference is a
`{"ref": "<path>"}` dict. The runtime resolves refs against the current
`(job, context, event)` triple.

## Storage

Parser output is stored in Redis along with the original source text:

```
eventstream:workflows                       SET of names
eventstream:workflow:<name>:versions        SORTED SET version → ts
eventstream:workflow:<name>:<version>       HASH {source, ast}
```

The source is kept so `workflow show <name> --source` round-trips the
original text exactly; the runtime always uses the AST.

## Open / deferred

- **Overrides on action references.** Today an `ACTION ref` is the
  complete line — no per-call payload overrides. A future relaxation would
  allow indented `PAYLOAD` lines after the reference. Workable; deferred
  until a real use case justifies the merge rule.
- **Conditional guards.** v1 branches purely on event *name*. Context-
  conditional transitions (`WHEN amount > 100 GOTO ...`) would require an
  expression sublanguage — which conflicts with the no-expressions stance
  on payloads. Open and deferred.
- **Parallel regions / fan-out.** One current state at a time. Statechart
  semantics with parallel regions are out of scope.
- **Compound timer durations.** `1h30m` syntax. Defer.
- **Escape for literal `$`.** No escape today. Add when needed.
- **VERSION pinning in the file.** Today version is fully server-side.
  Could add `EXPECT_VERSION N` for safety. Defer.
