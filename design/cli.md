# CLI

## Role

A click-based CLI that exercises the `eventstream/logic/` layer
directly — not via HTTP. The CLI and the meander HTTP server are
sibling adapters over the same Python functions; their behavior cannot
drift. See `design/logic.md` for the logic layer.

Invoke as `uv run -m eventstream.cli <subcommand>`. A console-script
entry point in `pyproject.toml` also exposes `eventstream <subcommand>`.

## Command surface

Top-level verbs for the hot path; noun-grouped subcommands for admin.

### Producer

```
eventstream publish <stream> [--key K]
                              [--payload JSON | --payload-file F | -]
                              [--deliver-at TIME]
                              [--idempotency-key K]
```

### Consumer

```
eventstream pull <sub> [--wait 30s] [--ack] [--json]
eventstream ack  <sub> <event-id>
```

### Stream admin

```
eventstream stream create <name>
eventstream stream list
eventstream stream show   <name>
eventstream stream delete <name>
```

### Subscription admin

```
eventstream sub create <name> --stream <stream>
eventstream sub list   [--stream <stream>]
eventstream sub show   <name>          # lag, in_flight, oldest_unacked_age
eventstream sub delete <name>
```

### Scheduled events

```
eventstream scheduled list   <stream>
eventstream scheduled cancel <stream> <schedule-id>
```

### DLQ

```
eventstream dlq peek  <sub>
eventstream dlq drop  <sub> <event-id>
eventstream dlq purge <sub>
```

## Worked examples

```bash
# publish, payload from stdin
echo '{"order_id":123}' | eventstream publish orders --key order-123 -

# publish, scheduled
eventstream publish reminders --key user-42 \
    --payload '{"msg":"hi"}' \
    --deliver-at 2026-05-30T18:00:00Z

# pull, print as JSON, leave lease open
eventstream pull billing-worker --json

# pull and auto-ack (one-shot scripts)
eventstream pull billing-worker --ack

# admin
eventstream sub show billing-worker
# → stream: orders   lag: 47   in_flight: 2   oldest_unacked: 12s
```

## Module layout

```
eventstream/cli/
    __init__.py        # gathers subcommands, entry point for -m
    publish.py
    pull.py
    ack.py
    stream.py          # `stream` command group
    sub.py             # `sub` command group
    scheduled.py
    dlq.py
```

Each module imports the corresponding logic module and translates
arguments → call → output:

```python
# eventstream/cli/publish.py
from eventstream.logic import events

@click.command()
@click.argument("stream")
@click.option("--key")
@click.option("--payload")
@click.option("--payload-file", type=click.File("r"))
@click.option("--deliver-at", type=click.DateTime())
@click.option("--idempotency-key")
def publish(stream, key, payload, payload_file, deliver_at,
            idempotency_key):
    """Publish an event to a stream."""
    raw = _read_payload(payload, payload_file)
    result = events.publish(stream, raw, key=key,
                            idempotency_key=idempotency_key,
                            deliver_at=deliver_at)
    click.echo(json.dumps(result))
```

The CLI is a translation layer only: parse arguments, call into logic,
format output. No business decisions live here.

## Design choices

### Pull defaults to not acking

Two-step `pull` → process → `ack` mirrors the API and makes "do work
between pull and ack" the obvious shape. `--ack` is a convenience for
one-shot scripts that don't need lease safety.

### Payload input is flexible but exclusive

`--payload` inline, `--payload-file`, or `-` (stdin). Exactly one must
be given for `publish`. Enforced by a click mutually-exclusive group.

### `--json` for machine output

Read commands (`pull`, `*-list`, `*-show`, `dlq peek`) default to a
compact human-readable form. `--json` switches to structured output for
scripting.

### No server URL flag

The CLI does not take `--url`. It calls logic directly, which means it
talks to the same backend (Redis) via the same config (`eventstream/
config.py`) the server uses. The CLI is an ops/dev tool running in the
same environment as the server, not a remote HTTP client.

### Exit codes

A small decorator translates domain exceptions to exit codes:

| Exception class    | Exit |
|--------------------|------|
| `*NotFound`        | 1    |
| `*AlreadyExists`   | 2    |
| `Invalid*`         | 3    |
| `LeaseExpired`     | 4    |
| other              | 99   |

Error messages go to stderr; structured data (if any) to stdout.

## Deferred

- `eventstream consume <sub> --exec CMD` — a pull/process/ack loop.
  Adds signal handling, lease extension, parallelism flags. Wait for
  a real use case; for now compose with shell.
- Batch publish (`publish --batch file.jsonl`) — defer with the API.
- Shell completion, man pages — nice to have.
