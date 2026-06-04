# Logic layer

## Role

The single source of truth for what the system does. Plain Python
functions in `eventstream/logic/`, organized by noun. Two adapters wrap
them:

- `eventstream/cli/` — click commands; import and call directly.
- `eventstream/server/` — meander HTTP routes; register the same
  functions as handlers.

```
              ┌────────────────────────┐
              │  eventstream/logic/    │   real work
              │    streams.py          │
              │    events.py           │
              │    subscriptions.py    │
              │    scheduled.py        │
              │    dlq.py              │
              └────────────────────────┘
                  ▲                ▲
                  │                │
        ┌─────────┘                └─────────┐
        │                                    │
 ┌──────────────┐                     ┌──────────────┐
 │ cli/ (click) │                     │ server/      │
 │              │                     │ (meander)    │
 └──────────────┘                     └──────────────┘
```

This is meander's own design idiom: a handler is just a plain annotated
function that knows nothing about HTTP. The server registers logic
functions as routes; the CLI imports them. HTTP-specific concerns (auth
headers, path-parameter extraction, content-type quirks) live in meander
`before=` hooks attached to routes, not in the logic functions
themselves.

The payoff: the CLI and the server cannot drift, because they share the
implementation rather than reimplementing it.

## Conventions

- Functions are **type-annotated**. Meander uses annotations for
  validation and coercion; click does the same in its own way.
- Functions are **sync** unless they need to be `async`. Long-poll
  `pull` is the obvious `async` candidate; everything else is sync.
- **No HTTP types.** Logic does not import `meander`, does not raise
  `HTTPException`, does not touch a `Request`. Domain exceptions only.
- **Translate at the edge.** The server side maps domain exceptions to
  HTTP status via a small adapter (decorator or `before` hook). The CLI
  maps them to nonzero exit codes + stderr.
- Return values are JSON-serializable: dicts, lists, primitives. Plain
  `dict` for now; revisit if a typed result class earns its keep.

## Module layout

```
eventstream/logic/
    __init__.py
    exceptions.py        # domain exceptions
    streams.py
    events.py
    subscriptions.py
    scheduled.py
    dlq.py
```

## Function signatures

### `events.py`

```python
def publish(stream: str, payload: dict, *,
            key: str | None = None,
            idempotency_key: str | None = None,
            deliver_at: datetime | None = None) -> dict:
    """Append (sync) or schedule (future) an event.

    Returns {"id": "evt_..."} for synchronous publish,
            {"schedule_id": "sch_..."} when deliver_at is in the future.
    """

async def pull(subscription: str,
               wait: timedelta = timedelta(seconds=30)) -> dict | None:
    """Long-poll one event. None on timeout.

    Returns {"id", "key", "payload", "ts"} on success.
    """

def ack(subscription: str, event_id: str) -> None:
    """Release lease and advance the subscription cursor past event_id."""
```

### `streams.py`

```python
def create(name: str) -> dict: ...         # {"name"}; idempotent
def list_() -> list[dict]: ...             # trailing _ avoids builtin shadow
def show(name: str) -> dict: ...           # metadata
def delete(name: str) -> None: ...
```

### `subscriptions.py`

```python
def create(name: str, stream: str) -> dict: ...    # idempotent, starts at tail
def list_(stream: str | None = None) -> list[dict]:
    """List subscriptions, optionally filtered by stream."""
def show(name: str) -> dict:
    """{name, stream, lag, in_flight, oldest_unacked_age}."""
def delete(name: str) -> None: ...
```

### `scheduled.py`

```python
def list_(stream: str) -> list[dict]: ...
def cancel(stream: str, schedule_id: str) -> None: ...
```

### `dlq.py`

```python
def peek(subscription: str) -> list[dict]: ...
def drop(subscription: str, event_id: str) -> None: ...
def purge(subscription: str) -> None: ...
```

## Domain exceptions

```python
# eventstream/logic/exceptions.py

class StreamNotFound(KeyError): pass
class StreamAlreadyExists(ValueError): pass
class SubscriptionNotFound(KeyError): pass
class SubscriptionAlreadyExists(ValueError): pass
class EventNotFound(KeyError): pass
class LeaseExpired(RuntimeError): pass
class InvalidPayload(ValueError): pass
class InvalidDeliverAt(ValueError): pass
```

Mapping to HTTP (done at the server edge, **not** in logic):

| Exception          | HTTP |
|--------------------|------|
| `*NotFound`        | 404  |
| `*AlreadyExists`   | 409  |
| `Invalid*`         | 400  |
| `LeaseExpired`     | 410  |
| anything else      | 500  |

## Open questions

- **Async vs sync.** `pull` benefits from `async` for non-blocking
  long-poll; everything else is short and sync. The CLI will need
  `asyncio.run()` for `pull`. Acceptable, but worth being explicit.
- **Result types.** Plain `dict` is simplest and serializes directly.
  Dataclasses with `to_dict()` give type safety but add a layer. Sticking
  with `dict` until something breaks.
