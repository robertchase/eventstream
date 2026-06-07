# HTTP server

A meander-based server that exposes two parallel route layers over the
existing `eventstream/logic/` functions:

1. **JSON API** under `/v1/...` — exactly the shape `design/api.md` already
   specifies; handlers are the logic functions themselves (meander's idiom).
2. **HTML admin** under `/` — a small Jinja2-rendered admin viewer with
   HTMX-driven section refresh. Read-only for v1.

The CLI keeps doing write operations (publish, ack, dlq drop/purge) until a
later round adds those endpoints. This first server pass is for *seeing*
what's happening, not for changing it.

## Module layout

```
eventstream/server/
    __init__.py          # builds the server, exposes run()
    __main__.py          # `python -m eventstream.server`
    routes.py            # all add_route() calls in one place
    web.py               # HTML handlers (render templates)
    hooks.py             # meander before-hooks (path args → content)
    templates/
        layout.html
        index.html
        stream.html
        subscription.html
    static/
        style.css
        htmx.min.js      # vendored
```

`routes.py` registers logic functions directly as JSON handlers; meander
serializes their returned dicts. Web handlers in `web.py` call the same
logic and render templates.

## URL surface

### JSON

```
GET /v1/streams                              streams.list_
GET /v1/streams/{name}                       streams.show
GET /v1/streams/{name}/events                streams.peek    (?count, ?reverse)
GET /v1/subscriptions                        subscriptions.list_  (?stream)
GET /v1/subscriptions/{name}                 subscriptions.show
GET /v1/subscriptions/{name}/pending         subscriptions.pending (?count)
GET /v1/subscriptions/{name}/dlq             dlq.peek              (?count)
```

Path captures are moved into `request.content` by a `path_args` before-hook
so the logic function's signature stays unaware of HTTP.

### HTML

```
GET /                                        overview: streams + subs tables
GET /streams/{name}                          stream detail: show + peek
GET /subscriptions/{name}                    sub detail: show + pending + dlq
GET /static/{path}                           CSS, htmx.min.js
```

Each HTML route returns either the full page (normal request) or just its
content fragment (HTMX request, identified by the `HX-Request` header).
Same URL either way — direct visits and HTMX refreshes hit the same handler.

## HTMX refresh model

Each content panel has `hx-get="<self>"` plus `hx-trigger="every Ns"`. The
panel polls itself; the server returns the fragment; HTMX swaps it. No
client state, no JS beyond htmx itself.

Default refresh cadences:
- overview: 5s
- stream / subscription detail: 3s

Configurable per panel in the template if a use case demands it.

## Rendering decisions

- **Jinja2 templates with autoescape.** Cheap correctness on payload/keys
  that flow into HTML; ~200KB dep, no build step.
- **One layout + per-page template.** Each page calls logic and passes a
  dict into the template. No per-section partials in v1 — refresh swaps the
  whole content panel.
- **Vendored htmx.min.js.** ~14KB, no CDN dependency at runtime.
- **No client JS we author.** htmx attributes only.

## CLI integration

```
eventstream server [--host HOST] [--port PORT]
```

Reads `EVENTSTREAM_HOST` and `EVENTSTREAM_PORT` from the environment with
defaults `127.0.0.1` and `8080`. Backed by `meander.web.add_server() / run()`.

## Open / deferred

- **Write endpoints.** Publish, ack, DLQ drop/purge over HTTP. Add when
  the web UI needs them (forms with HTMX `hx-post`).
- **Scheduled events and DLQ redeliver views.** Land with their features.
- **Auth.** Per `design/api.md` it's TBD; admin UI assumes a trusted local
  network for now.
- **Per-panel refresh granularity.** If a panel becomes expensive, split it
  into its own partial route.
