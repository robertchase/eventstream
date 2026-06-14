# Authentication — bearer tokens (API keys)

Machine-to-machine authentication for the HTTP **`/v1/*` JSON API**. Service
callers present a bearer token; a meander `before` hook verifies it against
tokens stored (hashed) in Redis and checks scope. Token issuance and
revocation are CLI/Redis operations — the trusted control plane.

This is the `design/api.md` "Auth model: TBD … token-based" line, made
concrete. Principals are services, not humans: no login, no sessions.

## Scope of this design

- **Gated:** the `/v1/*` JSON API.
- **Not gated (this iteration):** the HTML admin viewer (`/`, `/streams/…`,
  `/jobs/…`, `/static/…`). Browsers don't send `Authorization` cleanly, and
  bearer-token-in-browser is a known pain. The dashboard stays on the
  trusted-network posture; a separate mechanism (basic auth / SSO / proxy)
  is deferred.
- **Opt-in:** auth is **off by default** (`EVENTSTREAM_AUTH=0`), so existing
  deployments are unchanged. Turning it on requires every `/v1/*` request to
  carry a sufficiently-scoped token.

## Two planes (unchanged from the rest of the system)

- **CLI / direct Redis = trusted control plane.** Minting and revoking
  tokens (`eventstream key …`) happens here. Whoever has the Redis URL is
  the admin who issues credentials — issuance is itself privileged, so this
  is correct. The CLI does not authenticate; it never goes through HTTP.
- **HTTP `/v1/*` = authenticated data plane.** Service callers present
  tokens; the `before` hook checks them.

## Hard requirement: TLS

Bearer tokens are secrets in transit. The API **must** run behind TLS
(service mesh, reverse proxy, or gateway terminating TLS). Running the
token-gated API over plaintext on an untrusted path leaks credentials.
In-process TLS (meander's `ssl_certfile`/`ssl_keyfile`) is available but not
wired here — terminate upstream. This is a deployment obligation, not code.

## Token format

```
es_<keyid>_<secret>
```

- `keyid` — 8 hex chars (`secrets.token_hex(4)`). **Not secret.** Used as the
  storage lookup (so verification is one O(1) `HGETALL`, no scan) and shown
  in listings/logs as `es_<keyid>_…` so a key is identifiable without
  revealing the secret.
- `secret` — `secrets.token_urlsafe(32)` (~256 bits). The actual credential.

Presented as `Authorization: Bearer es_<keyid>_<secret>`.

## Storage — hashed at rest

```
eventstream:apikeys              SET of keyids
eventstream:apikey:<keyid>       HASH {
    hash         sha256(secret) hex
    name         human label (e.g. "billing-svc")
    scopes       comma-joined subset of {read, write, admin}
    created_at   unix
    last_used_at unix (updated on successful verify; best-effort)
}
```

**Never store the raw token** — only `sha256(secret)`. A Redis leak yields
hashes, not usable keys. The raw token is shown **once**, at creation, and is
never retrievable again (we kept only the hash).

**Why SHA-256, not bcrypt/argon2.** Slow hashing defends *low-entropy* human
passwords against brute force. An API secret is ~256 bits of randomness —
brute force is already infeasible, so a fast hash is correct and matters
because we hash on every request. bcrypt here would just make every API call
slow for no security gain.

## Verification — a meander `before` hook

```python
def require(scope: str):
    def hook(request):
        token = _bearer(request.http_headers)          # parse Authorization
        principal = apikeys.verify(token, required=scope)  # raises on failure
        request.content = (request.content or {}) | {"principal": principal}
    return hook
```

`apikeys.verify(token, required)` (in `logic/apikeys.py`, transport-agnostic):

1. Parse `keyid` from the token; `HGETALL` its record → `InvalidToken` (401)
   if missing.
2. Constant-time compare `sha256(secret)` to the stored hash
   (`hmac.compare_digest`) → `InvalidToken` (401) on mismatch.
3. Check `required` scope is in the key's scopes → `InsufficientScope` (403).
4. Touch `last_used_at`; return the key's name as the principal.

Raised `AuthError`s are mapped to HTTP status by the existing server
exception handler (same pattern as `StreamNotFound` → 404):

| Exception          | HTTP |
|--------------------|------|
| `InvalidToken`     | 401  |
| `InsufficientScope`| 403  |

`401` responses include `WWW-Authenticate: Bearer`.

## Scopes

Three coarse scopes; a token carries any subset.

| Scope   | Grants |
|---------|--------|
| `read`  | All `GET /v1/*` inspection (streams, subscriptions, dlq peek, workflows, jobs) |
| `write` | Producer/consumer hot path: publish, pull, ack |
| `admin` | Mutations: create/delete subscriptions, register/delete workflows, dlq drop/purge, job create/advance/cancel/delete |

Per-route required scope is fixed at registration (the `before=require(...)`
on each route). Today only reads exist over HTTP, so only `read` is enforced
in practice; `write`/`admin` come online with the write-API work and are
defined now so that work is a pure wiring step.

Per-stream / per-subscription ACLs are **deferred** — coarse scopes already
express "services produce/consume, ops administer." Richer rules would put a
structured grant in the key record and check it per request.

## CLI — `eventstream key`

Issuance lives with Redis access (the control plane):

```
eventstream key create --name billing-svc --scope write [--scope read]
    → prints the full token ONCE: es_ab12cd34_xPq7…
eventstream key list
    → keyid  name         scopes        created      last-used
      ab12cd34  billing-svc  write,read  2026-06-12…  2026-06-12…
eventstream key revoke <keyid>
    → deletes the record; instant (verify checks Redis every request)
```

No "edit" — **rotate** by creating a new key for the same service, deploying
it, then revoking the old (both valid during the overlap). `create` prints
the token to stdout once; everything else never shows the secret.

## Config

```
EVENTSTREAM_AUTH            "0" (default) | "1"   — enable /v1 token checks
```

When `0`, no `before=require(...)` hooks are attached — behavior is exactly
as today. When `1`, every `/v1/*` route gets its scope guard.

## Module / wiring layout

```
eventstream/logic/apikeys.py     create / list_ / revoke / verify
                                 + AuthError, InvalidToken, InsufficientScope
eventstream/cli/key.py           the `key` command group
eventstream/server/auth.py       the require(scope) before-hook factory + bearer parse
eventstream/server/routes.py     attach require(scope) per route when CONFIG.auth
eventstream/server/__init__.py   add InvalidToken→401, InsufficientScope→403 to _STATUS
```

`verify` and the management functions are async logic, mockable at
`backend.client()` like everything else — unit-tested against fakeredis, with
an integration test for the real round trip.

## Deferred / out of scope

- **HTML admin auth.** Dashboard stays trusted-net; basic-auth/SSO later.
- **Per-stream / per-subscription ACLs.** Coarse scopes for now.
- **Token expiry (`expires_at`).** Easy to add (a field + a freshness check
  in `verify`); not in v1.
- **In-process TLS.** Terminate upstream.
- **Rate limiting / per-key quotas.** Separate concern.
- **mTLS / public-key auth.** Considered and declined for an internal bus
  (client-side signing burden); bearer tokens chosen.

## Open questions

- Should `last_used_at` be best-effort (one extra `HSET` per request) or
  sampled to cut write load on hot keys? Best-effort for v1.
- When `EVENTSTREAM_AUTH=1` with zero keys registered, the API is locked to
  everyone — intended (fail closed), but worth a CLI warning on `server`
  start if auth is on and no keys exist.
