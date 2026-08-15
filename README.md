<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./icon-dark.png" />
    <img src="./icon-light.png" alt="UGC" width="144" />
  </picture>
</p>

<div align="center">

# UGC

</div>

Run creator-marketing campaigns end to end: briefs and budgets, a creator roster, post submissions with approve/reject review, post metrics refreshed through a curated Composio action map, and CPM/flat-rate payouts accrued, approved and marked paid.

> **The public home of `ryu-ugc`.** Source, builds, and releases live here —
> binaries for every platform are attached to each release.
>
> This tree is generated from the Ryu monorepo, so commits pushed here
> directly are replaced on the next sync. **Pull requests are welcome** —
> open them here and they are ported into the monorepo, then flow back out.
> Ryu as a whole: https://github.com/amajorai/ryu

## Install

**App:** [Install](ryu://apps/@ryu/ugc) (opens the Ryu desktop app and asks you to confirm)

**CLI:**

```bash
ryu apps add @ryu/ugc
```

**Crate:**

```bash
cargo install ryu-ugc
```

Prebuilt binaries for every platform are attached to [each release](https://github.com/amajorai/ryu/releases).

## License

Apache-2.0 — see [LICENSE](./LICENSE).

## How a refresh reaches Composio

**Directly, with no Core hop.** The backend depends on `ryu-composio` (the workspace
crate `crates/core/composio`) and calls
`ryu_composio::execute::dispatch(&http, action, args, None)`, which resolves the API
key, POSTs `{base}/tools/execute/{ACTION}` against **Composio's own host** with an
`x-api-key` header, and hands back a typed `ExecOutcome`. Nothing in that path touches
`apps/core`: the crate's own `Cargo.toml` states it has zero dependency on it, so
taking it keeps this app a self-contained satellite — the path dep resolves in-tree and
the `version` resolves from the registry in the published `amajorai/ryu-ugc` repo. It is
the same dependency idiom `apps-store/quests/backend` already uses for `ryu-app-events`.

Two details of `dispatch` are load-bearing here and should not be re-implemented locally:

- **The tool argument is the BARE action slug** — `YOUTUBE_VIDEOS_LIST`, not
  `composio__YOUTUBE_VIDEOS_LIST`. The `composio__` prefix is only how Core's
  `McpRegistry` *addresses* a Composio action inside its own tool namespace; it is not
  part of the action's name and Composio's execute URL would 404 on it.
- **The base URL is validated and the client follows no redirects.** `dispatch` pins
  https + an allowlisted Composio host before the key is sent, and builds a
  `redirect::Policy::none()` client so a 3xx from the allowlisted host cannot bounce the
  request (carrying `x-api-key`) to an inner host.

`ExecOutcome` has two arms, and the second is **not** a failure:

```rust
pub enum ExecOutcome {
    Ok(Value),                                         // Composio's `data`, unwrapped
    NeedsConnection { message: String, url: Option<String> },
}
```

`NeedsConnection` means the operator has not linked that platform's account yet. It is
reported as its own per-submission status, never as an error and never as a reading:
`RefreshOutcome::NeedsConnection` is *structurally* incapable of carrying a snapshot, so
nothing is written and no payout is re-priced. Inventing zeroes there would silently
re-price a live payout down to nothing on the next accrual pass.

### Refresh wire shape

Both `POST /submissions/:id/refresh` and `POST /campaigns/:id/refresh` report the
identical per-submission line, with every field always present (`null` rather than
omitted) so a consumer can switch on `status` without probing for keys:

```json
{ "submission_id": "sub_…", "status": "ok" | "needs_connection" | "error",
  "message": null, "connect_url": null, "snapshot": null }
```

The campaign-wide route adds counts `{ "ok": n, "needs_connection": n, "error": n }`.
`needs_connection` is counted apart from `error` on purpose: "link your TikTok account"
and "that action id does not exist" are different jobs for whoever reads the response,
and one platform being unlinked never fails the batch.

## The Composio API key is the app's own

Core injects only the ext env, shadow env and host env into a manifest sidecar
(`sidecar/manifest_sidecar.rs`), no Composio key, and `ryu_composio::auth`'s cache is
process-global, so this **separate process** starts with an empty one. The app therefore
owns its key end to end:

- `PUT /api/ugc/settings/composio-key` persists it and applies it immediately via
  `ryu_composio::auth::set_key`. Persist first, apply second: reversed, a failed write
  would leave the running process using, and reporting, an `app` key that is not on
  disk, and the lie would only surface at the next restart.
- On boot the sidecar reads that file back and applies it *before* the engine exists, so
  no refresh can race a still-empty cache.
- With no app key, `ryu_composio::auth::key()` falls back to `RYU_COMPOSIO_API_KEY` then
  `COMPOSIO_API_KEY` — the headless path, which needs no panel visit.
- Precedence is **app key wins**: the cache is consulted before the env, so a key set in
  the panel overrides a stray env var rather than being shadowed by it.

`GET /api/ugc/settings` answers
`{ "composio_configured": bool, "composio_key_source": "env" | "app" | "none" }`, and
`GET /api/ugc/platforms` now carries the same `composio_configured` — a real answer from
`auth::key().is_some()`, not a proxy for whether some gateway token exists.
`DELETE /api/ugc/settings/composio-key` removes the file, clears the cache so the env
fallback can resume, and reports the source **after** the delete: `env` when the
environment still supplies one. Reporting `none` there would tell an operator refreshes
are off when they are not.

**The key is never read back.** No route returns it, no prefix or suffix of it, and no
length. It lives only in `auth`'s cache and transiently in the two functions that touch
the file (owner-only `0600`, created with that mode rather than chmod'ed afterwards, so
it is never briefly on disk under the process umask). `SidecarUgcHost` holds a *path*,
not a key, so no field, `Debug` impl or log line can reach it, and the one error path
that could have formatted it is scrubbed before it is returned.

## Parts

- **`backend/` (`ryu-ugc`)** — the whole app: the SQLite `UgcStore`, the accrual engine,
  the curated Composio map, and the `/api/ugc/*` HTTP surface. Served **out-of-process**
  by the `ryu-ugc` bin (`[[bin]]`, `kind: local`, `public_mount`, `RYU_UGC_BIN` /
  `RYU_UGC_PORT`, default `:8004`); Core links **zero UGC code** (no path-dep, no
  `ugc_client.rs`, no `/api/ugc` route in `server/mod.rs` — the public mount is derived
  from this manifest at router-build time). The one remaining process concern (where
  the API key is persisted) is inverted through the `UgcHost` trait, so the crate has
  **zero dependency on `apps/core`**.
- **No `ui/`.** There is no companion, no `dist/index.html`, and no
  `plugin_manifest/fixtures/ugc.ui.html`. The surface is a **native desktop dock panel**
  registered under the key `@ryu/ugc/ugc` in `NATIVE_DOCK_PANELS`, which fetches
  `/api/ugc/*` directly. A companion frame runs under CSP `connect-src 'none'` and could
  only reach the host through per-app RPC verbs in `packages/app-host` plus rows in
  `crates/core/kernel-contracts` — i.e. exactly the per-app Core coupling `AGENTS.md`
  forbids. `runnables: []`.

## Manifest (`manifest.json`)

- **Sidecar:** `ugc` on `:8004`, `command: "ryu-ugc"`, `command_env: RYU_UGC_BIN`,
  `port_env: RYU_UGC_PORT`, `health_path: /health`, **`lazy: true`** with
  `idle_stop_secs: 300`. Twenty-two declared routes: `/`, `/health`, `/platforms`;
  `/settings` + `/settings/composio-key`; `/campaigns` + `:id` + per-campaign
  `summary`/`leaderboard`/`submissions`/`refresh`; `/creators` + `:id`; `/submissions` +
  `:id` + per-submission `review`/`metrics`/`refresh`; `/payouts` + `:id` +
  `approve`/`paid`.
- **Grants: none.** `permission_grants: []`, and the sidecar declares no `host_api`
  block at all. See [No host grants](#no-host-grants).
- **Levels:** `ugc.view` · `ugc.manage` (implies view) · `ugc.payouts` (implies view).
  Payout settlement is its own level because marking a payout paid **freezes its
  amount**: a paid row is never re-priced, so the act is the campaign's spend record,
  not a status change.
- **Contributes:** three blocks, all of them consumed. The `ugc` dock panel
  (`panel: "native"`, both docks), the `surface:ugc` list-detail view, and six hook
  events. Core serves all three from `GET /api/plugins/contributions`.

  Two blocks were declared here and have been **removed**, because neither could ever
  take effect and a declaration nothing honours is a claim the product does not keep:

  - `data_categories.campaigns` — Core resolves a category id through the closed table
    `DataCategory::from_id` (`server/data_admin.rs`), which knows
    `chats|spaces|memory|monitors|meetings` and nothing else. An unknown id is skipped
    with a warn, so the Settings danger-zone row never rendered. Backing it would take a
    new Core variant reaching into this app — the per-app Core coupling `AGENTS.md`
    forbids. Deleting a campaign is `DELETE /api/ugc/campaigns/:id`, which cascades in
    Rust.
  - `quotas.maxCampaigns` — a manifest quota only bites if the billing catalog carries
    the key *and* a client guards on it. `maxCampaigns` appears in neither
    (`packages/auth/src/lib/plans.ts` has no such row, and nothing calls
    `guard("maxCampaigns", …)`), so the cap was inert in both halves. `maxMonitors` is
    the counter-example worth copying if a cap is ever wanted: catalog row with
    `owner: "@ryu/monitors"`, plus a `guard()` at the create site.

### `http.routes` is enforcing, not advisory

`resolve_route` walks this list and 404s a path no row matches — the request never
reaches the sidecar. So the list must be exhaustive and must spell `:id` exactly as the
axum router does. That is why adding the settings surface meant adding **two** rows:
`/settings` and `/settings/composio-key` are separate patterns, and a nested path is not
covered by its parent. `"/"` is required on its own row: `public_mount_routes` registers
the bare mount and the `/*rest` wildcard as two separate axum routes, so without it a
bare `GET /api/ugc` 404s. The trailing-slash form `/api/ugc/` is deliberately never
registered (axum panics on it) and will not work.

## Why `lazy: true` — and the scheduler that is not here

Reasonable to expect the opposite: an app that refreshes post metrics sounds like it
wants a background loop, and `apps-store/dashboards` says in its own description that it
is started **eagerly** precisely so its refresh loop can run before any desktop opens.

This app is not that, as specified. Every refresh has an HTTP caller
(`POST /campaigns/:id/refresh` and `POST /submissions/:id/refresh`), and the schema has no
`next_run_at`, no queue, and no scheduler table: nothing in it creates work with no
request behind it. `idle_stop_secs` is the field that would kill an in-process timer, so
the two move together, and both point the same way here. The posture is browser's and
simulator's: an opt-in app nobody opens costs a boot nothing, and the first ext-proxy hit
from the dock panel wakes it behind a bounded health-wait.

**If** a nightly refresh sweep is added later, this is the line that has to change —
drop `lazy` and `idle_stop_secs` together and say why in the description, as dashboards
does. Do not keep `idle_stop_secs` and add a timer: the process would be reaped
mid-window and the sweep would silently stop happening.

## Auth / security

The sidecar binds **loopback only** and fail-closes: every `/api/ugc/*` route requires a
bearer resolved as `RYU_EXT_TOKEN` (Core's per-plugin secret, re-stamped on each proxied
hop). With no token configured, every protected route rejects 401. `GET /health` is the
one un-gated route, so Core's pre-auth probe succeeds before the token is presented. The
settings routes are protected like every other one — the key can only be written by a
caller that already reached the app through Core's proxy.

Composio actions are `&'static str`s from the curated table — a campaign cannot name
one. `checked_action` screens each slug before it becomes a URL path segment, because
`dispatch` interpolates it into `{base}/tools/execute/{tool}` with no percent-encoding: a
slug carrying `/` or `..` would not name an action, it would rewrite the path of a
request that carries this app's API key. The same guard screens the platform-native post
id parsed out of `submissions.post_url` — the ONE dynamic value this app ever hands to a
Composio action.

## No host grants

This app declares **no** `permission_grants` and **no** `sidecars[].host_api` block,
because there is nothing left for either to authorize. A grant declared and never
exercised is a permission the user is asked to approve for nothing.

The one Core capability the backend still uses is `events.emit`, and its row in
`KERNEL_CAPABILITIES` (`sidecar/ext_proxy.rs`) carries `grant: None` — so
`host_capability` skips the grant intersection entirely for it and dispatches straight
to `host_events_emit`. That row is not "unguarded": it is authorized by **ownership**,
which is tighter than any grant could be. `may_emit_event` requires the authenticated
caller to be the plugin the event id is namespaced to *and* to have declared that exact
id in its own `contributes.hook_events`. The widest possible abuse of a stolen grant
would be "an app emits its own events", which is the entire intended use.

`SidecarSpec::host_api`'s own doc says "Absent = the sidecar may not call back into Core
at all (deny-all)", which reads like a contradiction and is worth not re-litigating: that
sentence describes the **grant-gated** `/api/host/*` surface, which is the only thing
`host_api_grant_usable` is consulted for. A `grant: None` kernel capability never reaches
that check. `apps-store/quests` is the standing proof — it ships `ryu-app-events` and
declares no `host_api` either.

That ownership check is also why the six ids below and the `EventEmitter::emit`
arguments must be **byte-identical**: one character off and Core 403s, which `emit()`
logs and swallows, so it fails silently.

`tools.invoke` used to be declared here, for a `mcp.callTool` callback that has been
deleted along with it — see [Known limits](#known-limits) for why that seam is
unusable by any app but `@ryu/monitors`.

Not declared, on purpose: `sidecar:process`. A Core-tier built-in's sidecar spawns on
the unconditional auto-run path, and the Gateway *validates and denies* that grant at
enable — declaring it would make the enable itself fail.

## Hook events

Past tense, named for what happened. All six are raised by the backend (`lib.rs`
`EVENT_*` constants; none is declared-but-never-emitted). Each description states whether
it can re-fire for the same subject, because that is the only thing a consumer cannot
infer:

| id | re-fires? |
| --- | --- |
| `@ryu/ugc#submission.received` | once per row created; a duplicate post is a 409 |
| `@ryu/ugc#submission.approved` | transition-gated (pending → approved) |
| `@ryu/ugc#submission.rejected` | transition-gated |
| `@ryu/ugc#metrics.refreshed` | **every** successful refresh, unchanged counters included |
| `@ryu/ugc#payout.accrued` | only when `amount_cents` changes; paid rows never emit |
| `@ryu/ugc#campaign.budget.reached` | once on the crossing; re-arms if the budget is raised |

A `needs_connection` refresh emits nothing — `metrics.refreshed` fires only on the
branch that actually wrote a snapshot, so an unlinked account never looks like a reading
of zero to a downstream workflow.

## Permission levels are vocabulary, not a gate

The three levels are **reachable** — `GET /api/acl/vocabulary` merges every installed
app's `permission_levels` into one flat namespace, which is how a grant picker learns
`ugc.payouts` exists and that it implies `ugc.view`. They are not **enforced**: a route is
gated only when its `http.routes[]` row carries a `permission`, absent by default, and
none of the twenty-two rows here carries one. So today every level is grantable and
describes intent honestly, while the proxy forwards all twenty-two routes to any caller
who reaches the mount. Annotating them is the follow-up; it is a behaviour change, not a
doc fix — and `/settings/composio-key` is the row to annotate first when it happens.

## Swap seam

The platform map is data: one row per platform carrying the action id, the id argument,
constant extra args, and the dotted selectors. Correcting a platform (including
swapping the whole Composio hop for a direct API) is a one-line edit to one row plus,
at most, the single `composio::fetch_metrics` call site. `GET /api/ugc/platforms` serves
the table verbatim, so which action id and which selectors are live is inspectable from
the panel rather than buried in a constant.

## Known limits

**The curated action map is unverified against a live Composio account.** No row has been
confirmed end-to-end: not one action id has been checked to exist under that exact name,
and not one set of dotted selectors has been checked against a real response body. The
map is a considered guess at five APIs, and Composio renames actions and reshapes payloads
without notice. Treat a fresh install as "wired, not proven" — the refresh path is
correct, the five rows in it are not yet evidence.

The failure is loud rather than silent, which is the point of the design: an action id
that does not exist comes back as a per-submission `error` string, and a selector that
matches nothing yields no snapshot rather than a zero. One platform being wrong never
fails the batch or corrupts an accrual — a submission with no snapshot simply does not
re-price, and `@ryu/ugc#metrics.refreshed` does not fire for it.

**To correct a row**, a maintainer edits that one row of `PLATFORM_METRIC_SOURCES` in the
backend's `composio` module — its action id (the bare slug, no `composio__` prefix), the
argument that carries the post id, any constant extra args, and the dotted selector per
counter — then re-reads `GET /api/ugc/platforms`, which serves the table verbatim, and
runs `POST /api/ugc/submissions/:id/refresh` against one known-good post on that platform
with a key configured. A snapshot appearing with plausible counters is the confirmation;
`status: "error"` names what to fix, and `status: "needs_connection"` means the row may
well be right and the account simply is not linked yet. No other file changes, and
`linkedin` is the row to expect trouble from first.

**`mcp.callTool` and `notify.fanout` are pinned to `@ryu/monitors` in Core**, so no other
app can use them today. Their `KERNEL_CAPABILITIES` rows name no app (the *routing* is
generic), but both dispatch into `apps/core/src/monitors_client.rs`, whose handlers
re-run `authenticate_sidecar` and then return
`403 {"error":"not the monitors app"}` for any other caller —
`host_spider_crawl` at `apps/core/src/monitors_client.rs:314` and `host_monitor_alert` at
`apps/core/src/monitors_client.rs:367`. No change on this side can satisfy a check keyed on our own plugin id (that id must be
`@ryu/ugc` for the token comparison to pass at all), and relaxing the pin is an
`apps/core` edit an apps-store app must not make. **That is why this app takes
`ryu-composio` directly**: the crate needs no Core at all, and the app is not blocked
waiting on a Core change it cannot make. `events.emit` is the counter-example that made
fan-out workable: no pin, authorized by ownership of the event id.
