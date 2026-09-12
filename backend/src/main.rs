//! `ryu-ugc` — the standalone, out-of-process UGC campaign-tracker sidecar.
//!
//! Runs the `ryu_ugc` crate (the SQLite [`UgcStore`] + the [`UgcEngine`] + the
//! `/api/ugc/*` surface, defined in `lib.rs` / `api.rs`) as a SEPARATE PROCESS that
//! Core spawns, health-checks, and proxies to on loopback — exactly like
//! `ryu-quests` / `ryu-monitors`. The store, engine and handlers live in the crate
//! lib; this binary is only the process shell around them, so the SAME crate still
//! compiles into Core in-process as a path dependency (no code is duplicated).
//!
//! [`ryu_ugc::routes`] returns a state-baked, state-less `Router<()>` whose paths
//! are RELATIVE to `/api/ugc` (the manifest's `http.mount` / `public_mount`). This
//! binary nests it under that same prefix, so the generic ext-proxy forwards
//! `/api/ugc/*` unchanged and the desktop dock panel reaches the sidecar with no
//! per-app Core coupling at all.
//!
//! SECURITY: loopback-only bind (127.0.0.1) + a shared-secret bearer gate
//! (`RYU_EXT_TOKEN`, injected by Core at spawn and presented on the health probe +
//! every proxied hop). EVERY `/api/ugc/*` route is protected. The gate is
//! FAIL-CLOSED: with no token configured every protected route rejects with 401.
//! Health is the ONE un-gated surface (loopback probe, returns no campaign data),
//! so Core's pre-auth readiness check succeeds — and it is registered at BOTH
//! `/health` (the manifest's `health_path`, which Core probes directly) and
//! `/api/ugc/health` (the DECLARED proxy route: `proxy_for_plugin` forwards
//! `<mount><sub_path>` verbatim, so a proxied probe arrives with the prefix still
//! on it). `routes()` deliberately contains neither — health must answer before the
//! gate, so it cannot live inside the nest.
//!
//! Port: `RYU_UGC_PORT` env, default `8004`. Data dir: resolved via the inlined
//! `paths::ryu_dir` (`RYU_DIR`-env-first, injected by Core at spawn), so it opens
//! the SAME `ugc.db` the node uses.
//!
//! Provider calls go through Ryu's managed provider bridge. Core authenticates this
//! sidecar and Gateway supplies the Composio credential from its provider config;
//! the UGC process never stores or receives that key.
//!
//! Fan-out is `events.emit` via `ryu-app-events`, already wired inside
//! [`UgcEngine`]: unlike `notify.fanout` it is authorized by *ownership* of the
//! event id, so it works for every app.
//!
//! Preferences are a JSON map under `RYU_DIR`, written atomically (temp file +
//! rename). They are module-level functions rather than host-trait methods on
//! purpose: the one thing they carry — the auto-refresh cadence read by
//! [`refresh`] — is a process concern, not part of the crate's contract.

mod paths;
mod refresh;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use ryu_ugc::{routes, UgcCtx, UgcEngine, UgcHost, UgcStore, DB_FILE_NAME};

/// Default loopback port for the UGC sidecar (overridable via `RYU_UGC_PORT`).
/// 8004 is free — 8001 healing, 8002 learning, 8003 monitors and 7990-7999 (the
/// wave-1..3 apps) are taken. Kept identical in `manifest.json`.
const DEFAULT_PORT: u16 = 8004;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("RYU_UGC_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    // Shared-secret bearer Core injects via the generic ext-proxy loader
    // (`RYU_EXT_TOKEN`) — the per-plugin minted secret it stamps on every proxied
    // hop + the health probe. The protected `/api/ugc/*` routes require it.
    let token = std::env::var("RYU_EXT_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    if token.is_some() {
        tracing::info!(
            "ryu-ugc: protected /api/ugc/* routes require the injected shared-secret bearer"
        );
    } else {
        tracing::warn!(
            "ryu-ugc: no RYU_EXT_TOKEN set; protected /api/ugc/* routes are FAIL-CLOSED (reject all). Core injects this token when it spawns the sidecar."
        );
    }

    let dir = paths::ryu_dir();
    let store = UgcStore::open(dir.join(DB_FILE_NAME))?;

    let prefs_path = dir.join("ugc-prefs.json");

    let host: Arc<dyn UgcHost> = Arc::new(SidecarUgcHost);
    let engine = UgcEngine::new(store.clone(), reqwest::Client::new(), host);

    // Publish the process-global engine for parity with `ryu-quests` /
    // `ryu-monitors`. Its Core-side readers do not run in the sidecar, so it is an
    // inert-but-harmless consumer; the HTTP handlers use the state-baked `UgcCtx`
    // below, not `global_engine()`.
    ryu_ugc::set_global_engine(engine.clone());

    // The background metric refresh. Spawned after the store is open so a tick can
    // never race the schema; disabled entirely when the cadence resolves to 0.
    refresh::spawn(
        engine.clone(),
        refresh::RefreshPolicy::from_env(&load_prefs(&prefs_path)),
    );

    // The crate router (paths relative to `/api/ugc`) nested under the external
    // prefix, with the shared-secret gate layered over the whole nest — UGC has no
    // public route. `from_fn` closes over the resolved token so no extra state
    // field is needed.
    let gated_token = token.clone();
    //
    // `/openapi.json` rides INSIDE that same gate, at the SERVER ROOT. Core fetches
    // `http://127.0.0.1:<port>/openapi.json` on this sidecar's first Healthy edge and
    // lowers every operation it finds into searchable LLM tools, so routing this one
    // endpoint is what makes the whole `/api/ugc` surface callable by an agent
    // (`ryu_ugc::api::openapi()` was dead code until now — only tests read it).
    //
    // Root, not under `/api/ugc`: Core tries the root FIRST and only falls back to the
    // mount-prefixed form, and keeping the document off the mount keeps it out of the
    // manifest's declared `http.routes[]` — anything declared there is reachable
    // through the generic ext-proxy, and the schema is Core's to read, not an app
    // surface. Inside the gate, not next to the un-gated health probes: Core stamps
    // the injected `RYU_EXT_TOKEN` on the fetch (the Python sidecars already require
    // the bearer for everything but `/health`), so the gate costs the fetcher nothing
    // — while un-gated it would disclose this app's entire internal API surface to any
    // other process on loopback.
    let ugc = Router::new()
        .nest("/api/ugc", routes(UgcCtx::new(engine)))
        .route(
            "/openapi.json",
            get(|| async { axum::Json(ryu_ugc::api::openapi()) }),
        )
        .layer(from_fn(move |req: Request, next: Next| {
            let expected = gated_token.clone();
            async move { require_ugc_token(req, next, expected.as_deref()).await }
        }));

    // Health sits OUTSIDE the gated nest so the loopback probe succeeds before
    // auth, at both the paths a probe can arrive on (see the module docs). No axum
    // conflict: `routes()` registers neither.
    let probe_store = store.clone();
    let proxied_probe_store = store;
    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let store = probe_store.clone();
                async move { ryu_ugc::api::health(store).await }
            }),
        )
        .route(
            "/api/ugc/health",
            get(move || {
                let store = proxied_probe_store.clone();
                async move { ryu_ugc::api::health(store).await }
            }),
        )
        .merge(ugc);

    // LOOPBACK ONLY (belt) + shared-secret bearer (suspenders): Core is the auth
    // front and re-stamps the bearer on the proxied hop.
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ryu-ugc sidecar listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Shared-secret bearer gate for the proxied `/api/ugc/*` surface. Core stays the
/// auth front — it runs `require_auth`, then re-stamps `Authorization: Bearer
/// <RYU_EXT_TOKEN>` on the loopback hop — so a request that did NOT come through
/// Core (any other local process on a shared host) is rejected with 401.
///
/// **Fail-closed:** `expected == None`/empty (no token configured) rejects every
/// request rather than falling open.
async fn require_ugc_token(req: Request, next: Next, expected: Option<&str>) -> Response {
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if bearer_ok(provided, expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

/// Pure bearer check (factored out so the auth decision is unit-testable without an
/// axum `Request`/`Next`). Returns `true` only when `expected` is a non-empty token
/// AND `provided` equals it (constant-time compared). A `None`/empty `expected` is
/// the fail-closed case → always `false`.
fn bearer_ok(provided: Option<&str>, expected: Option<&str>) -> bool {
    ryu_sidecar_runtime::token_ok(provided, expected)
}

// ─────────────────────────────────────────────────────────────────────────────
// Preferences (process-local, atomically persisted)
// ─────────────────────────────────────────────────────────────────────────────

/// Read the persisted preference map (empty on a missing/corrupt file — a fresh
/// install just falls back to defaults).
fn load_prefs(path: &PathBuf) -> HashMap<String, String> {
    let Ok(bytes) = std::fs::read(path) else {
        return HashMap::new();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

/// Persist the preference map atomically (write a temp file, then rename) so a
/// crash mid-write cannot corrupt the live config file.
///
/// Currently exercised only by its round-trip test: nothing sets a preference at
/// runtime yet, because the cadence is read once at startup and the API surface has
/// no prefs route. The writer lives beside the reader anyway — the atomic contract
/// is the reason `load_prefs` can treat a truncated file as "fresh install" rather
/// than as data loss, and splitting the two would let that drift.
#[allow(dead_code)]
fn save_prefs(path: &PathBuf, map: &HashMap<String, String>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let bytes = serde_json::to_vec_pretty(map).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())
}

// ─────────────────────────────────────────────────────────────────────────────
// The host shim
// ─────────────────────────────────────────────────────────────────────────────

/// The sidecar host marker. Provider credentials are held by Gateway, not by this
/// process or its data directory.
struct SidecarUgcHost;

impl UgcHost for SidecarUgcHost {}

#[cfg(test)]
mod tests {
    use super::{bearer_ok, load_prefs, save_prefs};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn tmp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ryu-ugc-host-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ))
    }

    fn tmp_prefs_path() -> PathBuf {
        tmp_dir().join("ugc-prefs.json")
    }

    #[test]
    fn bearer_ok_matches_only_exact_nonempty_token() {
        assert!(bearer_ok(Some("secret"), Some("secret")));
        assert!(!bearer_ok(Some("secret"), Some("other")));
        assert!(!bearer_ok(Some("secre"), Some("secret")));
        assert!(!bearer_ok(None, Some("secret")));
    }

    #[test]
    fn bearer_ok_is_fail_closed_without_expected() {
        // No/empty configured token → reject everything, even a matching-looking
        // header.
        assert!(!bearer_ok(Some("secret"), None));
        assert!(!bearer_ok(Some(""), Some("")));
        assert!(!bearer_ok(None, None));
    }

    #[test]
    fn ct_eq_compares_content_not_prefix() {
        assert!(ryu_sidecar_runtime::constant_time_eq(b"abc", b"abc"));
        assert!(!ryu_sidecar_runtime::constant_time_eq(b"abc", b"abd"));
        // A shared prefix must not pass — length is checked, and the loop never
        // returns early on the first mismatched byte.
        assert!(!ryu_sidecar_runtime::constant_time_eq(b"abc", b"ab"));
        assert!(ryu_sidecar_runtime::constant_time_eq(b"", b""));
    }

    #[test]
    fn prefs_roundtrip_atomically_and_leave_no_tmp() {
        let path = tmp_prefs_path();
        // Missing file → empty map (fresh install falls back to defaults).
        assert!(load_prefs(&path).is_empty());

        let mut map = HashMap::new();
        map.insert("auto-refresh-interval-secs".to_string(), "900".to_string());
        save_prefs(&path, &map).expect("save prefs");

        // The atomic rename must leave only the final file, not the `.tmp` sibling.
        assert!(path.exists());
        assert!(!path.with_extension("json.tmp").exists());
        assert_eq!(
            load_prefs(&path)
                .get("auto-refresh-interval-secs")
                .map(String::as_str),
            Some("900")
        );

        // A corrupt file degrades to defaults rather than panicking.
        std::fs::write(&path, b"{ this is not json").unwrap();
        assert!(load_prefs(&path).is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
