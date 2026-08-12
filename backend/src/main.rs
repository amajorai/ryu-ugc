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
//! COMPOSIO, AND THE KEY THIS PROCESS OWNS. Metric refreshes go **straight to
//! Composio** through `ryu-composio`'s `execute::dispatch` (see
//! [`ryu_ugc::composio`]). No Core hop is involved, and that is not a preference:
//! the two kernel capabilities that could have carried one are pinned to a single
//! app. `mcp.callTool` is handled by `monitors_client::host_spider_crawl`
//! (`apps/core/src/monitors_client.rs:314`) and `notify.fanout` by
//! `monitors_client::host_monitor_alert` (`apps/core/src/monitors_client.rs:367`);
//! both answer 403 "not the monitors app" to every caller that is not
//! `@ryu/monitors`, *after* the declared grant has already passed. Neither is
//! called from this binary any more — a call that provably cannot succeed must not
//! sit in the code as if it might.
//!
//! Going direct means the app must own the credential, because Core injects only
//! the ext / shadow / host env into a manifest sidecar
//! (`apps/core/src/sidecar/manifest_sidecar.rs`) — never a Composio key. So:
//!
//! - the key is persisted at `<RYU_DIR>/ugc-composio-key`, written atomically
//!   (unique temp file, created **0600**, then renamed) and never logged;
//! - it is applied to [`ryu_composio::auth`]'s in-process cache at boot and on
//!   `PUT /api/ugc/settings/composio-key`;
//! - with no app key, the crate's own `RYU_COMPOSIO_API_KEY` / `COMPOSIO_API_KEY`
//!   env fallback is left to do its job, which is what
//!   [`ryu_ugc::ComposioKeySource::Env`] reports;
//! - the [`UgcHost`] shim exposes only *which source is active* — the key itself
//!   never leaves this file, is never held in a struct field, and never appears in
//!   an error string that a handler could put in an HTTP body.
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
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use ryu_ugc::{
    resolve_key_source, routes, ComposioKeySource, UgcCtx, UgcEngine, UgcHost, UgcStore,
    DB_FILE_NAME,
};

/// Default loopback port for the UGC sidecar (overridable via `RYU_UGC_PORT`).
/// 8004 is free — 8001 healing, 8002 learning, 8003 monitors and 7990-7999 (the
/// wave-1..3 apps) are taken. Kept identical in `manifest.json`.
const DEFAULT_PORT: u16 = 8004;

/// The app-owned Composio API key file, under `RYU_DIR`. A single raw line, not
/// JSON: there is no config here to grow, and a plain line removes the one code
/// path (a parse/serialize error) that could ever format the key into a message.
const COMPOSIO_KEY_FILE_NAME: &str = "ugc-composio-key";

/// Owner-only permissions for that file. Set explicitly at CREATE time rather than
/// chmod'ed afterwards, so the key is never briefly on disk under the process
/// umask.
#[cfg(unix)]
const KEY_FILE_MODE: u32 = 0o600;

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

    // Apply the app-owned Composio key BEFORE the engine exists, so the first
    // scheduled refresh cannot race a still-empty key cache. With no persisted key
    // this is a no-op and `ryu-composio`'s own env fallback resolves instead.
    let key_path = dir.join(COMPOSIO_KEY_FILE_NAME);
    apply_persisted_composio_key(&key_path);

    let host: Arc<dyn UgcHost> = Arc::new(SidecarUgcHost::new(key_path));
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
    let ugc = Router::new()
        .nest("/api/ugc", routes(UgcCtx::new(engine)))
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
    let Some(expected) = expected.filter(|t| !t.is_empty()) else {
        return false;
    };
    ct_eq(provided.unwrap_or("").as_bytes(), expected.as_bytes())
}

/// Constant-time byte comparison — no early return on the first mismatched byte, so
/// the token check does not leak length/prefix via timing.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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
// The app-owned Composio API key
// ─────────────────────────────────────────────────────────────────────────────

/// Apply the persisted key to `ryu-composio`'s in-process cache, if there is one.
///
/// Called once at boot. With no file, nothing is set and the crate's own
/// `RYU_COMPOSIO_API_KEY` / `COMPOSIO_API_KEY` fallback resolves instead — which is
/// why this deliberately does NOT call `set_key("")` in the empty case: clearing
/// would be indistinguishable from setting nothing, but it says something the file
/// does not.
///
/// The log line reports only that a key was applied. The value is never logged, at
/// any level.
fn apply_persisted_composio_key(path: &Path) {
    match read_app_key(path) {
        Some(key) => {
            ryu_composio::auth::set_key(&key);
            tracing::info!("ryu-ugc: applied the app-persisted Composio API key");
        }
        None => {
            tracing::debug!(
                "ryu-ugc: no app-persisted Composio API key; falling back to the environment"
            );
        }
    }
}

/// Read the persisted key, or `None` when the file is absent, unreadable or blank.
///
/// A blank file reads as "no key" rather than as an empty key, so a truncated write
/// degrades to the env fallback instead of disabling Composio with a key that is
/// technically set.
fn read_app_key(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let key = raw.trim();
    if key.is_empty() {
        None
    } else {
        Some(key.to_string())
    }
}

/// Persist `key` atomically with owner-only permissions.
///
/// Two properties this must not lose:
///
/// 1. **The temp file is created 0600**, not chmod'ed afterwards — otherwise the
///    key sits on disk under the process umask for as long as the write takes, and
///    `rename` would then just preserve whatever that window allowed. The temp name
///    is unique per call (pid + uuid) so a crashed predecessor's leftover can never
///    collide with, or be mistaken for, this one.
/// 2. **No error message contains the key.** Every error here is built from the io
///    error and the path; the caller puts these strings in an HTTP body.
///
/// # Errors
/// The key is blank, or the directory/file/rename operations fail.
fn write_app_key(path: &Path, key: &str) -> Result<(), String> {
    let key = key.trim();
    if key.is_empty() {
        return Err("the Composio API key must not be empty".to_string());
    }
    let dir = path
        .parent()
        .ok_or_else(|| "the key path has no parent directory".to_string())?;
    std::fs::create_dir_all(dir).map_err(|e| format!("could not create {}: {e}", dir.display()))?;

    let tmp = dir.join(format!(
        "{COMPOSIO_KEY_FILE_NAME}.{}.{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let write = (|| -> std::io::Result<()> {
        use std::io::Write as _;
        let mut file = create_private(&tmp)?;
        file.write_all(key.as_bytes())?;
        file.sync_all()
    })();
    if let Err(e) = write {
        // Never leave a partially-written key behind.
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("could not write {}: {e}", tmp.display()));
    }
    // `rename` preserves the mode the temp file was created with.
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("could not replace {}: {e}", path.display())
    })
}

/// Create a new file that only its owner can read. On unix the mode is part of the
/// `open` call; elsewhere the platform default applies (Windows has no umask race
/// to lose to, and no mode to set here).
fn create_private(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(KEY_FILE_MODE);
    }
    opts.open(path)
}

/// Forget the persisted key. A key that was not there is not an error — DELETE is
/// idempotent.
///
/// # Errors
/// The removal failing for any reason other than the file being absent.
fn remove_app_key(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("could not remove {}: {e}", path.display())),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The host shim
// ─────────────────────────────────────────────────────────────────────────────

/// The sidecar's standalone [`UgcHost`]: where this process persists the app's
/// Composio API key.
///
/// That is the whole seam now. It used to also carry Gateway coordinates and a
/// `composio_execute` callback into Core's `mcp.callTool`; the callback could never
/// succeed (pinned to `@ryu/monitors` — see the module docs) and the Gateway was
/// never in the Composio path at all, so both are gone and metric fetches dispatch
/// to Composio directly.
///
/// **It holds a path, not a key.** The value lives only in
/// `ryu_composio::auth`'s cache and transiently in the two functions that touch the
/// file, so no field, `Debug` impl or log line can reach it.
struct SidecarUgcHost {
    key_path: PathBuf,
}

impl SidecarUgcHost {
    fn new(key_path: PathBuf) -> Self {
        Self { key_path }
    }
}

impl UgcHost for SidecarUgcHost {
    fn set_composio_key(&self, key: &str) -> Result<ComposioKeySource, String> {
        // Persist FIRST, apply second. Reversed, a failed write would leave this
        // process using — and reporting — an `app` key that is not on disk, and the
        // lie would only surface at the next restart.
        write_app_key(&self.key_path, key)?;
        ryu_composio::auth::set_key(key.trim());
        Ok(ComposioKeySource::App)
    }

    fn clear_composio_key(&self) -> Result<ComposioKeySource, String> {
        remove_app_key(&self.key_path)?;
        // Clearing the cache is what lets the env fallback resume; without it the
        // deleted key would keep working until the process restarted.
        ryu_composio::auth::set_key("");
        Ok(resolve_key_source(false, ryu_composio::auth::key().is_some()))
    }

    fn composio_key_source(&self) -> ComposioKeySource {
        // Read the file each time rather than caching: this answers a settings
        // request, so it is cold, and not holding the key is worth more than the
        // read. `key().is_some()` is a boolean — the value is dropped immediately.
        resolve_key_source(
            read_app_key(&self.key_path).is_some(),
            ryu_composio::auth::key().is_some(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bearer_ok, ct_eq, load_prefs, read_app_key, remove_app_key, save_prefs, write_app_key,
        SidecarUgcHost, COMPOSIO_KEY_FILE_NAME,
    };
    use ryu_ugc::{ComposioKeySource, UgcHost};
    use std::collections::HashMap;
    use std::path::PathBuf;

    /// A key that must never appear in a log line, an error string or a response.
    /// Used as the tripwire in the assertions below.
    const SECRET: &str = "comp_live_do_not_leak_me";

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
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        // A shared prefix must not pass — length is checked, and the loop never
        // returns early on the first mismatched byte.
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(ct_eq(b"", b""));
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
            load_prefs(&path).get("auto-refresh-interval-secs").map(String::as_str),
            Some("900")
        );

        // A corrupt file degrades to defaults rather than panicking.
        std::fs::write(&path, b"{ this is not json").unwrap();
        assert!(load_prefs(&path).is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// The key file round-trips, replaces cleanly, and DELETE is idempotent.
    #[test]
    fn key_file_roundtrips_and_leaves_no_temp_behind() {
        let dir = tmp_dir();
        let path = dir.join(COMPOSIO_KEY_FILE_NAME);
        assert!(read_app_key(&path).is_none(), "a missing file is 'no key'");

        write_app_key(&path, &format!("  {SECRET}  ")).expect("write the key");
        // Stored trimmed, read back verbatim.
        assert_eq!(read_app_key(&path).as_deref(), Some(SECRET));

        // Replacing overwrites in place, and no temp file survives either write —
        // a leftover temp would be a second copy of the key on disk.
        write_app_key(&path, "comp_live_second").expect("replace the key");
        assert_eq!(read_app_key(&path).as_deref(), Some("comp_live_second"));
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files left behind: {leftovers:?}");

        remove_app_key(&path).expect("remove the key");
        assert!(read_app_key(&path).is_none());
        // Idempotent: removing what is not there is not an error.
        remove_app_key(&path).expect("removing twice is fine");

        // A blank file reads as "no key", so a truncated write degrades to the env
        // fallback instead of disabling Composio with a key that is technically set.
        std::fs::write(&path, b"   \n").unwrap();
        assert!(read_app_key(&path).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The key lands owner-only, and it lands that way from the moment it is
    /// CREATED — a chmod after the write would leave it umask-readable in between.
    #[cfg(unix)]
    #[test]
    fn key_file_is_owner_only_from_creation() {
        use super::create_private;
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tmp_dir();
        let path = dir.join(COMPOSIO_KEY_FILE_NAME);
        write_app_key(&path, SECRET).expect("write the key");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "key file mode is {mode:o}, expected 600");

        // The temp file the rename came from is created with the same mode, which
        // is the half of the property `rename` alone cannot give.
        std::fs::create_dir_all(&dir).unwrap();
        let tmp = dir.join("mode-probe.tmp");
        drop(create_private(&tmp).unwrap());
        let tmp_mode = std::fs::metadata(&tmp).unwrap().permissions().mode() & 0o777;
        assert_eq!(tmp_mode, 0o600, "temp file mode is {tmp_mode:o}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Nothing that can be returned to a caller — or logged — may quote the key.
    /// Both failure paths are checked: the blank-key refusal and a write that
    /// cannot create its directory (the parent is a FILE, not a directory).
    #[test]
    fn no_error_string_ever_contains_the_key() {
        let dir = tmp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let blocker = dir.join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();

        let err = write_app_key(&blocker.join(COMPOSIO_KEY_FILE_NAME), SECRET).unwrap_err();
        assert!(!err.contains(SECRET), "the key leaked into: {err}");

        let err = write_app_key(&dir.join(COMPOSIO_KEY_FILE_NAME), "   ").unwrap_err();
        assert!(err.contains("must not be empty"), "{err}");
        assert!(!err.contains(SECRET), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A blank key is refused BEFORE anything is written or applied — an empty
    /// `set_key` would clear the cache and silently disable a working env key.
    #[test]
    fn host_refuses_a_blank_key_without_writing_or_applying_it() {
        let dir = tmp_dir();
        let path = dir.join(COMPOSIO_KEY_FILE_NAME);
        let host = SidecarUgcHost::new(path.clone());
        let err = host.set_composio_key("   ").unwrap_err();
        assert!(err.contains("must not be empty"), "{err}");
        assert!(!path.exists(), "a refused key must not reach the disk");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The source resolver reports `app` from the file's presence alone.
    ///
    /// Only this branch is asserted here on purpose: `env` / `none` depend on
    /// `ryu_composio::auth`'s process-global cache and the ambient
    /// `COMPOSIO_API_KEY` env, which this binary's other tests share and none may
    /// mutate safely. The pure split lives in the lib's `resolve_key_source` test.
    #[test]
    fn host_reports_the_app_source_when_a_key_file_exists() {
        let dir = tmp_dir();
        let path = dir.join(COMPOSIO_KEY_FILE_NAME);
        let host = SidecarUgcHost::new(path.clone());
        write_app_key(&path, SECRET).expect("write the key");
        assert_eq!(host.composio_key_source(), ComposioKeySource::App);

        // …and stops reporting `app` once the file is gone. (What it reports
        // instead — `env` or `none` — is whatever the environment supplies.)
        remove_app_key(&path).unwrap();
        assert_ne!(host.composio_key_source(), ComposioKeySource::App);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
