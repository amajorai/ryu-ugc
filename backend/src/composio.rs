//! The **curated platform → Composio action map**: the only way this app fetches
//! post metrics, and the only place a Composio action slug appears in the whole
//! crate.
//!
//! # How a row actually reaches Composio
//!
//! A row's `action` is an action SLUG (`YOUTUBE_VIDEOS_LIST`), and that slug is
//! exactly what [`ryu_composio::execute::dispatch`] takes: it posts
//! `<composio-base>/tools/execute/<SLUG>` to **Composio's own API**, with the API
//! key this app owns, through a no-redirect client so a 3xx cannot bounce the key
//! to another host. There is no Core in the path at all.
//!
//! That is a deliberate correction. The app now sends the bare slug directly to
//! Composio instead of routing through Core's generic `mcp.callTool` capability.
//! The old capability path landed in `monitors_client::host_spider_crawl`, which
//! returns 403 "not the monitors app" for every caller that is not
//! `@ryu/monitors` (`apps/core/src/monitors_client.rs:314`). The declared
//! `tools.invoke` grant passed and the handler then refused, so that hop could
//! never succeed for `@ryu/ugc`.
//!
//! # Whose account do the numbers come from?
//!
//! [`dispatch`](ryu_composio::execute::dispatch) takes a `user_id` selecting the
//! Composio *entity* whose connected accounts are used. This app has no per-user
//! seam — a campaign is the node operator's — so it passes `None`, and the entity
//! resolves from `COMPOSIO_ENTITY_ID`, or `"default"`. That is the pointer to
//! follow when a refresh returns somebody else's numbers.
//!
//! # Why a curated table and not a configurable action
//!
//! `apps-store/dashboards` lets a widget name any Composio action, and pays for it
//! with an id guard, because the value is user config that ends up addressing a
//! privileged surface. A UGC campaign has no reason to name an action at all:
//! "refresh this TikTok post's views" is one fixed lookup per platform. So the
//! slug is a `&'static str` in this table, a campaign never supplies one, and the
//! attack surface dashboards had does not exist here. The guard is kept anyway
//! (defense-in-depth over a constant, asserted by
//! [`tests::every_curated_action_is_a_safe_tool_id_segment`]) so that adding a row
//! can never quietly reintroduce it — and going direct made it MORE load-bearing,
//! not less: the slug is now interpolated into a URL path
//! (`{base}/tools/execute/{slug}`, unencoded), so it is the traversal screen on a
//! request built against Composio's host. See [`checked_action`].
//!
//! # THE ACTION IDS BELOW ARE UNVERIFIED
//!
//! Nothing in this repository pins a Composio action id — the only id in-tree is
//! the illustrative `GITHUB_CREATE_ISSUE` in `crates/core/composio`. Every id and
//! every dotted selector here is therefore a best-effort guess at the shape
//! Composio's v3.1 toolkits use (`TOOLKIT_VERB_OBJECT`, screaming snake case). That
//! is exactly why they live in ONE table with the selectors beside them: correcting
//! a platform is a one-line edit to one row, verified against `GET
//! /api/ugc/platforms`, which serves this table verbatim. Do NOT copy a slug into a
//! second place.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use ryu_composio::execute::{dispatch, ExecOutcome};

/// One platform's metric source: the action to run, how to pass the post id, and
/// where each counter lives in the response.
///
/// `Serialize` because `GET /api/ugc/platforms` returns this table as-is — the
/// operator can see precisely which action id and which selectors are in use, which
/// is what makes an unverified id correctable instead of mysterious.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct PlatformMetricSource {
    /// The lowercase platform key stored on `submissions.platform`.
    pub platform: &'static str,
    /// Human label for the panel's platform picker.
    pub label: &'static str,
    /// The Composio action SLUG, bare — this is the value
    /// [`ryu_composio::execute::dispatch`] takes and puts in the execute URL.
    /// UNVERIFIED — see the module docs.
    pub action: &'static str,
    /// The argument name that carries the post id. The ONLY dynamic value that ever
    /// reaches Composio from this app.
    pub id_arg: &'static str,
    /// Constant arguments the action needs beyond the post id (e.g. a `part` /
    /// `fields` projection). Kept as data so a fix never touches code.
    pub extra_args: &'static [(&'static str, &'static str)],
    /// Dotted selector ("a.b.0.c") for the view count in the response.
    pub views: &'static str,
    /// Dotted selector for the like count.
    pub likes: &'static str,
    /// Dotted selector for the comment count.
    pub comments: &'static str,
    /// Dotted selector for shares/reposts, or `None` when the platform reports none.
    pub shares: Option<&'static str>,
    /// Dotted selector for saves/bookmarks, or `None` when the platform reports none.
    pub saves: Option<&'static str>,
}

/// The five platforms a UGC campaign actually targets.
///
/// Ordered most- to least-used for creator marketing; `GET /api/ugc/platforms`
/// preserves this order, so it is the panel's picker order too.
pub const PLATFORM_METRIC_SOURCES: &[PlatformMetricSource] = &[
    // YouTube reports the counters under `statistics` on the video resource; the
    // `part` projection is what makes the API return that block at all.
    PlatformMetricSource {
        platform: "youtube",
        label: "YouTube",
        action: "YOUTUBE_VIDEOS_LIST",
        id_arg: "id",
        extra_args: &[("part", "statistics")],
        views: "items.0.statistics.viewCount",
        likes: "items.0.statistics.likeCount",
        comments: "items.0.statistics.commentCount",
        shares: None,
        saves: None,
    },
    // TikTok's video query returns per-video stats inline. `saves` is "favourites"
    // upstream, which is the same user action a UGC brief means by it.
    PlatformMetricSource {
        platform: "tiktok",
        label: "TikTok",
        action: "TIKTOK_GET_VIDEO_DETAILS",
        id_arg: "video_id",
        extra_args: &[],
        views: "data.videos.0.view_count",
        likes: "data.videos.0.like_count",
        comments: "data.videos.0.comment_count",
        shares: Some("data.videos.0.share_count"),
        saves: Some("data.videos.0.favourites_count"),
    },
    // Instagram splits reach/impressions (insights) from likes/comments (the media
    // resource). This row reads the media resource and takes `play_count` as the
    // view figure, which is what Reels report; a feed image has none and yields 0.
    PlatformMetricSource {
        platform: "instagram",
        label: "Instagram",
        action: "INSTAGRAM_GET_MEDIA_BY_ID",
        id_arg: "media_id",
        extra_args: &[(
            "fields",
            "id,media_type,play_count,like_count,comments_count",
        )],
        views: "play_count",
        likes: "like_count",
        comments: "comments_count",
        shares: None,
        saves: None,
    },
    // X/Twitter: `public_metrics` on the post lookup. `impression_count` is the
    // closest thing to a view and is what a UGC payout on X is priced against.
    PlatformMetricSource {
        platform: "x",
        label: "X (Twitter)",
        action: "TWITTER_POST_LOOKUP_BY_POST_ID",
        id_arg: "id",
        extra_args: &[("tweet__fields", "public_metrics")],
        views: "data.public_metrics.impression_count",
        likes: "data.public_metrics.like_count",
        comments: "data.public_metrics.reply_count",
        shares: Some("data.public_metrics.retweet_count"),
        saves: Some("data.public_metrics.bookmark_count"),
    },
    // LinkedIn is the least certain row of the five: organic post analytics are not
    // a first-class toolkit action the way the other four are. Expect to correct
    // this one first.
    PlatformMetricSource {
        platform: "linkedin",
        label: "LinkedIn",
        action: "LINKEDIN_GET_POST_ANALYTICS",
        id_arg: "post_urn",
        extra_args: &[],
        views: "elements.0.totalShareStatistics.impressionCount",
        likes: "elements.0.totalShareStatistics.likeCount",
        comments: "elements.0.totalShareStatistics.commentCount",
        shares: Some("elements.0.totalShareStatistics.shareCount"),
        saves: None,
    },
];

/// One reading of a post's counters. `metric_snapshots` rows are built from this.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricSample {
    pub views: i64,
    pub likes: i64,
    pub comments: i64,
    pub shares: i64,
    pub saves: i64,
}

/// What one curated fetch produced. Two outcomes, both of them **successes**.
///
/// [`Self::NeedsConnection`] is the case where the operator has not linked that
/// platform's account to their Composio entity yet. It is not an error and above
/// all not a reading: it carries no [`MetricSample`], so no caller can turn it into
/// a snapshot even by accident — which is the whole point of splitting it out of
/// [`MetricError`]. A zero snapshot from an unlinked account would re-price a live
/// payout down to nothing on the next accrual pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricOutcome {
    /// The counters this platform's row selected out of the action's response.
    Sample(MetricSample),
    /// The account behind this action is not connected. `connect_url` is the link
    /// Composio offered, when it offered one.
    NeedsConnection {
        message: String,
        connect_url: Option<String>,
    },
}

/// Why a curated fetch could not produce an outcome at all.
///
/// Two variants because the API owes the caller two different answers, and they map
/// straight onto `crate::RefreshError`'s own split:
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricError {
    /// A precondition the OPERATOR fixes, not a transient failure: no source for
    /// the platform, no post id to look up, a row whose slug fails
    /// [`checked_action`], or no Composio API key configured for this app at all
    /// ([`key_precondition`]). The caller turns this into a 400 naming the fix.
    Unsupported(String),
    /// The Composio call failed, or answered a shape this row does not describe.
    /// Transient or a wrong row — either way a 502, never a reading.
    Upstream(String),
}

impl std::fmt::Display for MetricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(m) | Self::Upstream(m) => write!(f, "{m}"),
        }
    }
}

/// The curated source for `platform`, or `None` when the platform has none (the
/// submission is then manual-metrics-only, which the API says explicitly rather
/// than silently reporting zeroes).
#[must_use]
pub fn source_for(platform: &str) -> Option<&'static PlatformMetricSource> {
    PLATFORM_METRIC_SOURCES
        .iter()
        .find(|s| s.platform.eq_ignore_ascii_case(platform.trim()))
}

/// True when a Composio API key resolves — the app-persisted one this sidecar
/// applied at boot / on `PUT /api/ugc/settings/composio-key`, or the
/// `RYU_COMPOSIO_API_KEY` / `COMPOSIO_API_KEY` env fallback.
///
/// This is the real answer `GET /api/ugc/platforms` and `GET /api/ugc/settings`
/// report as `composio_configured`; it replaces the old proxy hint (does the node
/// have a Gateway bearer?), which was true of nodes that could not refresh and
/// false of nodes that could. It reads only *whether* a key resolves — never the
/// key.
#[must_use]
pub fn is_configured() -> bool {
    ryu_composio::auth::key().is_some()
}

/// Refuse a refresh when no Composio API key resolves in THIS process, naming the
/// place that actually fixes it.
///
/// Without this the request still leaves [`fetch_with_source`] and
/// [`ryu_composio::execute::dispatch`] refuses it one line in, with its own message:
/// *"Composio API key not set (Settings → Integrations)"*. That message is written
/// for Core, whose desktop Settings → Integrations page writes the key into **Core's**
/// preferences — which this sidecar never reads. Core injects only the ext/shadow/host
/// env into a manifest sidecar, so an operator who follows that pointer configures a
/// key, sees nothing change, and has no way to tell why. The app owns its credential
/// (`PUT /api/ugc/settings/composio-key`), so the app owes the accurate sentence.
///
/// It is also the wrong *class* of failure to report as upstream trouble: nothing was
/// contacted, so a 502 would invite a retry that cannot possibly succeed. Hence
/// [`MetricError::Unsupported`] → 400, the same answer an uncurated platform gets.
///
/// Takes the answer rather than calling [`is_configured`] so both branches are
/// assertable without touching `ryu_composio::auth`'s process-global key cache, which
/// every test in this binary shares and none may mutate safely — the same idiom as
/// `crate::resolve_key_source`.
///
/// # Errors
/// `configured` is false.
pub fn key_precondition(configured: bool) -> Result<(), MetricError> {
    if configured {
        return Ok(());
    }
    Err(MetricError::Unsupported(
        "no Composio API key is configured for this app — add one in the UGC panel's settings \
         (PUT /api/ugc/settings/composio-key) or set RYU_COMPOSIO_API_KEY for this node, and \
         record metrics manually meanwhile. Core's own Settings → Integrations key does not \
         reach this sidecar."
            .to_string(),
    ))
}

/// Return `slug` unchanged if it is safe to interpolate into the execute URL,
/// refusing it otherwise.
///
/// The guard runs BEFORE a request exists because the slug is now a **URL path
/// segment**: `dispatch` builds `format!("{base}/tools/execute/{tool}")` with no
/// percent-encoding, so a slug carrying `/` or `..` would not name a Composio
/// action at all — it would rewrite the path of a request that carries this app's
/// API key. Refusing here keeps that path exactly `/tools/execute/` + one opaque
/// token.
///
/// # Errors
/// The slug is empty, contains `..`, or carries any character outside
/// `[A-Za-z0-9_.-]`. Since every slug is a `&'static str` from
/// [`PLATFORM_METRIC_SOURCES`], this can only fire on a bad new row — which is
/// precisely when it should.
pub fn checked_action(slug: &str) -> Result<&str, MetricError> {
    if id_segment_is_safe(slug) {
        Ok(slug)
    } else {
        Err(MetricError::Unsupported(format!(
            "curated action '{slug}' is not a safe action slug"
        )))
    }
}

/// Is `id` a single opaque identifier — no separators, no dot-segments — and
/// therefore safe to embed somewhere its shape would otherwise be load-bearing?
///
/// Two call sites, both of which need the same answer for different reasons:
///
/// - [`checked_action`] screens a curated slug before it becomes a path segment of
///   a Composio execute URL;
/// - `api::parse_post_id` screens the platform-native post id — the ONE dynamic
///   value this app ever hands to a Composio action — before it is stored.
///
/// Reimplemented here rather than imported: dashboards' copy is `pub(crate)`, and
/// dashboards itself mirrored clips' `clip_id_is_safe` — a per-crate copy IS the
/// house pattern for this guard.
#[must_use]
pub fn id_segment_is_safe(id: &str) -> bool {
    !id.is_empty()
        && !id.contains("..")
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Walk a dotted path ("a.b.0.c") into a JSON value. Each segment is an object key
/// or, if numeric, an array index. Returns null when the path misses — so a
/// platform that stopped reporting a counter yields 0, never an error that blocks
/// the other four counters in the same response.
#[must_use]
pub fn select(value: &Value, selector: &str) -> Value {
    let path = selector.trim();
    if path.is_empty() {
        return value.clone();
    }
    let mut cur = value;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(map) => match map.get(seg) {
                Some(v) => v,
                None => return Value::Null,
            },
            Value::Array(arr) => match seg.parse::<usize>().ok().and_then(|i| arr.get(i)) {
                Some(v) => v,
                None => return Value::Null,
            },
            _ => return Value::Null,
        };
    }
    cur.clone()
}

/// Coerce a selected JSON value into a counter.
///
/// Handles the three shapes these APIs actually return: a JSON number, a
/// **stringified** number (YouTube's `statistics` block does exactly this), and a
/// float. Anything else — null, object, garbage — is 0, because a missing counter
/// must degrade to "no data" and not poison the whole refresh.
#[must_use]
pub fn as_count(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or(0),
        Value::String(s) => s.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

/// Fetch one post's current counters through the curated map.
///
/// The whole Composio surface of this app is this function: pick the row, build its
/// arguments from the ONE dynamic value (`external_post_id`), execute the action
/// against Composio directly, and pull the counters out with the row's own
/// selectors.
///
/// # Errors
/// See [`MetricError`]: no curated source for the platform, an empty post id, an
/// unsafe slug or no configured API key are [`MetricError::Unsupported`]; a failed
/// call or an unrecognisable response is [`MetricError::Upstream`]. A not-connected
/// account is neither — it comes back as [`MetricOutcome::NeedsConnection`].
pub async fn fetch_metrics(
    http: &Client,
    platform: &str,
    external_post_id: &str,
) -> Result<MetricOutcome, MetricError> {
    let src = source_for(platform).ok_or_else(|| {
        MetricError::Unsupported(format!(
            "no Composio metric source is curated for platform '{platform}' — add a row to \
             PLATFORM_METRIC_SOURCES or record metrics manually"
        ))
    })?;
    fetch_with_source(http, src, external_post_id).await
}

/// [`fetch_metrics`] once the row is resolved. Split out so the preconditions that
/// must hold BEFORE anything leaves the process — a safe slug, a non-empty post id
/// and a configured API key — are drivable in a test with a deliberately-bad row,
/// which the `&'static` table cannot express.
async fn fetch_with_source(
    http: &Client,
    src: &PlatformMetricSource,
    external_post_id: &str,
) -> Result<MetricOutcome, MetricError> {
    // Guard the slug first: it becomes a path segment of the URL the API key is
    // sent to, so a bad new row must be refused before a request exists.
    let action = checked_action(src.action)?;
    let post_id = external_post_id.trim();
    if post_id.is_empty() {
        return Err(MetricError::Unsupported(
            "this submission has no external post id to look up".to_string(),
        ));
    }
    // Last, so the two row-shaped refusals above still short-circuit before this
    // reads process-global state — which is what keeps the tests that drive a
    // deliberately-bad row hermetic whatever key the ambient environment supplies.
    key_precondition(is_configured())?;

    // `user_id: None` — this app has no per-user seam, so the Composio entity
    // resolves from `COMPOSIO_ENTITY_ID` / `"default"` (see the module docs).
    let outcome = dispatch(http, action, Value::Object(build_args(src, post_id)), None)
        .await
        .map_err(|e| MetricError::Upstream(e.to_string()))?;

    match outcome {
        // Not an error and not a reading: hand it straight back so the caller can
        // report "connect this account" and write nothing.
        ExecOutcome::NeedsConnection { message, url } => Ok(MetricOutcome::NeedsConnection {
            message,
            connect_url: url,
        }),
        ExecOutcome::Ok(body) => sample_from(src, &body).map(MetricOutcome::Sample),
    }
}

/// The arguments one curated row sends: the post id under the row's own arg name,
/// plus the row's constant projection args.
fn build_args(src: &PlatformMetricSource, post_id: &str) -> serde_json::Map<String, Value> {
    let mut args = serde_json::Map::new();
    args.insert(src.id_arg.to_string(), Value::String(post_id.to_string()));
    for (key, value) in src.extra_args {
        args.insert((*key).to_string(), Value::String((*value).to_string()));
    }
    args
}

/// Read a row's counters out of an action's response, refusing a response that
/// carries none of them.
///
/// ALL of this row's selectors missing means the response is not the shape the row
/// describes — a soft failure that answered 200 with nothing useful (Composio
/// passes a `successful: false` body with no `data` through whole), or an action
/// id/selector set that is simply wrong (the module docs say plainly that every id
/// and selector here is UNVERIFIED). Coercing that to five zeroes would be the
/// worst outcome available: `refresh_submission` would write a `source: composio`
/// snapshot of 0 views, emit `metrics.refreshed` with a negative delta, and the
/// accrual pass would re-price a live payout DOWN to nothing — silently, with the
/// panel reporting a successful refresh. Refusing turns that into a per-submission
/// 502 the operator can read.
///
/// A genuinely-zero counter is unaffected: [`select`] returns `Value::Null` only
/// when the path misses, and a real `0` comes back as a number (or the stringified
/// `"0"` YouTube sends), so a brand-new post with no views yet still refreshes
/// normally. A not-connected account never reaches here either — that is a typed
/// [`MetricOutcome::NeedsConnection`] one level up — so this refusal now means
/// exactly one thing: the row's action or selectors do not match what came back.
fn sample_from(src: &PlatformMetricSource, body: &Value) -> Result<MetricSample, MetricError> {
    let selected: Vec<Value> = [
        Some(src.views),
        Some(src.likes),
        Some(src.comments),
        src.shares,
        src.saves,
    ]
    .into_iter()
    .flatten()
    .map(|selector| select(body, selector))
    .collect();
    if selected.iter().all(Value::is_null) {
        return Err(MetricError::Upstream(format!(
            "the '{}' action answered without any of the counters this platform's row \
             selects (e.g. '{}') — the curated action or its selectors need correcting; \
             record metrics manually meanwhile",
            src.action, src.views
        )));
    }

    Ok(MetricSample {
        views: as_count(&select(body, src.views)),
        likes: as_count(&select(body, src.likes)),
        comments: as_count(&select(body, src.comments)),
        shares: src.shares.map_or(0, |s| as_count(&select(body, s))),
        saves: src.saves.map_or(0, |s| as_count(&select(body, s))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn youtube() -> &'static PlatformMetricSource {
        source_for("youtube").expect("youtube is curated")
    }

    /// The guard must hold for every row, or a new platform ships a slug that
    /// [`checked_action`] will refuse at refresh time — the catalog `GET
    /// /api/ugc/platforms` advertises and the actions that can actually be
    /// executed must agree.
    #[test]
    fn every_curated_action_is_a_safe_tool_id_segment() {
        for src in PLATFORM_METRIC_SOURCES {
            assert!(
                id_segment_is_safe(src.action),
                "'{}' ({}) is not a safe action slug",
                src.action,
                src.platform
            );
            assert!(!src.id_arg.is_empty(), "{} has no id_arg", src.platform);
            assert!(
                !src.views.is_empty(),
                "{} has no views selector",
                src.platform
            );
            assert_eq!(
                src.platform,
                src.platform.to_ascii_lowercase(),
                "platform keys are stored lowercased on submissions.platform"
            );
        }
    }

    #[test]
    fn id_segment_is_safe_rejects_traversal_and_separators() {
        assert!(id_segment_is_safe("YOUTUBE_VIDEOS_LIST"));
        assert!(!id_segment_is_safe(""));
        assert!(!id_segment_is_safe(".."));
        assert!(!id_segment_is_safe("../../api/agents/foo?"));
        assert!(!id_segment_is_safe("a/b"));
        assert!(!id_segment_is_safe("a b"));
    }

    /// The table hands `dispatch` the BARE slug: it is the action id Composio's
    /// own execute endpoint is addressed with, not a Core-qualified tool id.
    #[test]
    fn curated_actions_are_bare_slugs_with_no_mcp_prefix() {
        for src in PLATFORM_METRIC_SOURCES {
            let action = checked_action(src.action).expect("curated slug passes the guard");
            assert_eq!(action, src.action);
            assert!(
                !action.contains('.'),
                "'{action}' is not a bare Composio action slug"
            );
        }
    }

    /// With no key anywhere the operator must be told where THIS app's key lives.
    /// The message `dispatch` would have produced instead points at Core's desktop
    /// Settings → Integrations, which writes Core's preferences — a surface this
    /// sidecar never reads, so following it changes nothing and explains nothing.
    #[test]
    fn key_precondition_names_this_apps_own_settings_not_cores() {
        assert_eq!(key_precondition(true), Ok(()));
        let err = key_precondition(false).unwrap_err();
        assert!(
            matches!(err, MetricError::Unsupported(_)),
            "an unset key is a precondition the operator fixes, not upstream trouble \
             (a 502 would invite a retry that cannot succeed): {err}"
        );
        assert!(
            err.to_string().contains("/api/ugc/settings/composio-key"),
            "{err}"
        );
        assert!(err.to_string().contains("RYU_COMPOSIO_API_KEY"), "{err}");
        // …and it says plainly that the pointer `ryu-composio` would have given is
        // the wrong one for a sidecar.
        assert!(
            err.to_string().contains("does not reach this sidecar"),
            "{err}"
        );
    }

    #[test]
    fn checked_action_refuses_an_unsafe_slug() {
        for bad in ["", "..", "../../api/agents/foo?", "a/b", "a b"] {
            let err = checked_action(bad).unwrap_err();
            assert!(matches!(err, MetricError::Unsupported(_)));
            assert!(err.to_string().contains("not a safe action slug"), "{err}");
        }
    }

    #[test]
    fn source_lookup_is_case_insensitive_and_total() {
        assert_eq!(source_for("YouTube").unwrap().platform, "youtube");
        assert_eq!(source_for("  x  ").unwrap().platform, "x");
        assert!(source_for("myspace").is_none());
    }

    #[test]
    fn as_count_reads_numbers_strings_and_degrades_to_zero() {
        assert_eq!(as_count(&serde_json::json!(41_200)), 41_200);
        // YouTube returns its statistics as STRINGS — the case that would silently
        // report every video as 0 views if this branch were dropped.
        assert_eq!(as_count(&serde_json::json!("41200")), 41_200);
        assert_eq!(as_count(&serde_json::json!(12.9)), 12);
        assert_eq!(as_count(&Value::Null), 0);
        assert_eq!(as_count(&serde_json::json!({})), 0);
    }

    #[test]
    fn select_walks_objects_and_array_indices_and_misses_to_null() {
        let body = serde_json::json!({ "items": [{ "statistics": { "viewCount": "7" } }] });
        assert_eq!(select(&body, "items.0.statistics.viewCount"), "7");
        // A platform that stopped reporting a counter must yield null (→ 0), not
        // block the four counters that did come back in the same response.
        assert_eq!(select(&body, "items.0.statistics.likeCount"), Value::Null);
        assert_eq!(select(&body, "items.9"), Value::Null);
        assert_eq!(select(&body, ""), body);
    }

    #[test]
    fn build_args_carries_the_post_id_under_the_rows_arg_name() {
        let args = build_args(youtube(), "dQw4w9WgXcQ");
        assert_eq!(args["id"], serde_json::json!("dQw4w9WgXcQ"));
        // …plus the row's constant projection, which is what makes the API return
        // the statistics block at all.
        assert_eq!(args["part"], serde_json::json!("statistics"));
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn sample_from_applies_the_rows_selectors() {
        let body = serde_json::json!({
            "items": [{ "statistics": {
                "viewCount": "41200", "likeCount": "812", "commentCount": "45"
            }}]
        });
        let sample = sample_from(youtube(), &body).unwrap();
        assert_eq!(sample.views, 41_200);
        assert_eq!(sample.likes, 812);
        assert_eq!(sample.comments, 45);
        // No shares/saves selector for YouTube ⇒ 0, not an error.
        assert_eq!(sample.shares, 0);
        assert_eq!(sample.saves, 0);
    }

    /// The money-critical case: a soft failure that mentions no connection (rate
    /// limit, bad argument) is NOT elicitation-detected by `dispatch` and NOT a
    /// non-2xx, so it arrives here as an `Ok` body carrying no counters. Reading it
    /// as "0 views" would re-price a live payout down to nothing on the next
    /// accrual pass, with the panel reporting a successful refresh.
    #[test]
    fn sample_from_refuses_a_response_that_carries_none_of_its_counters() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({ "successful": false, "error": "rate limited" }),
        ] {
            let err = sample_from(youtube(), &body).unwrap_err();
            assert!(
                matches!(err, MetricError::Upstream(_)),
                "a shapeless response is upstream trouble, not an unsupported row"
            );
            assert!(
                err.to_string().contains("without any of the counters"),
                "{err}"
            );
        }
    }

    /// …and the guard must not fire on a post that legitimately has no views yet:
    /// `select` distinguishes a missed path (null) from a real zero.
    #[test]
    fn sample_from_accepts_a_genuinely_zero_counter() {
        let body = serde_json::json!({ "items": [{ "statistics": { "viewCount": "0" } }] });
        assert_eq!(sample_from(youtube(), &body).unwrap().views, 0);
    }

    /// Both preconditions must be refused BEFORE anything leaves the process. The
    /// unsafe slug matters most: it would otherwise be interpolated into the path
    /// of a request carrying this app's Composio API key. Driven against a closed
    /// loopback port so a regression that DID dispatch fails loudly (connection
    /// refused) instead of reaching Composio.
    #[tokio::test]
    async fn fetch_refuses_an_unsafe_slug_and_an_empty_post_id_without_dispatching() {
        let http = Client::new();
        let bad = PlatformMetricSource {
            action: "../../api/agents/foo?",
            ..PLATFORM_METRIC_SOURCES[0]
        };
        let err = fetch_with_source(&http, &bad, "dQw4w9WgXcQ")
            .await
            .unwrap_err();
        assert!(matches!(err, MetricError::Unsupported(_)), "{err}");
        assert!(err.to_string().contains("not a safe action slug"), "{err}");

        let err = fetch_metrics(&http, "youtube", "   ").await.unwrap_err();
        assert!(err.to_string().contains("no external post id"), "{err}");
    }

    #[tokio::test]
    async fn fetch_metrics_refuses_a_platform_with_no_curated_source() {
        let err = fetch_metrics(&Client::new(), "myspace", "1")
            .await
            .unwrap_err();
        assert!(matches!(err, MetricError::Unsupported(_)));
        assert!(
            err.to_string().contains("no Composio metric source"),
            "{err}"
        );
    }

    /// The two outcomes are structurally distinct: a `NeedsConnection` carries no
    /// [`MetricSample`], so there is nothing a caller could write as a snapshot
    /// even by mistake.
    #[test]
    fn needs_connection_carries_no_sample() {
        let outcome = MetricOutcome::NeedsConnection {
            message: "No active connection for TikTok".to_string(),
            connect_url: Some("https://composio.dev/connect/abc".to_string()),
        };
        assert!(!matches!(outcome, MetricOutcome::Sample(_)));
    }
}
