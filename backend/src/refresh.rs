//! The sidecar's background metric-refresh loop.
//!
//! One task, one job: every so often, walk the **active** campaigns and hand each
//! one to [`ryu_ugc::UgcEngine::refresh_campaign`]. It owns no logic of its own —
//! the curated Composio map, the snapshot write, the accrual re-pricing and every
//! hook event live in the crate lib, and re-implementing any of them here would
//! fork the emit + accrual path. This module is scheduling, and nothing else.
//!
//! It lives in the **binary**, not the lib: `lib.rs` deliberately exposes the
//! refresh as a request-scoped call (`POST /api/ugc/campaigns/:id/refresh`) so an
//! in-process host can drive it from its own scheduler, exactly as Core owns the
//! `ryu-monitors` tick. Only the standalone sidecar needs a clock of its own.
//!
//! # Three properties that are deliberate, not incidental
//!
//! 1. **The first tick is delayed by a full period.** The manifest marks this
//!    sidecar `lazy: true, idle_stop_secs: 300`, so the process cold-starts every
//!    time the dock panel is reopened and is killed five minutes after it closes.
//!    A `tokio::time::interval` fires *immediately* on its first `tick()`, which
//!    would turn every panel open into a full Composio fan-out across every active
//!    campaign. [`spawn`] therefore starts the clock one period out.
//! 2. **Campaigns are refreshed one at a time, and so are the posts inside one.**
//!    Not a style choice: the accrual pass prices each post against the money
//!    committed *so far*, so a parallel fan-out would price every post against the
//!    same stale total and blow past `budget_cents` by every post but one. The
//!    sequencing lives in `refresh_campaign`; this loop must not "optimise" around
//!    it by running campaigns concurrently, because the per-creator cap spans
//!    campaigns.
//! 3. **A failure never leaves the loop.** `refresh_campaign` is already
//!    best-effort per submission (one platform being down yields an `error` line
//!    in the report, not an aborted batch); anything that still escapes is logged
//!    and the tick ends. The loop is the one thing that must survive a bad night.
//!
//! A submission whose platform account is not linked to the operator's Composio
//! entity yet comes back as `needs_connection` — counted, and logged, apart from
//! the failures. It is not broken, it is unconfigured, and it wrote nothing.

use std::collections::HashMap;
use std::time::Duration;

use ryu_ugc::{CampaignStatus, UgcEngine};

/// Cadence when neither the env nor the prefs file says otherwise. Six hours:
/// view counts on a week-old post move slowly, and every tick costs one Composio
/// call per approved submission — a figure that scales with the campaign, not with
/// the clock.
const DEFAULT_INTERVAL_SECS: u64 = 6 * 60 * 60;

/// Floor on the cadence. A mistyped `5` would otherwise turn the loop into a busy
/// hammer against the Composio hop (and the operator's rate limit) rather than a
/// slightly eager refresh.
const MIN_INTERVAL_SECS: u64 = 300;

/// Env override for the cadence, in seconds. `0` disables the loop entirely, which
/// is the escape hatch for a node that only ever wants the manual
/// `POST /api/ugc/campaigns/:id/refresh`.
const INTERVAL_ENV: &str = "RYU_UGC_REFRESH_SECS";

/// Prefs-file key for the same cadence, so the setting survives without an env
/// edit. The env wins when both are present.
pub const INTERVAL_PREF: &str = "auto-refresh-interval-secs";

/// The resolved schedule for the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshPolicy {
    /// `None` when auto-refresh is off — [`spawn`] then starts no task at all,
    /// rather than starting one that wakes up to do nothing.
    pub interval: Option<Duration>,
}

impl RefreshPolicy {
    /// Resolve the cadence from the env (which wins) then the persisted prefs,
    /// falling back to [`DEFAULT_INTERVAL_SECS`].
    ///
    /// Pure over its inputs so the precedence and the clamp are unit-testable
    /// without mutating process-global env.
    #[must_use]
    pub fn resolve(env_secs: Option<&str>, prefs: &HashMap<String, String>) -> Self {
        let raw = env_secs
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| prefs.get(INTERVAL_PREF).cloned());
        Self {
            interval: interval_from(raw.as_deref()),
        }
    }

    /// The policy read from the real process environment.
    #[must_use]
    pub fn from_env(prefs: &HashMap<String, String>) -> Self {
        Self::resolve(std::env::var(INTERVAL_ENV).ok().as_deref(), prefs)
    }
}

/// Parse a cadence in seconds: `0` (or anything unparseable-but-present that reads
/// as off) disables, everything else is clamped up to [`MIN_INTERVAL_SECS`].
///
/// Garbage is treated as "no opinion" — a typo in a prefs file must not silently
/// switch auto-refresh off, because the symptom (stale view counts) looks like a
/// broken Composio integration rather than a config error.
#[must_use]
fn interval_from(raw: Option<&str>) -> Option<Duration> {
    let secs = match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => match s.parse::<u64>() {
            Ok(v) => v,
            Err(_) => DEFAULT_INTERVAL_SECS,
        },
        None => DEFAULT_INTERVAL_SECS,
    };
    if secs == 0 {
        return None;
    }
    Some(Duration::from_secs(secs.max(MIN_INTERVAL_SECS)))
}

/// Start the refresh loop. Call once, after the store is open.
///
/// A disabled policy spawns nothing. The first tick lands one full period out (see
/// the module docs — an immediate tick would fan out on every lazy cold start).
pub fn spawn(engine: UgcEngine, policy: RefreshPolicy) {
    let Some(period) = policy.interval else {
        tracing::info!(
            "ryu-ugc: auto-refresh is off ({INTERVAL_ENV}=0); metrics refresh only on request"
        );
        return;
    };
    tracing::info!(
        "ryu-ugc: auto-refreshing active campaigns every {}s",
        period.as_secs()
    );
    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + period;
        let mut tick = tokio::time::interval_at(start, period);
        loop {
            tick.tick().await;
            run_once(&engine).await;
        }
    });
}

/// One pass: refresh every active campaign, sequentially, swallowing failures.
///
/// Only `active` campaigns are touched. A `draft` one has nothing to price yet,
/// and a `paused`/`ended` one is precisely the state an operator uses to stop the
/// spend moving — a background loop that kept re-pricing it would make "pause"
/// mean nothing.
///
/// Returns the campaign ids it walked, in order. Not for the caller — [`spawn`]
/// drops it — but for the test that asserts the rule above. Metrics are fetched
/// through Ryu's managed Composio provider, so the host has no credential callback
/// to count calls
/// through, and *which campaigns were touched* is the money-critical fact: a
/// paused campaign that still got walked would keep spending.
async fn run_once(engine: &UgcEngine) -> Vec<String> {
    let campaigns = match engine
        .store
        .list_campaigns(Some(CampaignStatus::Active))
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("ryu-ugc: refresh tick could not list campaigns: {e:#}");
            return Vec::new();
        }
    };
    let mut walked = Vec::with_capacity(campaigns.len());
    for campaign in campaigns {
        walked.push(campaign.id.clone());
        match engine.refresh_campaign(&campaign.id).await {
            Ok(report) => {
                // `refresh_campaign` is best-effort per submission, so a mixed
                // batch is the normal case, not an anomaly — log the split rather
                // than the whole payload. `needs_connection` is reported apart
                // from `error` here for the same reason the API splits them: the
                // fix is "link the account", not "the platform is down".
                let counts = report.counts;
                if counts.needs_connection > 0 || counts.error > 0 {
                    tracing::info!(
                        "ryu-ugc: refreshed campaign '{}' — {} ok, {} awaiting an account \
                         connection, {} failed",
                        campaign.id,
                        counts.ok,
                        counts.needs_connection,
                        counts.error
                    );
                }
            }
            Err(e) => {
                tracing::warn!("ryu-ugc: refreshing campaign '{}' failed: {e}", campaign.id);
            }
        }
    }
    walked
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use ryu_ugc::{
        Campaign, PayoutRule, Submission, SubmissionStatus, UgcHost, UgcStore, DB_FILE_NAME,
    };

    /// The smallest thing that satisfies the marker [`UgcHost`].
    ///
    /// It deliberately does not create or mutate provider credentials: those are
    /// owned by Ryu's Gateway and are outside the refresh loop.
    struct NoKeyHost;

    impl UgcHost for NoKeyHost {}

    fn temp_store() -> UgcStore {
        let dir = std::env::temp_dir().join(format!("ryu-ugc-refresh-{}", uuid::Uuid::new_v4()));
        UgcStore::open(dir.join(DB_FILE_NAME)).expect("open temp store")
    }

    fn campaign(id: &str, status: CampaignStatus) -> Campaign {
        Campaign {
            id: id.into(),
            brand: "Acme".into(),
            brief: "post a clip".into(),
            status,
            platforms: vec!["youtube".into()],
            required_hashtags: vec![],
            required_mentions: vec![],
            starts_at: None,
            ends_at: None,
            budget_cents: 0,
            payout: PayoutRule::Cpm { cpm_cents: 250 },
            bonus_tiers: vec![],
            max_payout_per_creator_cents: 0,
            created_at: ryu_ugc::now_iso(),
            updated_at: ryu_ugc::now_iso(),
        }
    }

    fn submission(id: &str, campaign_id: &str, post_id: &str) -> Submission {
        Submission {
            id: id.into(),
            campaign_id: campaign_id.into(),
            creator_id: "crt1".into(),
            platform: "youtube".into(),
            post_url: format!("https://youtu.be/{post_id}"),
            external_post_id: post_id.into(),
            status: SubmissionStatus::Pending,
            submitted_at: ryu_ugc::now_iso(),
            reviewed_at: None,
            rejection_reason: None,
            created_at: ryu_ugc::now_iso(),
            updated_at: ryu_ugc::now_iso(),
        }
    }

    #[test]
    fn interval_defaults_clamps_and_can_be_switched_off() {
        assert_eq!(
            interval_from(None),
            Some(Duration::from_secs(DEFAULT_INTERVAL_SECS))
        );
        // A mistyped `5` must not become a busy hammer against Composio.
        assert_eq!(
            interval_from(Some("5")),
            Some(Duration::from_secs(MIN_INTERVAL_SECS))
        );
        assert_eq!(interval_from(Some("900")), Some(Duration::from_secs(900)));
        // `0` is the documented off switch.
        assert_eq!(interval_from(Some("0")), None);
        // Garbage reads as "no opinion", NOT as off — silently disabling would
        // look like a broken Composio integration, not a config typo.
        assert_eq!(
            interval_from(Some("soon")),
            Some(Duration::from_secs(DEFAULT_INTERVAL_SECS))
        );
    }

    #[test]
    fn env_beats_prefs_which_beats_the_default() {
        let mut prefs = HashMap::new();
        prefs.insert(INTERVAL_PREF.to_string(), "900".to_string());
        assert_eq!(
            RefreshPolicy::resolve(None, &prefs).interval,
            Some(Duration::from_secs(900))
        );
        assert_eq!(
            RefreshPolicy::resolve(Some("1800"), &prefs).interval,
            Some(Duration::from_secs(1800))
        );
        // An empty env var is "unset", not "0".
        assert_eq!(
            RefreshPolicy::resolve(Some("  "), &prefs).interval,
            Some(Duration::from_secs(900))
        );
        assert_eq!(
            RefreshPolicy::resolve(None, &HashMap::new()).interval,
            Some(Duration::from_secs(DEFAULT_INTERVAL_SECS))
        );
    }

    fn test_engine() -> UgcEngine {
        UgcEngine::new(temp_store(), reqwest::Client::new(), Arc::new(NoKeyHost))
    }

    /// A submission the refresh path can never dispatch: its platform has no
    /// curated Composio row, so `refresh_submission` refuses it before anything
    /// leaves the process. That is what keeps these tick tests hermetic now that
    /// metrics are fetched through the managed Composio provider — a refreshable row would try to
    /// reach the real API.
    fn unrefreshable(id: &str, campaign_id: &str, post_id: &str) -> Submission {
        let mut s = submission(id, campaign_id, post_id);
        s.platform = "myspace".into();
        s
    }

    #[tokio::test]
    async fn a_tick_walks_active_campaigns_only() {
        let engine = test_engine();
        for (id, status) in [
            ("c-active", CampaignStatus::Active),
            ("c-paused", CampaignStatus::Paused),
        ] {
            engine
                .store
                .upsert_campaign(&campaign(id, status))
                .await
                .unwrap();
        }
        for (sub, camp) in [("s1", "c-active"), ("s2", "c-paused")] {
            engine
                .create_submission(&unrefreshable(sub, camp, sub))
                .await
                .unwrap();
            engine.review_submission(sub, true, None).await.unwrap();
        }

        // `pause` has to actually stop the spend moving, or it means nothing —
        // and which campaigns were walked is the only place that shows now that
        // the fetch has no host callback to count.
        assert_eq!(run_once(&engine).await, vec!["c-active".to_string()]);
        // A refusal writes nothing, for either campaign: the batch reports it and
        // moves on rather than leaving a zero reading behind.
        for sub in ["s1", "s2"] {
            assert!(engine.store.latest_snapshot(sub).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn a_tick_survives_a_batch_where_every_submission_fails() {
        let engine = test_engine();
        engine
            .store
            .upsert_campaign(&campaign("c1", CampaignStatus::Active))
            .await
            .unwrap();
        for sub in ["s1", "s2"] {
            engine
                .create_submission(&unrefreshable(sub, "c1", sub))
                .await
                .unwrap();
            engine.review_submission(sub, true, None).await.unwrap();
        }

        // The loop is the one thing that must survive a bad night: every
        // submission failing is a completed tick, not an aborted one.
        assert_eq!(run_once(&engine).await, vec!["c1".to_string()]);
        // And the accrued money is exactly what approval priced it at (0 views,
        // so 0c) — a failed refresh must never re-price anything.
        for sub in ["s1", "s2"] {
            assert_eq!(
                engine
                    .store
                    .payout_for_submission(sub)
                    .await
                    .unwrap()
                    .unwrap()
                    .amount_cents,
                0
            );
        }
    }

    #[tokio::test]
    async fn a_tick_over_an_empty_store_is_a_silent_no_op() {
        let engine = test_engine();
        // The loop must survive a night with nothing to do — and a campaign with
        // no approved submissions is still a walk with nothing in it.
        assert!(run_once(&engine).await.is_empty());
        engine
            .store
            .upsert_campaign(&campaign("c1", CampaignStatus::Active))
            .await
            .unwrap();
        assert_eq!(run_once(&engine).await, vec!["c1".to_string()]);
    }
}
