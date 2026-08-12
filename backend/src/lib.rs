//! UGC: the creator-marketing campaign tracker.
//!
//! A **campaign** is a brief a brand pays creators to post against. Creators on
//! the roster submit posts; a reviewer approves or rejects each one; approved
//! posts have their view/like/comment counters refreshed through a curated
//! platform → Composio action map; and every refresh re-prices what the campaign
//! owes that creator. The whole app answers one question an operator otherwise
//! keeps in a spreadsheet: **what do I owe, and how much budget is left?**
//!
//! This crate owns the store ([`UgcStore`]), the runtime ([`UgcEngine`]), the
//! curated Composio map ([`composio`]) and the `/api/ugc/*` HTTP surface
//! ([`api`]). It has **ZERO dependency on `apps/core`** — the rule every
//! apps-store backend follows (`QuestsHost`, `MonitorsHost`, `DashboardsHost`),
//! and what lets `apps-store/ugc` ship as a self-contained satellite repo.
//!
//! # Composio is reached DIRECTLY, and the key is this app's own
//!
//! A metric refresh calls [`ryu_composio::execute::dispatch`] — a workspace crate
//! that is itself Core-free — which posts the action to Composio's own API. No
//! Core hop, no Gateway hop. Which means the app must own the credential: the
//! sidecar persists a Composio API key under `RYU_DIR` and applies it to
//! [`ryu_composio::auth`] at boot, because Core injects no Composio key into a
//! manifest sidecar's environment. Where that file lives is a *process* concern,
//! so it — and only it — is inverted through the [`UgcHost`] trait.
//!
//! **Two Core kernel capabilities are unusable by this app, permanently.**
//! `mcp.callTool` and `notify.fanout` are routed generically by
//! `sidecar/ext_proxy.rs` but handled by `monitors_client::host_spider_crawl`
//! (`apps/core/src/monitors_client.rs:314`) and
//! `monitors_client::host_monitor_alert` (`apps/core/src/monitors_client.rs:367`),
//! both of which 403 "not the monitors app" for every caller that is not
//! `@ryu/monitors`. The declared grant passes and the handler then refuses. That
//! is not something a manifest, an argument or a retry can fix from this side, so
//! neither is called anywhere in this crate — do not re-attempt it.
//!
//! # Two invariants the rest of the code is built around
//!
//! 1. **Money is integer cents, always.** Nothing on the payout path is `f64`.
//!    The one division (`views * cpm_cents / 1000`) is `i64` and floors on
//!    purpose — see [`payout_for`].
//! 2. **Accrual is idempotent re-pricing, not appending.** A submission has at
//!    most one payout row (`idx_payouts_submission`), and the accrual pass
//!    rewrites its `amount_cents` in place as views grow. Appending instead would
//!    double a campaign's spend on the second refresh.
//!
//! # Fan-out
//!
//! Other plugins and workflows react to this app through its declared
//! `contributes.hook_events`, raised with [`ryu_app_events::EventEmitter`]. That
//! is deliberately **not** `notify.fanout`: Core pins `notify.fanout` to
//! `@ryu/monitors` and 403s every other caller, whereas `events.emit` is
//! authorized by *ownership* — Core re-checks that the caller is the plugin the
//! event id is namespaced to and that the id appears in that plugin's own
//! manifest. Which is why the six ids below are `const`s used at the emit sites:
//! one character of drift from `manifest.json` and every emit is silently
//! rejected.

pub mod api;
pub mod composio;
mod store;

use std::sync::Arc;

use serde::Serialize;

pub use api::{routes, UgcCtx};
// The domain types live in `store.rs` rather than here (it had to ship compiling
// and tested before this file existed), but every downstream `use crate::Campaign`
// should read like monitors' `use crate::Monitor` — hence the flat re-export.
pub use store::{
    clamp_limit, clamp_payout, payout_for, AccrualInputs, BonusTier, Campaign, CampaignStatus,
    CampaignSummary, Creator, CreatorTotals, LeaderboardRow, MetricSnapshot, MetricSource, Payout,
    PayoutFilter, PayoutRule, PayoutStatus, Submission, SubmissionCounts, SubmissionFilter,
    SubmissionStatus, SubmissionWithMetrics, UgcEvent, UgcOverview, UgcStore, WriteOutcome,
    DB_FILE_NAME,
};

/// This app's plugin id. MUST stay byte-identical to `manifest.json`'s `id`:
/// Core re-checks on every emit that the event's namespace half is the plugin the
/// authenticated caller actually is, so a drift here turns every emit into a
/// silent 403.
pub const UGC_PLUGIN_ID: &str = "@ryu/ugc";

/// A creator's post was recorded — once per `submissions` row actually created.
/// A duplicate post is a 409 and never reaches an emit.
pub const EVENT_SUBMISSION_RECEIVED: &str = "@ryu/ugc#submission.received";
/// Transition-gated: once per `pending -> approved`. Re-approving emits nothing.
pub const EVENT_SUBMISSION_APPROVED: &str = "@ryu/ugc#submission.approved";
/// Transition-gated: once per move into `rejected`. Re-rejecting emits nothing.
pub const EVENT_SUBMISSION_REJECTED: &str = "@ryu/ugc#submission.rejected";
/// NOT transition-gated: every successful Composio refresh, even one where the
/// counters did not move. The payload carries the delta so a consumer can gate
/// itself.
pub const EVENT_METRICS_REFRESHED: &str = "@ryu/ugc#metrics.refreshed";
/// A payout row's `amount_cents` changed (created, or re-priced as views grew).
/// A refresh that leaves the amount equal emits nothing.
pub const EVENT_PAYOUT_ACCRUED: &str = "@ryu/ugc#payout.accrued";
/// Transition-gated: once, when committed money first crosses `budget_cents`.
/// Re-arms only if the budget is raised back above the committed total.
pub const EVENT_CAMPAIGN_BUDGET_REACHED: &str = "@ryu/ugc#campaign.budget.reached";

// ─────────────────────────────────────────────────────────────────────────────
// The host seam
// ─────────────────────────────────────────────────────────────────────────────

/// Which source backs the Composio API key that is currently active.
///
/// Reported by `GET /api/ugc/settings` (and by the two writes) so the panel can
/// say something true about the credential **without ever handling it**. There is
/// deliberately no variant, field or method here that carries the key, a prefix of
/// it, or its length: a key that is never formatted cannot leak into a log line, an
/// error body or a JSON response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ComposioKeySource {
    /// The key this app persisted itself (`PUT /api/ugc/settings/composio-key`).
    App,
    /// No app key; `RYU_COMPOSIO_API_KEY` / `COMPOSIO_API_KEY` is what resolves.
    Env,
    /// No key at all — refreshes will fail until one is set.
    None,
}

impl ComposioKeySource {
    /// The wire value: `"app"` / `"env"` / `"none"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::App => "app",
            Self::Env => "env",
            Self::None => "none",
        }
    }

    /// Whether a key resolves at all — the `composio_configured` flag.
    #[must_use]
    pub fn is_configured(self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Decide which source is active, given whether the app has a persisted key and
/// whether [`ryu_composio::auth::key`] resolves anything.
///
/// Pure so both branches are assertable without touching the process-global key
/// cache (which no test may mutate safely — it is shared by every test in the
/// binary). The `app_persisted` case wins because this process only ever populates
/// that cache from its own persisted key: nothing else calls
/// [`ryu_composio::auth::set_key`] in a sidecar, so "a key resolves and we did not
/// persist one" can only mean the environment supplied it.
#[must_use]
pub fn resolve_key_source(app_persisted: bool, any_key_resolves: bool) -> ComposioKeySource {
    if app_persisted {
        ComposioKeySource::App
    } else if any_key_resolves {
        ComposioKeySource::Env
    } else {
        ComposioKeySource::None
    }
}

/// The one thing `ryu-ugc` still needs from whatever process is hosting it:
/// **where the app's Composio API key is persisted**.
///
/// It used to carry the Gateway's URL/bearer and a `composio_execute` callback.
/// All three are gone: the Gateway was never in the Composio path, and the callback
/// posted to Core's `mcp.callTool`, which 403s every caller but `@ryu/monitors`
/// (`apps/core/src/monitors_client.rs:314`) — a call that provably could not
/// succeed. [`composio::fetch_metrics`] now dispatches to Composio directly, so the
/// only cross-cutting thing left is a *file path*, which depends on the host
/// process (`RYU_DIR` in the sidecar, a temp dir in a test).
///
/// The methods are sync: they touch one small file, and making them async would buy
/// nothing but a `#[async_trait]` on every implementor.
///
/// **The key never crosses this trait outbound.** [`Self::set_composio_key`] takes
/// one; nothing returns one, and implementations must keep it out of their error
/// strings — the caller puts those in an HTTP body.
pub trait UgcHost: Send + Sync {
    /// Persist `key` as the app-owned Composio credential and make it active for
    /// this process.
    ///
    /// Implementations persist FIRST and apply second: an applied-but-unpersisted
    /// key would make the app report `app` until the next restart, when it would
    /// silently become `env`/`none`.
    ///
    /// # Errors
    /// An empty/whitespace key (the caller 400s), or the write failing. The message
    /// must never contain the key.
    fn set_composio_key(&self, key: &str) -> Result<ComposioKeySource, String>;

    /// Forget the app-owned key and stop using it.
    ///
    /// Returns the source that is active *afterwards* — [`ComposioKeySource::Env`]
    /// when the environment still supplies one, which the API reports honestly
    /// rather than claiming the app is now unconfigured.
    ///
    /// # Errors
    /// The removal failing. A key that was not there is not an error.
    fn clear_composio_key(&self) -> Result<ComposioKeySource, String>;

    /// Which source backs the active key right now. Never reveals it.
    fn composio_key_source(&self) -> ComposioKeySource;
}

/// Process-global engine, set once at startup. Kept for parity with
/// `ryu-quests` / `ryu-monitors`, whose state-free schedulers read it when a job
/// fires. Nothing in the sidecar reads it — the HTTP handlers use the
/// state-baked [`UgcCtx`] — so out-of-process it is inert but harmless.
static ENGINE: std::sync::OnceLock<UgcEngine> = std::sync::OnceLock::new();

/// Publish the global engine. Idempotent: a second call is ignored.
pub fn set_global_engine(engine: UgcEngine) {
    let _ = ENGINE.set(engine);
}

/// The global engine, if it has been published.
#[must_use]
pub fn global_engine() -> Option<&'static UgcEngine> {
    ENGINE.get()
}

// ─────────────────────────────────────────────────────────────────────────────
// Engine outcomes
// ─────────────────────────────────────────────────────────────────────────────

/// What one accrual pass did to a submission's payout row.
#[derive(Debug, Clone)]
pub struct AccrualOutcome {
    /// The row after the pass. `None` when the submission has not cleared review
    /// and never had a row — unreviewed work must not eat the budget.
    pub payout: Option<Payout>,
    /// What the row was worth before the pass (0 when it did not exist).
    pub previous_cents: i64,
    /// Whether `amount_cents` actually moved. The `payout.accrued` event is gated
    /// on this, so a nightly refresh of a finished campaign is silent.
    pub changed: bool,
}

/// What a review decision did.
#[derive(Debug, Clone)]
pub struct ReviewOutcome {
    pub submission: Submission,
    /// The accrued row an approval created or re-priced.
    pub payout: Option<Payout>,
    /// `false` when the submission was already in the requested state — a 200
    /// no-op that emits nothing, so a double-click never doubles a consumer's
    /// work.
    pub changed: bool,
}

/// What a metrics refresh produced. Both variants are successes.
///
/// An enum rather than a struct with an optional snapshot because
/// [`Self::NeedsConnection`] must be **structurally incapable** of carrying a
/// reading: the operator has not linked that platform's account yet, so there are
/// no counters, and inventing zeroes would re-price a live payout down to nothing
/// on the next accrual pass. The store is only touched in the [`Self::Refreshed`]
/// branch of [`UgcEngine::apply_metric_outcome`].
#[derive(Debug, Clone)]
pub enum RefreshOutcome {
    /// Counters were read and written.
    Refreshed {
        snapshot: MetricSnapshot,
        /// The snapshot this one replaces, for the delta a consumer gates on.
        previous: Option<MetricSnapshot>,
        payout: Option<Payout>,
    },
    /// Composio needs the account connected first. Nothing was written.
    NeedsConnection {
        message: String,
        /// The link Composio offered, when it offered one.
        connect_url: Option<String>,
    },
}

/// The three answers a refresh can give about ONE submission, as the API reports
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshStatus {
    /// Counters were read and a snapshot written.
    Ok,
    /// The account behind this platform is not linked yet. No snapshot, no
    /// re-pricing — deliberately NOT an error, because there is nothing broken.
    NeedsConnection,
    /// Anything else: an unsupported row, or upstream trouble.
    Error,
}

/// One submission's line in a refresh response — the single-submission and
/// campaign-wide endpoints report the identical shape.
///
/// Every field is always present (`message`/`connect_url`/`snapshot` serialize as
/// `null` rather than being skipped) so a consumer can switch on `status` without
/// probing for keys.
#[derive(Debug, Clone, Serialize)]
pub struct SubmissionRefreshReport {
    pub submission_id: String,
    pub status: RefreshStatus,
    pub message: Option<String>,
    pub connect_url: Option<String>,
    pub snapshot: Option<MetricSnapshot>,
}

impl SubmissionRefreshReport {
    /// The report for a refresh that produced an outcome.
    #[must_use]
    pub fn from_outcome(submission_id: &str, outcome: RefreshOutcome) -> Self {
        match outcome {
            RefreshOutcome::Refreshed { snapshot, .. } => Self {
                submission_id: submission_id.to_string(),
                status: RefreshStatus::Ok,
                message: None,
                connect_url: None,
                snapshot: Some(snapshot),
            },
            RefreshOutcome::NeedsConnection {
                message,
                connect_url,
            } => Self {
                submission_id: submission_id.to_string(),
                status: RefreshStatus::NeedsConnection,
                message: Some(message),
                connect_url,
                // Never a snapshot on this branch — see `RefreshOutcome`.
                snapshot: None,
            },
        }
    }

    /// The report for a refresh that failed.
    #[must_use]
    pub fn from_error(submission_id: &str, error: &RefreshError) -> Self {
        Self {
            submission_id: submission_id.to_string(),
            status: RefreshStatus::Error,
            message: Some(error.to_string()),
            connect_url: None,
            snapshot: None,
        }
    }
}

/// The campaign-level split. `needs_connection` is counted apart from `error` so
/// the panel can say "link these accounts" instead of "3 failed".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RefreshCounts {
    pub ok: usize,
    pub needs_connection: usize,
    pub error: usize,
}

impl RefreshCounts {
    /// Tally a batch of reports.
    #[must_use]
    pub fn of(reports: &[SubmissionRefreshReport]) -> Self {
        let mut counts = Self::default();
        for report in reports {
            match report.status {
                RefreshStatus::Ok => counts.ok += 1,
                RefreshStatus::NeedsConnection => counts.needs_connection += 1,
                RefreshStatus::Error => counts.error += 1,
            }
        }
        counts
    }
}

/// What refreshing a whole campaign did: one line per approved submission, plus
/// the split. Serialized verbatim by the campaign refresh endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct CampaignRefreshReport {
    pub results: Vec<SubmissionRefreshReport>,
    pub counts: RefreshCounts,
}

/// Why a refresh could not happen. The variants exist because the API owes the
/// caller three genuinely different answers, and collapsing them into one string
/// would make "add this row to the curated map" indistinguishable from "TikTok is
/// down".
#[derive(Debug, Clone)]
pub enum RefreshError {
    /// No such submission — 404.
    NotFound,
    /// A precondition the operator fixes before this can refresh: its platform has
    /// no curated Composio source, its URL could not be parsed into a post id, or
    /// no Composio API key is configured for this app at all
    /// ([`composio::key_precondition`]) — 400, with the message naming the fix and
    /// telling the user to record metrics by hand meanwhile.
    Unsupported(String),
    /// The Composio call failed, or answered a shape the curated row does not
    /// describe. Surfaced verbatim as a 502 body. NOT the not-connected case —
    /// that is [`RefreshOutcome::NeedsConnection`], a success.
    Upstream(String),
    /// The store failed — 500.
    Internal(String),
}

impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "submission not found"),
            Self::Unsupported(m) | Self::Upstream(m) | Self::Internal(m) => write!(f, "{m}"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The engine
// ─────────────────────────────────────────────────────────────────────────────

/// The UGC runtime: the store, the inverted [`UgcHost`], an HTTP client and the
/// app-event emitter. Cheap to clone.
#[derive(Clone)]
pub struct UgcEngine {
    pub store: UgcStore,
    host: Arc<dyn UgcHost>,
    /// The client every curated Composio action is dispatched on. Held because
    /// [`ryu_composio::execute::dispatch`] takes one — it builds its own
    /// no-redirect client for the request that carries the API key and falls back
    /// to this only if that build fails.
    http: reqwest::Client,
    /// Raises the app events the writes below produce. NOT a constructor
    /// parameter: every call site would build the identical thing, and an emitter
    /// is inert when the process is not Core-hosted — so reading the environment
    /// here keeps the emit path out of every caller's signature (and keeps this
    /// crate's own tests hermetic, since no `RYU_CORE_PORT` means no emit).
    events: ryu_app_events::EventEmitter,
}

impl UgcEngine {
    #[must_use]
    pub fn new(store: UgcStore, http: reqwest::Client, host: Arc<dyn UgcHost>) -> Self {
        Self {
            store,
            host,
            // Reuses the caller's client for both the emits and the Composio
            // dispatch: an emit is one more loopback POST, and a second connection
            // pool would buy nothing.
            events: ryu_app_events::EventEmitter::with_client(UGC_PLUGIN_ID, http.clone()),
            http,
        }
    }

    /// The inverted host, so the API surface can read and write the app's Composio
    /// key without knowing where this process persists it.
    #[must_use]
    pub fn host(&self) -> &Arc<dyn UgcHost> {
        &self.host
    }

    // ---- submissions ------------------------------------------------------

    /// Record a submission and raise `submission.received`.
    ///
    /// A duplicate `(campaign_id, platform, external_post_id)` comes back as
    /// [`WriteOutcome::DuplicatePost`] and emits nothing — that unique index is
    /// the only thing stopping one post being paid twice.
    pub async fn create_submission(&self, s: &Submission) -> Result<WriteOutcome, String> {
        let outcome = self
            .store
            .insert_submission(s)
            .await
            .map_err(|e| e.to_string())?;
        if outcome == WriteOutcome::Written {
            self.events
                .emit(
                    EVENT_SUBMISSION_RECEIVED,
                    serde_json::json!({
                        "submission_id": s.id,
                        "campaign_id": s.campaign_id,
                        "creator_id": s.creator_id,
                        "platform": s.platform,
                        "post_url": s.post_url,
                        // Empty when the URL could not be parsed: the submission
                        // is reviewable by hand, it just cannot auto-refresh.
                        "external_post_id": s.external_post_id,
                        "status": s.status.as_str(),
                        "submitted_at": s.submitted_at,
                    }),
                )
                .await;
        }
        Ok(outcome)
    }

    /// Approve or reject a submission.
    ///
    /// **Transition-gated.** Deciding a submission that is already in the
    /// requested state is a no-op that emits nothing. Approving stamps
    /// `reviewed_at` and runs the accrual pass (creating the payout row);
    /// rejecting stamps the reason and removes any *unpaid* payout — un-accruing
    /// money is fine, un-paying it is not.
    ///
    /// # Errors
    /// `Ok(None)` when there is no such submission (the caller 404s). `Err` when
    /// the transition itself is refused — rejecting an already-paid submission
    /// would leave money owed against a post the brand disowned.
    pub async fn review_submission(
        &self,
        id: &str,
        approve: bool,
        reason: Option<String>,
    ) -> Result<Option<ReviewOutcome>, String> {
        let Some(current) = self.store.get_submission(id).await.map_err(|e| e.to_string())? else {
            return Ok(None);
        };

        let already = if approve {
            // `paid` is past `approved`; re-approving it is equally a no-op.
            matches!(
                current.status,
                SubmissionStatus::Approved | SubmissionStatus::Paid
            )
        } else {
            matches!(current.status, SubmissionStatus::Rejected)
        };
        if already {
            let payout = self
                .store
                .payout_for_submission(id)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(Some(ReviewOutcome {
                submission: current,
                payout,
                changed: false,
            }));
        }
        if !approve && current.status == SubmissionStatus::Paid {
            return Err("this submission has already been paid and cannot be rejected".to_string());
        }

        let now = now_iso();
        let target = if approve {
            SubmissionStatus::Approved
        } else {
            SubmissionStatus::Rejected
        };
        let reason = reason.map(|r| r.trim().to_string()).filter(|r| !r.is_empty());
        let Some(updated) = self
            .store
            .set_submission_status(
                id,
                target,
                Some(&now),
                if approve { None } else { reason.as_deref() },
                &now,
            )
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };

        if approve {
            let accrual = self.accrue_submission(id).await?;
            let payout = accrual.and_then(|a| a.payout);
            self.events
                .emit(
                    EVENT_SUBMISSION_APPROVED,
                    serde_json::json!({
                        "submission_id": updated.id,
                        "campaign_id": updated.campaign_id,
                        "creator_id": updated.creator_id,
                        "platform": updated.platform,
                        "post_url": updated.post_url,
                        "payout_id": payout.as_ref().map(|p| p.id.clone()),
                        "amount_cents": payout.as_ref().map_or(0, |p| p.amount_cents),
                        "reviewed_at": now,
                    }),
                )
                .await;
            return Ok(Some(ReviewOutcome {
                submission: updated,
                payout,
                changed: true,
            }));
        }

        self.store
            .delete_unpaid_payout_for_submission(id)
            .await
            .map_err(|e| e.to_string())?;
        self.events
            .emit(
                EVENT_SUBMISSION_REJECTED,
                serde_json::json!({
                    "submission_id": updated.id,
                    "campaign_id": updated.campaign_id,
                    "creator_id": updated.creator_id,
                    "platform": updated.platform,
                    "reason": reason,
                    "reviewed_at": now,
                }),
            )
            .await;
        Ok(Some(ReviewOutcome {
            submission: updated,
            payout: None,
            changed: true,
        }))
    }

    // ---- metrics ----------------------------------------------------------

    /// Refresh one submission's counters through the curated Composio map,
    /// append the snapshot and re-run its accrual.
    ///
    /// Two shapes of success: [`RefreshOutcome::Refreshed`] wrote a snapshot and
    /// re-priced the payout, and [`RefreshOutcome::NeedsConnection`] wrote nothing
    /// because the operator has not linked that platform's account to their
    /// Composio entity yet. The second is NOT an error — reporting it as one would
    /// invite the caller to treat it like upstream trouble, and treating it like a
    /// reading would re-price a live payout to zero.
    ///
    /// # Errors
    /// See [`RefreshError`] — the three failure modes are genuinely different
    /// answers (400 / 502 / 404) and the API maps them accordingly.
    pub async fn refresh_submission(&self, id: &str) -> Result<RefreshOutcome, RefreshError> {
        let submission = self
            .store
            .get_submission(id)
            .await
            .map_err(|e| RefreshError::Internal(e.to_string()))?
            .ok_or(RefreshError::NotFound)?;

        // Both preconditions are permanent facts about this row, not transient
        // upstream trouble, so they are 400s that tell the user what to do
        // instead: record the metrics by hand.
        if composio::source_for(&submission.platform).is_none() {
            return Err(RefreshError::Unsupported(format!(
                "no Composio metric source is curated for platform '{}' — record metrics manually",
                submission.platform
            )));
        }
        if submission.external_post_id.trim().is_empty() {
            return Err(RefreshError::Unsupported(
                "this submission's post url could not be parsed into a post id — record metrics \
                 manually"
                    .to_string(),
            ));
        }

        let previous = self
            .store
            .latest_snapshot(id)
            .await
            .map_err(|e| RefreshError::Internal(e.to_string()))?;

        let outcome = composio::fetch_metrics(
            &self.http,
            &submission.platform,
            &submission.external_post_id,
        )
        .await
        .map_err(|e| match e {
            composio::MetricError::Unsupported(m) => RefreshError::Unsupported(m),
            composio::MetricError::Upstream(m) => RefreshError::Upstream(m),
        })?;

        self.apply_metric_outcome(&submission, previous, outcome)
            .await
    }

    /// Write what a fetch produced.
    ///
    /// The store is touched in exactly ONE branch. A
    /// [`composio::MetricOutcome::NeedsConnection`] returns before the first write,
    /// so an unlinked account can neither append a snapshot nor re-price a payout —
    /// the property this split exists to make readable (and testable without a
    /// network).
    async fn apply_metric_outcome(
        &self,
        submission: &Submission,
        previous: Option<MetricSnapshot>,
        outcome: composio::MetricOutcome,
    ) -> Result<RefreshOutcome, RefreshError> {
        let sample = match outcome {
            composio::MetricOutcome::NeedsConnection {
                message,
                connect_url,
            } => {
                return Ok(RefreshOutcome::NeedsConnection {
                    message,
                    connect_url,
                })
            }
            composio::MetricOutcome::Sample(sample) => sample,
        };
        let id = submission.id.as_str();

        let mut snapshot = MetricSnapshot {
            id: 0,
            submission_id: submission.id.clone(),
            captured_at: now_iso(),
            views: sample.views,
            likes: sample.likes,
            comments: sample.comments,
            shares: sample.shares,
            saves: sample.saves,
            source: MetricSource::Composio,
        };
        snapshot.id = self
            .store
            .insert_snapshot(&snapshot)
            .await
            .map_err(|e| RefreshError::Internal(e.to_string()))?;

        // NOT transition-gated: "we looked and nothing moved" is itself the
        // answer a nightly report wants, so the delta rides along instead.
        self.events
            .emit(
                EVENT_METRICS_REFRESHED,
                serde_json::json!({
                    "submission_id": submission.id,
                    "campaign_id": submission.campaign_id,
                    "platform": submission.platform,
                    "source": MetricSource::Composio.as_str(),
                    "captured_at": snapshot.captured_at,
                    "metrics": {
                        "views": snapshot.views,
                        "likes": snapshot.likes,
                        "comments": snapshot.comments,
                        "shares": snapshot.shares,
                        "saves": snapshot.saves,
                    },
                    "delta": {
                        "views": snapshot.views - previous.as_ref().map_or(0, |p| p.views),
                        "likes": snapshot.likes - previous.as_ref().map_or(0, |p| p.likes),
                        "comments": snapshot.comments - previous.as_ref().map_or(0, |p| p.comments),
                    },
                }),
            )
            .await;

        let payout = self
            .accrue_submission(id)
            .await
            .map_err(RefreshError::Internal)?
            .and_then(|a| a.payout);

        Ok(RefreshOutcome::Refreshed {
            snapshot,
            previous,
            payout,
        })
    }

    /// Refresh every approved submission in a campaign, **one at a time**.
    ///
    /// Sequential is not a style choice: the accrual pass prices each post
    /// against the money committed *so far*, so gathering every submission's
    /// inputs up front and only then writing would price them all against the
    /// same stale total and blow past `budget_cents` by every post but one.
    ///
    /// Best-effort per submission: one platform being down never fails the batch
    /// and never rolls back the snapshots that did land. A submission whose account
    /// is not linked is reported as `needs_connection` and counted apart from the
    /// failures — it is not broken, it is unconfigured, and it wrote nothing.
    pub async fn refresh_campaign(
        &self,
        campaign_id: &str,
    ) -> Result<CampaignRefreshReport, String> {
        let filter = SubmissionFilter {
            campaign_id: Some(campaign_id.to_string()),
            status: Some(SubmissionStatus::Approved),
            ..SubmissionFilter::default()
        };
        let submissions = self
            .store
            .list_submissions(&filter)
            .await
            .map_err(|e| e.to_string())?;

        let mut results = Vec::with_capacity(submissions.len());
        for submission in submissions {
            let report = match self.refresh_submission(&submission.id).await {
                Ok(outcome) => SubmissionRefreshReport::from_outcome(&submission.id, outcome),
                Err(e) => SubmissionRefreshReport::from_error(&submission.id, &e),
            };
            results.push(report);
        }
        let counts = RefreshCounts::of(&results);
        Ok(CampaignRefreshReport { results, counts })
    }

    /// Append a hand-entered snapshot and re-run the submission's accrual.
    ///
    /// No `metrics.refreshed` event: that event means "the curated Composio
    /// action answered", and a consumer that reacted to a human typing a number
    /// would be reacting to the wrong thing.
    ///
    /// `Ok(None)` when there is no such submission.
    pub async fn append_manual_snapshot(
        &self,
        id: &str,
        sample: composio::MetricSample,
    ) -> Result<Option<(MetricSnapshot, Option<Payout>)>, String> {
        let Some(submission) = self.store.get_submission(id).await.map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        let mut snapshot = MetricSnapshot {
            id: 0,
            submission_id: submission.id,
            captured_at: now_iso(),
            views: sample.views,
            likes: sample.likes,
            comments: sample.comments,
            shares: sample.shares,
            saves: sample.saves,
            source: MetricSource::Manual,
        };
        snapshot.id = self
            .store
            .insert_snapshot(&snapshot)
            .await
            .map_err(|e| e.to_string())?;
        let payout = self.accrue_submission(id).await?.and_then(|a| a.payout);
        Ok(Some((snapshot, payout)))
    }

    // ---- accrual ----------------------------------------------------------

    /// Re-price one submission's payout row from its latest snapshot.
    ///
    /// **In place.** The row's id is reused, so a second refresh rewrites the
    /// amount rather than adding a second row — which is what keeps a campaign's
    /// spend from doubling.
    ///
    /// Rows at `approved` or `paid` are frozen. A `paid` row is money that has
    /// left; an `approved` row is a specific number a human signed off on, and
    /// silently moving it under them would make the approval meaningless.
    ///
    /// `Ok(None)` when the submission (or its campaign) is gone.
    pub async fn accrue_submission(&self, id: &str) -> Result<Option<AccrualOutcome>, String> {
        let Some(inputs) = self
            .store
            .accrual_inputs(id)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        let existing = inputs.existing_payout.clone();
        let previous_cents = existing.as_ref().map_or(0, |p| p.amount_cents);

        let frozen = matches!(
            existing.as_ref().map(|p| p.status),
            Some(PayoutStatus::Approved | PayoutStatus::Paid)
        );
        let unreviewed = !matches!(
            inputs.submission.status,
            SubmissionStatus::Approved | SubmissionStatus::Paid
        );
        if frozen || unreviewed {
            return Ok(Some(AccrualOutcome {
                payout: existing,
                previous_cents,
                changed: false,
            }));
        }

        let views = inputs.views();
        let amount_cents = inputs.amount_cents();
        let now = now_iso();
        let payout = Payout {
            id: existing
                .as_ref()
                .map_or_else(|| new_id("pay"), |p| p.id.clone()),
            campaign_id: inputs.submission.campaign_id.clone(),
            creator_id: inputs.submission.creator_id.clone(),
            submission_id: Some(inputs.submission.id.clone()),
            amount_cents,
            status: PayoutStatus::Accrued,
            reason: accrual_reason(views, &inputs.campaign, amount_cents),
            accrued_at: existing
                .as_ref()
                .map_or_else(|| now.clone(), |p| p.accrued_at.clone()),
            approved_at: None,
            paid_at: None,
            created_at: existing
                .as_ref()
                .map_or_else(|| now.clone(), |p| p.created_at.clone()),
            updated_at: now.clone(),
        };
        self.store
            .upsert_payout(&payout)
            .await
            .map_err(|e| e.to_string())?;

        let changed = amount_cents != previous_cents;
        if changed {
            self.events
                .emit(
                    EVENT_PAYOUT_ACCRUED,
                    serde_json::json!({
                        "payout_id": payout.id,
                        "campaign_id": payout.campaign_id,
                        "creator_id": payout.creator_id,
                        "submission_id": payout.submission_id,
                        "amount_cents": payout.amount_cents,
                        "previous_cents": previous_cents,
                        "status": payout.status.as_str(),
                        "reason": payout.reason,
                    }),
                )
                .await;
        }

        // Budget crossing, derived rather than stored: the "excluding this"
        // figure is the campaign's committed total minus this row, so adding the
        // old and the new amount to it gives before/after with no extra query.
        // The gate re-arms by construction — once `before` is already at or over
        // the budget it can never fire again, and raising the budget puts
        // `before` back under it.
        let budget = inputs.campaign.budget_cents;
        if budget > 0 {
            let base = inputs.campaign_committed_excluding_this;
            let before = base + previous_cents;
            let after = base + amount_cents;
            if before < budget && after >= budget {
                self.events
                    .emit(
                        EVENT_CAMPAIGN_BUDGET_REACHED,
                        serde_json::json!({
                            "campaign_id": inputs.campaign.id,
                            "brand": inputs.campaign.brand,
                            "budget_cents": budget,
                            "accrued_cents": after,
                            "reached_at": now,
                        }),
                    )
                    .await;
            }
        }

        Ok(Some(AccrualOutcome {
            payout: Some(payout),
            previous_cents,
            changed,
        }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Small shared helpers
// ─────────────────────────────────────────────────────────────────────────────

/// The one timestamp format this app writes, everywhere.
#[must_use]
pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// A prefixed opaque id (`cmp_`, `crt_`, `sub_`, `pay_`). The prefix is for the
/// human reading a payout row, not for parsing — nothing branches on it.
#[must_use]
pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// The human sentence stored on a payout row, so an operator can see *why* a
/// number is what it is without re-deriving it.
fn accrual_reason(views: i64, campaign: &Campaign, amount_cents: i64) -> String {
    let mut reason = match campaign.payout {
        PayoutRule::Cpm { cpm_cents } => format!("cpm {cpm_cents}c x {views} views"),
        PayoutRule::Flat { flat_cents } => format!("flat {flat_cents}c per approved post"),
    };
    if let Some(tier) = campaign
        .bonus_tiers
        .iter()
        .filter(|t| views >= t.views)
        .max_by_key(|t| t.views)
    {
        reason.push_str(&format!(
            " + bonus {}c @{} views",
            tier.bonus_cents, tier.views
        ));
    }
    // The unclamped figure, recomputed rather than threaded through, so the note
    // cannot disagree with the number beside it.
    let raw = payout_for(views, &campaign.payout, &campaign.bonus_tiers);
    if amount_cents < raw {
        reason.push_str(" (clamped by the per-creator cap or the campaign budget)");
    }
    reason
}

// ─────────────────────────────────────────────────────────────────────────────
// Test scaffolding
// ─────────────────────────────────────────────────────────────────────────────

/// Shared test scaffolding (a fake [`UgcHost`] + temp-store engine builders)
/// reused by every module's `#[cfg(test)] mod tests` via `crate::testutil`. Kept
/// in one place so the fake host contract lives once, not copied per module.
#[cfg(test)]
pub(crate) mod testutil {
    use super::*;
    use std::sync::Mutex;

    /// A fully in-memory [`UgcHost`]: the app key lives in a field instead of a
    /// file, so the key lifecycle is drivable with no filesystem — and, crucially,
    /// **without touching [`ryu_composio::auth`]'s process-global cache**, which
    /// every test in this binary shares and none may mutate safely.
    #[derive(Default)]
    pub struct FakeHost {
        pub app_key_set: Mutex<bool>,
        /// What [`UgcHost::composio_key_source`] should report when no app key is
        /// set — the "the environment supplies one" case, faked.
        pub env_key: bool,
    }

    impl UgcHost for FakeHost {
        fn set_composio_key(&self, key: &str) -> Result<ComposioKeySource, String> {
            if key.trim().is_empty() {
                return Err("the Composio API key must not be empty".to_string());
            }
            *self.app_key_set.lock().unwrap() = true;
            Ok(ComposioKeySource::App)
        }
        fn clear_composio_key(&self) -> Result<ComposioKeySource, String> {
            *self.app_key_set.lock().unwrap() = false;
            Ok(resolve_key_source(false, self.env_key))
        }
        fn composio_key_source(&self) -> ComposioKeySource {
            resolve_key_source(*self.app_key_set.lock().unwrap(), self.env_key)
        }
    }

    /// A store backed by a fresh temp `ugc.db` (unique per call).
    pub fn temp_store() -> UgcStore {
        let dir = std::env::temp_dir().join(format!("ryu-ugc-test-{}", uuid::Uuid::new_v4()));
        UgcStore::open(dir.join(DB_FILE_NAME)).expect("open temp store")
    }

    /// An engine over a fresh temp store with the supplied host.
    ///
    /// Emits are inert here: `EventEmitter` no-ops when `RYU_CORE_PORT` /
    /// `RYU_EXT_TOKEN` are unset, which is exactly the standalone-test state.
    pub fn engine_with(host: Arc<dyn UgcHost>) -> UgcEngine {
        UgcEngine::new(temp_store(), reqwest::Client::new(), host)
    }

    /// An engine over a fresh temp store with a default [`FakeHost`].
    pub fn test_engine() -> UgcEngine {
        engine_with(Arc::new(FakeHost::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{test_engine, FakeHost};

    fn campaign(id: &str, payout: PayoutRule, budget_cents: i64) -> Campaign {
        Campaign {
            id: id.into(),
            brand: "Acme".into(),
            brief: "post a clip".into(),
            status: CampaignStatus::Active,
            platforms: vec!["youtube".into()],
            required_hashtags: vec![],
            required_mentions: vec![],
            starts_at: None,
            ends_at: None,
            budget_cents,
            payout,
            bonus_tiers: vec![],
            max_payout_per_creator_cents: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
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
            submitted_at: now_iso(),
            reviewed_at: None,
            rejection_reason: None,
            created_at: now_iso(),
            updated_at: now_iso(),
        }
    }

    #[tokio::test]
    async fn approving_twice_is_a_no_op_the_second_time() {
        let engine = test_engine();
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 5000 }, 0))
            .await
            .unwrap();
        engine
            .create_submission(&submission("s1", "c1", "abc"))
            .await
            .unwrap();

        let first = engine
            .review_submission("s1", true, None)
            .await
            .unwrap()
            .expect("submission exists");
        assert!(first.changed, "the first approval is a real transition");
        assert_eq!(first.payout.as_ref().unwrap().amount_cents, 5000);

        let second = engine
            .review_submission("s1", true, None)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !second.changed,
            "re-approving must be a no-op that emits nothing"
        );
        // And it did not add a second payout row.
        assert_eq!(
            second.payout.as_ref().unwrap().id,
            first.payout.as_ref().unwrap().id
        );
    }

    #[tokio::test]
    async fn accrual_reprices_in_place_and_never_doubles_spend() {
        let engine = test_engine();
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Cpm { cpm_cents: 250 }, 0))
            .await
            .unwrap();
        engine
            .create_submission(&submission("s1", "c1", "abc"))
            .await
            .unwrap();
        engine.review_submission("s1", true, None).await.unwrap();

        for views in [10_000i64, 41_200] {
            engine
                .append_manual_snapshot(
                    "s1",
                    composio::MetricSample {
                        views,
                        ..composio::MetricSample::default()
                    },
                )
                .await
                .unwrap();
        }

        let payouts = engine
            .store
            .list_payouts(&PayoutFilter::default())
            .await
            .unwrap();
        assert_eq!(payouts.len(), 1, "one row per submission, re-priced in place");
        // 41 200 views at 250c/1k floors to 10 300c — integer cents, never a float.
        assert_eq!(payouts[0].amount_cents, 10_300);
    }

    #[tokio::test]
    async fn rejecting_removes_the_accrued_payout() {
        let engine = test_engine();
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 5000 }, 0))
            .await
            .unwrap();
        engine
            .create_submission(&submission("s1", "c1", "abc"))
            .await
            .unwrap();
        engine.review_submission("s1", true, None).await.unwrap();
        assert!(engine
            .store
            .payout_for_submission("s1")
            .await
            .unwrap()
            .is_some());

        let out = engine
            .review_submission("s1", false, Some("missing the hashtag".into()))
            .await
            .unwrap()
            .unwrap();
        assert!(out.changed);
        assert_eq!(out.submission.rejection_reason.as_deref(), Some("missing the hashtag"));
        assert!(
            engine
                .store
                .payout_for_submission("s1")
                .await
                .unwrap()
                .is_none(),
            "un-accruing money is fine; the row must go"
        );
    }

    #[tokio::test]
    async fn accrual_clamps_to_the_campaign_budget() {
        let engine = test_engine();
        // Budget 7 500c against two 5 000c flat posts: the second is clamped to
        // the 2 500c of headroom left, not paid in full.
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 5000 }, 7500))
            .await
            .unwrap();
        for (id, post) in [("s1", "aaa"), ("s2", "bbb")] {
            engine
                .create_submission(&submission(id, "c1", post))
                .await
                .unwrap();
            engine.review_submission(id, true, None).await.unwrap();
        }
        let summary = engine.store.campaign_summary("c1").await.unwrap().unwrap();
        assert_eq!(summary.committed_cents, 7500);
        assert_eq!(summary.remaining_cents, Some(0));
    }

    #[tokio::test]
    async fn refresh_refuses_a_platform_with_no_curated_source() {
        let engine = test_engine();
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 1 }, 0))
            .await
            .unwrap();
        let mut s = submission("s1", "c1", "abc");
        s.platform = "myspace".into();
        engine.create_submission(&s).await.unwrap();

        let err = engine.refresh_submission("s1").await.unwrap_err();
        assert!(
            matches!(err, RefreshError::Unsupported(ref m) if m.contains("no Composio metric source")),
            "got {err}"
        );
    }

    /// A refreshable submission for the two `apply_metric_outcome` tests: approved
    /// (so accrual can price it) and on a curated platform.
    async fn approved_submission(engine: &UgcEngine) -> Submission {
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Cpm { cpm_cents: 250 }, 0))
            .await
            .unwrap();
        engine
            .create_submission(&submission("s1", "c1", "aaa"))
            .await
            .unwrap();
        engine.review_submission("s1", true, None).await.unwrap();
        engine.store.get_submission("s1").await.unwrap().unwrap()
    }

    /// THE money-critical property, tested where it actually lives now that the
    /// fetch talks to Composio directly (so no fake host can stand in for it): a
    /// not-connected account writes NO snapshot and re-prices NO payout. Five
    /// zeroes here would drop a live payout to nothing on the next accrual pass.
    #[tokio::test]
    async fn a_needs_connection_outcome_writes_no_snapshot_and_reprices_nothing() {
        let engine = test_engine();
        let sub = approved_submission(&engine).await;
        let before = engine
            .store
            .payout_for_submission("s1")
            .await
            .unwrap()
            .map_or(0, |p| p.amount_cents);

        let outcome = engine
            .apply_metric_outcome(
                &sub,
                None,
                composio::MetricOutcome::NeedsConnection {
                    message: "No active connection for YouTube".to_string(),
                    connect_url: Some("https://composio.dev/connect/abc".to_string()),
                },
            )
            .await
            .expect("not connected is an outcome, never an error");

        assert!(matches!(outcome, RefreshOutcome::NeedsConnection { ref message, .. }
            if message.contains("No active connection")));
        assert!(
            engine.store.latest_snapshot("s1").await.unwrap().is_none(),
            "an unlinked account must not leave a reading behind"
        );
        assert_eq!(
            engine
                .store
                .payout_for_submission("s1")
                .await
                .unwrap()
                .map_or(0, |p| p.amount_cents),
            before,
            "and must not re-price the payout"
        );

        // The report the API serves carries the connect link and no snapshot.
        let report = SubmissionRefreshReport::from_outcome("s1", outcome);
        assert_eq!(report.status, RefreshStatus::NeedsConnection);
        assert!(report.snapshot.is_none());
        assert_eq!(
            report.connect_url.as_deref(),
            Some("https://composio.dev/connect/abc")
        );
    }

    /// …and the same seam DOES write on a real sample, so the guard above is a
    /// branch, not a broken path.
    #[tokio::test]
    async fn a_sample_outcome_writes_the_snapshot_and_reprices() {
        let engine = test_engine();
        let sub = approved_submission(&engine).await;

        let outcome = engine
            .apply_metric_outcome(
                &sub,
                None,
                composio::MetricOutcome::Sample(composio::MetricSample {
                    views: 41_200,
                    ..composio::MetricSample::default()
                }),
            )
            .await
            .unwrap();

        let RefreshOutcome::Refreshed { snapshot, .. } = outcome else {
            panic!("a sample must refresh");
        };
        assert_eq!(snapshot.views, 41_200);
        // cpm 250c x 41 200 views / 1000 = 10 300c.
        assert_eq!(
            engine
                .store
                .payout_for_submission("s1")
                .await
                .unwrap()
                .unwrap()
                .amount_cents,
            10_300
        );
        assert_eq!(
            SubmissionRefreshReport::from_outcome("s1", RefreshOutcome::Refreshed {
                snapshot,
                previous: None,
                payout: None,
            })
            .status,
            RefreshStatus::Ok
        );
    }

    /// One bad submission never fails the batch, and each gets its own line.
    ///
    /// Both rows are unrefreshable on purpose — one on an uncurated platform, one
    /// whose URL never parsed — which is also what keeps the test hermetic: a
    /// refreshable row would dispatch to Composio for real. (They differ in
    /// platform because `(campaign, platform, external_post_id)` is unique, so two
    /// empty post ids would be a duplicate, not a second row.)
    #[tokio::test]
    async fn campaign_refresh_reports_per_submission_and_never_fails_the_batch() {
        let engine = test_engine();
        engine
            .store
            .upsert_campaign(&campaign("c1", PayoutRule::Cpm { cpm_cents: 250 }, 0))
            .await
            .unwrap();
        let mut uncurated = submission("s1", "c1", "aaa");
        uncurated.platform = "myspace".into();
        engine.create_submission(&uncurated).await.unwrap();
        let mut unparsed = submission("s2", "c1", "");
        unparsed.external_post_id = String::new();
        engine.create_submission(&unparsed).await.unwrap();
        for id in ["s1", "s2"] {
            engine.review_submission(id, true, None).await.unwrap();
        }

        let report = engine.refresh_campaign("c1").await.unwrap();
        assert_eq!(report.results.len(), 2);
        assert_eq!(
            report.counts,
            RefreshCounts {
                ok: 0,
                needs_connection: 0,
                error: 2
            }
        );
        assert!(report.results.iter().all(|r| r.status == RefreshStatus::Error
            && r.snapshot.is_none()
            && r.message.is_some()));
        // Nothing was written for either.
        assert!(engine.store.latest_snapshot("s1").await.unwrap().is_none());
    }

    /// The three statuses are counted apart — "link this account" must never be
    /// reported to the panel as a failure.
    #[test]
    fn counts_split_ok_needs_connection_and_error() {
        let reports = vec![
            SubmissionRefreshReport::from_outcome(
                "s1",
                RefreshOutcome::NeedsConnection {
                    message: "connect it".to_string(),
                    connect_url: None,
                },
            ),
            SubmissionRefreshReport::from_error("s2", &RefreshError::Upstream("502".to_string())),
            SubmissionRefreshReport::from_error("s3", &RefreshError::NotFound),
        ];
        assert_eq!(
            RefreshCounts::of(&reports),
            RefreshCounts {
                ok: 0,
                needs_connection: 1,
                error: 2
            }
        );
        // Every field is present on the wire, `null` where it does not apply, so a
        // consumer can switch on `status` without probing for keys.
        let wire = serde_json::to_value(&reports[0]).unwrap();
        assert_eq!(wire["status"], "needs_connection");
        assert!(wire.get("connect_url").is_some_and(serde_json::Value::is_null));
        assert!(wire.get("snapshot").is_some_and(serde_json::Value::is_null));
    }

    /// The key source is reported from two independent facts, and `env` is never
    /// mistaken for "gone" after a delete.
    #[test]
    fn key_source_prefers_the_app_key_then_the_env_then_none() {
        assert_eq!(resolve_key_source(true, true), ComposioKeySource::App);
        assert_eq!(resolve_key_source(true, false), ComposioKeySource::App);
        assert_eq!(resolve_key_source(false, true), ComposioKeySource::Env);
        assert_eq!(resolve_key_source(false, false), ComposioKeySource::None);
        assert!(ComposioKeySource::Env.is_configured());
        assert!(!ComposioKeySource::None.is_configured());
        assert_eq!(ComposioKeySource::App.as_str(), "app");
        assert_eq!(
            serde_json::to_value(ComposioKeySource::None).unwrap(),
            serde_json::json!("none")
        );
    }

    /// The host seam takes a key and gives none back: a delete falls through to the
    /// environment honestly rather than claiming the app is unconfigured.
    #[test]
    fn host_key_lifecycle_reports_the_source_without_returning_the_key() {
        let host = Arc::new(FakeHost {
            env_key: true,
            ..FakeHost::default()
        });
        // Reached the way the API surface reaches it, through the engine.
        let engine = testutil::engine_with(host.clone());
        assert_eq!(
            engine.host().composio_key_source(),
            ComposioKeySource::Env,
            "no app key yet, but the environment has one"
        );
        assert_eq!(
            engine.host().set_composio_key("comp_live_secret").unwrap(),
            ComposioKeySource::App
        );
        assert_eq!(host.composio_key_source(), ComposioKeySource::App);
        // Clearing falls back to the env key that is still there.
        assert_eq!(
            host.clear_composio_key().unwrap(),
            ComposioKeySource::Env,
            "an env key still present must be reported, not hidden"
        );
        // An empty key is refused, and no refusal ever quotes a key.
        let err = host.set_composio_key("   ").unwrap_err();
        assert!(!err.contains("comp_live"), "{err}");
    }

    #[test]
    fn accrual_reason_names_the_rule_the_bonus_and_the_clamp() {
        let mut c = campaign("c1", PayoutRule::Cpm { cpm_cents: 250 }, 0);
        c.bonus_tiers = vec![
            BonusTier {
                views: 10_000,
                bonus_cents: 500,
            },
            BonusTier {
                views: 25_000,
                bonus_cents: 2_000,
            },
        ];
        // 41 200 views: 10 300c base + the 25k tier only (tiers are never summed).
        let reason = accrual_reason(41_200, &c, 12_300);
        assert!(reason.contains("cpm 250c x 41200 views"), "{reason}");
        assert!(reason.contains("bonus 2000c @25000 views"), "{reason}");
        assert!(!reason.contains("clamped"), "{reason}");
        assert!(accrual_reason(41_200, &c, 100).contains("clamped"));
    }

    #[test]
    fn global_engine_is_publishable_once() {
        set_global_engine(test_engine());
        assert!(global_engine().is_some());
        // Idempotent: a second publish is ignored, not a panic.
        set_global_engine(test_engine());
    }
}
