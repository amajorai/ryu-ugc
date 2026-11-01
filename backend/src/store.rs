//! SQLite-backed persistence for UGC campaigns, creators, submissions, metric
//! snapshots and payouts.
//!
//! Five tables live in `<data dir>/plugin-data/@ryu+ugc/ugc.db`, split along the house hybrid that
//! `ryu-monitors` / `ryu-dashboards` / `ryu-quests` all use:
//!   - `campaigns`, `creators` — **definition** tables. The authoritative record is
//!     the `json` blob; only the columns SQL must filter or sort on are
//!     denormalised out of it. Those columns are rewritten from the blob on every
//!     upsert and never read back into the struct (the same contract as quests'
//!     `kind` column).
//!   - `submissions`, `metric_snapshots`, `payouts` — **append-only / aggregated**
//!     tables with real typed columns, because every derived read in this app
//!     (spend vs budget, total views, the creator leaderboard) is a SQL aggregate
//!     and a blob would force it into memory.
//!
//! Two rules the schema encodes rather than the code:
//!   - a post may be submitted to a campaign **once** (`idx_submissions_post`), so
//!     the same video can never be paid twice;
//!   - a submission has **one** payout row (`idx_payouts_submission`), so the
//!     accrual pass re-prices in place as views grow instead of appending a second
//!     row and doubling the campaign's spend.
//!
//! There are no SQLite foreign keys here (no apps-store backend turns them on) —
//! cascading deletes are explicit Rust `DELETE`s, exactly like
//! `MonitorStore::delete_monitor`.
//!
//! Money is **integer cents everywhere**. Nothing on the payout path is `f64`.
//!
//! A broadcast channel fans changed rows out to SSE subscribers, mirroring
//! `QuestStore`/`MonitorStore`; no route consumes it yet.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use ryu_crypto::content_sqlite::{ColumnPlan, KeyKind};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

use crate::seal_keys::PluginKeys;
use crate::seal_sql::{is_unreadable, open_store, opened, SqlSeal};

/// The database file name, joined onto the host's data dir (`<plugin data dir>/ugc.db`)
/// by whoever opens the store. Kept here so the sidecar and an in-process host
/// cannot drift onto two different files.
pub const DB_FILE_NAME: &str = "ugc.db";

// ─────────────────────────────────────────────────────────────────────────────
// Enums — stored as the lowercase strings the schema comments name, read back
// leniently so an unknown value from a newer writer degrades instead of failing
// the whole list query.
// ─────────────────────────────────────────────────────────────────────────────

/// Lifecycle of a campaign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignStatus {
    #[default]
    Draft,
    Active,
    Paused,
    Ended,
}

impl CampaignStatus {
    /// The stored form (`campaigns.status`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Ended => "ended",
        }
    }

    /// Strict parse of a caller-supplied filter value. `None` for anything
    /// unrecognised — the list endpoint turns that into "all", never an empty
    /// list that reads to the user like "you have none".
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "draft" => Some(Self::Draft),
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            "ended" => Some(Self::Ended),
            _ => None,
        }
    }
}

/// Review state of one submitted post.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionStatus {
    #[default]
    Pending,
    Approved,
    Rejected,
    Paid,
}

impl SubmissionStatus {
    /// The stored form (`submissions.status`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Paid => "paid",
        }
    }

    /// Strict parse for a caller-supplied `?status=` filter.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pending" => Some(Self::Pending),
            "approved" => Some(Self::Approved),
            "rejected" => Some(Self::Rejected),
            "paid" => Some(Self::Paid),
            _ => None,
        }
    }

    /// Lenient read of a stored value; an unknown string degrades to `Pending`
    /// (an unreviewed row) rather than dropping the submission from the list.
    #[must_use]
    pub fn from_db(s: &str) -> Self {
        Self::parse(s).unwrap_or(Self::Pending)
    }
}

/// Money state of one payout row. Money only ever moves forward:
/// `accrued -> approved -> paid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayoutStatus {
    #[default]
    Accrued,
    Approved,
    Paid,
}

impl PayoutStatus {
    /// The stored form (`payouts.status`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accrued => "accrued",
            Self::Approved => "approved",
            Self::Paid => "paid",
        }
    }

    /// Strict parse for a caller-supplied `?status=` filter.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "accrued" => Some(Self::Accrued),
            "approved" => Some(Self::Approved),
            "paid" => Some(Self::Paid),
            _ => None,
        }
    }

    /// Lenient read of a stored value; unknown degrades to `Accrued`, the state
    /// that has not yet released any money.
    #[must_use]
    pub fn from_db(s: &str) -> Self {
        Self::parse(s).unwrap_or(Self::Accrued)
    }
}

/// Where a metric snapshot came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricSource {
    /// Entered by hand — the only option for a platform with no curated
    /// Composio source, and the correction path for a bad automated read.
    #[default]
    Manual,
    /// Read through the curated platform → Composio action map.
    Composio,
}

impl MetricSource {
    /// The stored form (`metric_snapshots.source`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Composio => "composio",
        }
    }

    /// Lenient read of a stored value; unknown degrades to `Manual`, which never
    /// claims an automated read happened.
    #[must_use]
    pub fn from_db(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "composio" => Self::Composio,
            _ => Self::Manual,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Definition records
// ─────────────────────────────────────────────────────────────────────────────

/// How a campaign prices an approved post. Externally tagged the same way
/// monitors' `CheckType` is, so the wire form is
/// `{"type":"cpm","cpm_cents":250}` / `{"type":"flat","flat_cents":5000}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PayoutRule {
    /// Cents per **1 000** views, priced off the submission's latest snapshot.
    Cpm { cpm_cents: i64 },
    /// A flat fee per approved post, regardless of views.
    Flat { flat_cents: i64 },
}

impl Default for PayoutRule {
    fn default() -> Self {
        Self::Flat { flat_cents: 0 }
    }
}

/// A bonus unlocked at a view threshold. Tiers are **not** summed — the tier with
/// the highest met threshold wins (see [`payout_for`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct BonusTier {
    /// The view count at which this tier unlocks. Met when `views >= views`.
    pub views: i64,
    pub bonus_cents: i64,
}

/// A brand campaign creators post against.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Campaign {
    pub id: String,
    pub brand: String,
    pub brief: String,
    #[serde(default)]
    pub status: CampaignStatus,
    /// Platform keys this campaign accepts (`youtube`, `tiktok`, …), lowercased.
    #[serde(default)]
    pub platforms: Vec<String>,
    /// Hashtags a post must carry to qualify (stored without the `#`).
    #[serde(default)]
    pub required_hashtags: Vec<String>,
    /// Accounts a post must mention (stored without the `@`).
    #[serde(default)]
    pub required_mentions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starts_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ends_at: Option<String>,
    /// Campaign ceiling in cents. **0 means uncapped.**
    #[serde(default)]
    pub budget_cents: i64,
    #[serde(default)]
    pub payout: PayoutRule,
    #[serde(default)]
    pub bonus_tiers: Vec<BonusTier>,
    /// Per-creator ceiling in cents. **0 means uncapped**, the same convention as
    /// `budget_cents` — without it a campaign with no per-creator cap would pay
    /// every creator zero.
    #[serde(default)]
    pub max_payout_per_creator_cents: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// A creator on the roster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Creator {
    pub id: String,
    pub display_name: String,
    /// Per-platform handles, keyed by the same lowercase platform key
    /// `submissions.platform` uses. Ordered so the JSON blob is stable.
    #[serde(default)]
    pub handles: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_email: Option<String>,
    /// Where money actually goes (PayPal address, bank alias, …). Free text: this
    /// app tracks what is owed, it does not move money.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payout_handle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Append-only / aggregated records
// ─────────────────────────────────────────────────────────────────────────────

/// One post submitted to a campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Submission {
    pub id: String,
    pub campaign_id: String,
    pub creator_id: String,
    /// Lowercase platform key — it is the lookup key into the curated Composio
    /// metric-source table.
    pub platform: String,
    pub post_url: String,
    /// The platform-native post id parsed out of `post_url`, or `""` when the URL
    /// could not be parsed. Empty rows are deliberately excluded from the
    /// duplicate-post unique index, so an unparseable URL is still recordable and
    /// reviewable by hand — it just cannot auto-refresh.
    #[serde(default)]
    pub external_post_id: String,
    #[serde(default)]
    pub status: SubmissionStatus,
    pub submitted_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// One reading of a post's counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSnapshot {
    /// Assigned by SQLite; `0` on a row that has not been inserted yet.
    #[serde(default)]
    pub id: i64,
    pub submission_id: String,
    pub captured_at: String,
    #[serde(default)]
    pub views: i64,
    #[serde(default)]
    pub likes: i64,
    #[serde(default)]
    pub comments: i64,
    #[serde(default)]
    pub shares: i64,
    #[serde(default)]
    pub saves: i64,
    #[serde(default)]
    pub source: MetricSource,
}

/// Money owed to a creator for a campaign. At most one row per submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payout {
    pub id: String,
    pub campaign_id: String,
    pub creator_id: String,
    /// `None` for a campaign-level bonus or manual adjustment that belongs to a
    /// creator but not to one post.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
    #[serde(default)]
    pub amount_cents: i64,
    #[serde(default)]
    pub status: PayoutStatus,
    /// What the accrual pass computed, in words ("cpm 250c x 41.2k views").
    #[serde(default)]
    pub reason: String,
    pub accrued_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paid_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Derived reads
// ─────────────────────────────────────────────────────────────────────────────

/// Submission counts by review state. Used by the overview, the campaign summary
/// and a creator's cross-campaign totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionCounts {
    pub pending: i64,
    pub approved: i64,
    pub rejected: i64,
    pub paid: i64,
}

impl SubmissionCounts {
    /// Fold one `(status, count)` row from a `GROUP BY status` query.
    fn add(&mut self, status: SubmissionStatus, count: i64) {
        match status {
            SubmissionStatus::Pending => self.pending += count,
            SubmissionStatus::Approved => self.approved += count,
            SubmissionStatus::Rejected => self.rejected += count,
            SubmissionStatus::Paid => self.paid += count,
        }
    }

    /// Total submissions across every state.
    #[must_use]
    pub fn total(&self) -> i64 {
        self.pending + self.approved + self.rejected + self.paid
    }
}

/// Spend vs budget for one campaign. Counter totals come from each submission's
/// **latest** snapshot, never from the sum of all snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignSummary {
    /// 0 = uncapped.
    pub budget_cents: i64,
    pub accrued_cents: i64,
    pub approved_cents: i64,
    pub paid_cents: i64,
    /// Every payout row regardless of status — accrued money is already
    /// committed, so this is what the budget is actually spent against.
    pub committed_cents: i64,
    /// `budget_cents - committed_cents`, floored at 0. `None` when the campaign is
    /// uncapped (`budget_cents == 0`), so "unlimited" is never rendered as "0 left".
    pub remaining_cents: Option<i64>,
    pub total_views: i64,
    pub total_likes: i64,
    pub total_comments: i64,
    pub submissions: SubmissionCounts,
    /// Distinct creators who submitted to this campaign.
    pub creators: i64,
}

/// One row of a campaign's creator leaderboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardRow {
    pub creator_id: String,
    /// Empty when the creator row was deleted out from under the submissions.
    pub display_name: String,
    /// Sum of the latest snapshot's views across this creator's submissions.
    pub views: i64,
    /// Submissions that cleared review (`approved` or `paid`).
    pub approved_submissions: i64,
    pub accrued_cents: i64,
    pub paid_cents: i64,
}

/// A submission with its latest counters and its payout row's money attached —
/// what both the campaign submission list and the single-submission read serve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionWithMetrics {
    #[serde(flatten)]
    pub submission: Submission,
    /// `None` until the first snapshot lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest: Option<MetricSnapshot>,
    /// The joined payout row's `amount_cents`, or `None` when this submission has
    /// no payout row at all.
    ///
    /// **`None` is not `0`.** A payout row priced at zero (an exhausted budget, a
    /// zero-view post) genuinely says "worth nothing"; no row at all says "nothing
    /// has accrued yet". Collapsing the second into the first is a lie the panel
    /// would render as a real figure, so — unlike `latest` — this field is
    /// serialized even when absent (no `skip_serializing_if`): the key is always
    /// present and "not accrued" arrives as an explicit `null`, the same contract
    /// `CampaignSummary::remaining_cents` uses for "uncapped".
    ///
    /// Not to be confused with [`CampaignSummary::accrued_cents`], which is a sum
    /// filtered to `accrued`-status money. This is *this row's* amount whatever
    /// its state — for a paid payout it holds the paid amount, and only
    /// `payout_status` tells the two apart.
    #[serde(default)]
    pub accrued_cents: Option<i64>,
    /// The joined payout row's state (`accrued` / `approved` / `paid`), so the
    /// panel can tell committed money from settled money without a second fetch.
    /// `None` exactly when `accrued_cents` is.
    ///
    /// Deliberately a **string**, not [`PayoutStatus`]: `from_db` degrades an
    /// unrecognised value to `Accrued`, which would report already-paid money as
    /// still accruing. The list read binds `payouts.status` straight into this
    /// field, so a state a newer writer invented survives instead of being
    /// renamed into one this build knows. (Every write here goes through
    /// `PayoutStatus::as_str`, so only the three known values are ever stored —
    /// which is why the single-submission read may reconstruct it from its
    /// already-parsed [`Payout`] row.)
    #[serde(default)]
    pub payout_status: Option<String>,
}

/// A creator's totals across every campaign.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreatorTotals {
    pub submissions: SubmissionCounts,
    pub accrued_cents: i64,
    pub paid_cents: i64,
}

/// The dock panel's first paint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UgcOverview {
    pub campaigns: i64,
    pub creators: i64,
    pub submissions: SubmissionCounts,
    pub accrued_cents: i64,
    pub paid_cents: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Filters + write outcomes
// ─────────────────────────────────────────────────────────────────────────────

/// SQL-side filter for the submission lists. Every field is optional and ANDed;
/// an unrecognised status/platform is dropped by the caller before it gets here.
#[derive(Debug, Clone, Default)]
pub struct SubmissionFilter {
    pub campaign_id: Option<String>,
    pub creator_id: Option<String>,
    pub status: Option<SubmissionStatus>,
    pub platform: Option<String>,
    /// Clamped server-side by [`clamp_limit`].
    pub limit: Option<i64>,
}

/// SQL-side filter for the payout list.
#[derive(Debug, Clone, Default)]
pub struct PayoutFilter {
    pub campaign_id: Option<String>,
    pub creator_id: Option<String>,
    pub status: Option<PayoutStatus>,
    pub limit: Option<i64>,
}

/// What a submission write did. Distinguishing "duplicate post" from "no such
/// row" is what lets the API answer 409 vs 404 without re-querying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Written,
    /// The `(campaign_id, dedup_key)` or `(campaign_id, platform,
    /// external_post_id)` unique index rejected it — this
    /// exact post (by parsed id, or by canonical URL when none parsed) is already
    /// in this campaign. Answering anything but 409
    /// here would let the same post be paid twice.
    DuplicatePost,
    /// No row with that id (update only).
    NotFound,
}

/// What a delete that can cascade into payouts did.
///
/// Paid payouts are the campaign's settlement record (marking one paid needs the
/// separate `ugc.payouts` permission), so NO delete path may remove one: a
/// campaign delete, a forced creator delete and a submission delete all refuse
/// with [`Self::PaidPayoutsRetained`] — inside the same store transaction that
/// would have done the delete, so nothing at all is changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    /// No row with that id.
    NotFound,
    /// The cascade would have removed this many paid payouts; refused untouched.
    PaidPayoutsRetained {
        paid: i64,
    },
}

/// Default and maximum page size for the history/list reads that take `?limit=`.
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

/// Clamp a caller-supplied `?limit=` into the server's own bounds. The clamped
/// value is interpolated into the SQL as an integer literal (SQLite gives a
/// TEXT-bound `LIMIT ?` numeric affinity only by accident); an `i64` rendered by
/// `format!` cannot carry an injection.
#[must_use]
pub fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

// ─────────────────────────────────────────────────────────────────────────────
// The payout math — pure, so every rule below is testable without a database
// ─────────────────────────────────────────────────────────────────────────────

/// Price one approved submission from its latest view count.
///
/// `base` is the campaign's rule; on top of it sits **one** bonus: the tier with
/// the highest met threshold (`views >= tier.views`). Tiers are deliberately NOT
/// summed — a 10k/50k/100k ladder pays the 100k figure, not all three.
///
/// The CPM division is integer `i64` and **floors**: 41 200 views at 250c/1k is
/// 10 300c, and 999 views at 250c/1k is 249c, not 249.75c. That is intentional —
/// money is integer cents on this whole path and nobody should "fix" it into a
/// float. `saturating_mul` keeps an absurd view count from wrapping into a
/// negative payout.
#[must_use]
pub fn payout_for(views: i64, rule: &PayoutRule, tiers: &[BonusTier]) -> i64 {
    let views = views.max(0);
    let base = match rule {
        PayoutRule::Cpm { cpm_cents } => views.saturating_mul((*cpm_cents).max(0)) / 1000,
        PayoutRule::Flat { flat_cents } => (*flat_cents).max(0),
    };
    let bonus = tiers
        .iter()
        .filter(|t| views >= t.views)
        .max_by_key(|t| t.views)
        .map_or(0, |t| t.bonus_cents.max(0));
    base.saturating_add(bonus)
}

/// Clamp a freshly-priced payout against the two ceilings, given the money
/// already committed **elsewhere**.
///
/// Both `_excluding_this` figures come from SQL sums that skip the submission
/// being re-priced, because accrual rewrites its row in place rather than adding
/// a second one — counting the old amount would shrink the cap every refresh.
///
/// `0` means **uncapped** for both ceilings, matching `Campaign::budget_cents`
/// and `Campaign::max_payout_per_creator_cents`.
#[must_use]
pub fn clamp_payout(
    raw_cents: i64,
    creator_committed_excluding_this: i64,
    max_payout_per_creator_cents: i64,
    campaign_committed_excluding_this: i64,
    budget_cents: i64,
) -> i64 {
    let mut amount = raw_cents.max(0);
    if max_payout_per_creator_cents > 0 {
        let headroom = max_payout_per_creator_cents - creator_committed_excluding_this.max(0);
        amount = amount.min(headroom.max(0));
    }
    if budget_cents > 0 {
        let headroom = budget_cents - campaign_committed_excluding_this.max(0);
        amount = amount.min(headroom.max(0));
    }
    amount
}

/// Everything the accrual pass needs about one submission, read in a single
/// store round-trip so the engine composes the pure functions above instead of
/// interleaving five queries with the arithmetic.
#[derive(Debug, Clone)]
pub struct AccrualInputs {
    pub submission: Submission,
    pub campaign: Campaign,
    /// The submission's most recent counters. `None` before the first refresh.
    pub latest: Option<MetricSnapshot>,
    /// The payout row already on this submission, if any — its `id` is reused so
    /// the re-price is an update, never a second row.
    pub existing_payout: Option<Payout>,
    /// Money committed to this creator on this campaign, excluding this
    /// submission's own row.
    pub creator_committed_excluding_this: i64,
    /// Money committed across the whole campaign, excluding this submission's
    /// own row.
    pub campaign_committed_excluding_this: i64,
}

impl AccrualInputs {
    /// The latest view count, or 0 before the first snapshot.
    #[must_use]
    pub fn views(&self) -> i64 {
        self.latest.as_ref().map_or(0, |m| m.views)
    }

    /// What this submission is worth right now, both ceilings applied.
    ///
    /// A submission that has not cleared review is worth nothing: pricing a
    /// pending post would let unreviewed work eat the campaign budget.
    #[must_use]
    pub fn amount_cents(&self) -> i64 {
        if !matches!(
            self.submission.status,
            SubmissionStatus::Approved | SubmissionStatus::Paid
        ) {
            return 0;
        }
        let raw = payout_for(
            self.views(),
            &self.campaign.payout,
            &self.campaign.bonus_tiers,
        );
        clamp_payout(
            raw,
            self.creator_committed_excluding_this,
            self.campaign.max_payout_per_creator_cents,
            self.campaign_committed_excluding_this,
            self.campaign.budget_cents,
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Live events
// ─────────────────────────────────────────────────────────────────────────────

/// A change worth pushing to a live viewer. Mirrors `QuestEvent`; nothing
/// subscribes yet, but the channel is the house shape and costs nothing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UgcEvent {
    CampaignChanged {
        campaign: Box<Campaign>,
    },
    SubmissionChanged {
        submission: Box<Submission>,
    },
    PayoutChanged {
        payout: Box<Payout>,
    },
    /// `entity` is the table name (`campaign`, `creator`, `submission`).
    Deleted {
        entity: String,
        id: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// The store
// ─────────────────────────────────────────────────────────────────────────────

/// Column list for `submissions`, shared by every read so the row mapper cannot
/// drift out of order with the queries.
const SUBMISSION_COLUMNS: &str = "id, campaign_id, creator_id, platform, post_url, \
     external_post_id, status, submitted_at, reviewed_at, rejection_reason, created_at, updated_at";

/// Column list for `payouts`.
const PAYOUT_COLUMNS: &str = "id, campaign_id, creator_id, submission_id, amount_cents, status, \
     reason, accrued_at, approved_at, paid_at, created_at, updated_at";

/// Column list for `metric_snapshots`.
const SNAPSHOT_COLUMNS: &str =
    "id, submission_id, captured_at, views, likes, comments, shares, saves, source";

/// SQLite-backed UGC store. Cheap to clone (wraps `Arc`s).
#[derive(Clone)]
pub struct UgcStore {
    conn: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<UgcEvent>,
    seal: SqlSeal,
}

/// The plugin id (the sealing key's scope).
pub const PLUGIN_ID: &str = "@ryu/ugc";
/// The registered content-store name (part of every key derivation; never rename).
pub const STORE: &str = "ugc.db";

/// The sealed columns (`encb:v1:`, bound to `creators.<column>` and the id):
/// the whole creator record (handles, payout handle, notes) and the
/// denormalised name and email the roster sorts and searches after opening.
pub fn column_plans() -> Result<Vec<ColumnPlan>> {
    ["json", "display_name", "contact_email"]
        .iter()
        .map(|column| ColumnPlan::text("creators", column, "id", KeyKind::Text))
        .collect()
}

impl UgcStore {
    /// Open (or create) the store at a specific path and run migrations. The
    /// caller provides the path so data-folder relocation is honoured.
    ///
    /// The sealing key is resolved with the store's directory as the plugin
    /// data dir (see [`crate::seal_keys`]).
    pub fn open(path: PathBuf) -> Result<Self> {
        let dir = path
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let keys = PluginKeys::resolve(PLUGIN_ID, &dir)?;
        Self::open_with_keys(path, &keys)
    }

    /// [`Self::open`] with explicit keys.
    pub fn open_with_keys(path: PathBuf, keys: &PluginKeys) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir {}", parent.display()))?;
        }
        let mut conn = Connection::open(&path)
            .with_context(|| format!("opening ugc db {}", path.display()))?;
        ryu_crypto::content_sqlite::enable_secure_delete(&conn)?;
        Self::init_schema(&conn)?;
        let seal = open_store(&mut conn, keys, STORE, &column_plans()?, Some(&path))
            .context("sealing ugc.db")?;
        let (tx, _rx) = broadcast::channel(128);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            tx,
            seal,
        })
    }

    /// The sealing health item: `{state, detail?, unreadable_rows}`.
    pub fn seal_health(&self) -> serde_json::Value {
        self.seal.health()
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;

             CREATE TABLE IF NOT EXISTS campaigns (
                 id           TEXT PRIMARY KEY,
                 json         TEXT NOT NULL,
                 -- Denormalised out of the blob so the list query filters in SQL.
                 status       TEXT NOT NULL DEFAULT 'draft',
                 brand        TEXT NOT NULL DEFAULT '',
                 -- 0 = uncapped. Denormalised because the budget-exhaustion check
                 -- joins it against SUM(payouts.amount_cents).
                 budget_cents INTEGER NOT NULL DEFAULT 0,
                 starts_at    TEXT,
                 ends_at      TEXT,
                 created_at   TEXT NOT NULL,
                 updated_at   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_campaigns_status
                 ON campaigns(status, updated_at DESC);

             CREATE TABLE IF NOT EXISTS creators (
                 id            TEXT PRIMARY KEY,
                 json          TEXT NOT NULL,
                 -- Denormalised for the roster's sort + search. Handles, payout
                 -- handle and notes stay in `json`; they are never filtered on.
                 display_name  TEXT NOT NULL DEFAULT '',
                 contact_email TEXT,
                 created_at    TEXT NOT NULL,
                 updated_at    TEXT NOT NULL
             );
             -- Name and email are sealed: the roster sorts and searches them
             -- after opening, so no index covers them (the old ones are dropped).
             DROP INDEX IF EXISTS idx_creators_name;
             DROP INDEX IF EXISTS idx_creators_email;

             CREATE TABLE IF NOT EXISTS submissions (
                 id               TEXT PRIMARY KEY,
                 campaign_id      TEXT NOT NULL,
                 creator_id       TEXT NOT NULL,
                 platform         TEXT NOT NULL,
                 post_url         TEXT NOT NULL,
                 -- The only value ever interpolated into a Composio action's
                 -- arguments, so it is stored rather than re-parsed at refresh.
                 external_post_id TEXT NOT NULL DEFAULT '',
                 -- Nonempty for every accepted row and derived from post_url
                 -- alone, never the platform label: `id:<platform>:<post id>` when
                 -- the URL names a curated platform's post, else
                 -- `url:<canonical url>` (see submission_dedup_key). Older
                 -- databases gain it in migrate_submission_dedup_key.
                 dedup_key        TEXT NOT NULL DEFAULT '',
                 status           TEXT NOT NULL DEFAULT 'pending',
                 submitted_at     TEXT NOT NULL,
                 reviewed_at      TEXT,
                 rejection_reason TEXT,
                 created_at       TEXT NOT NULL,
                 updated_at       TEXT NOT NULL
             );
             -- One row per metrics source: (platform label, parsed id) is what a
             -- refresh fetches, so two rows here would price one upstream post
             -- twice. A parsed id is only stored when the URL's host belongs to
             -- the label (api::parse_post_id), so for every such row the pair is
             -- also exactly the post its dedup_key names. It exempts
             -- unparseable-URL rows (empty id);
             -- idx_submissions_identity (created in migrate_submission_dedup_key)
             -- is the label-independent constraint that covers EVERY row, so the
             -- same post can never accrue and pay twice in one campaign.
             CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_post
                 ON submissions(campaign_id, platform, external_post_id)
                 WHERE external_post_id <> '';
             CREATE INDEX IF NOT EXISTS idx_submissions_campaign
                 ON submissions(campaign_id, status, submitted_at DESC);
             CREATE INDEX IF NOT EXISTS idx_submissions_creator
                 ON submissions(creator_id, submitted_at DESC);

             CREATE TABLE IF NOT EXISTS metric_snapshots (
                 id            INTEGER PRIMARY KEY AUTOINCREMENT,
                 submission_id TEXT NOT NULL,
                 captured_at   TEXT NOT NULL,
                 views         INTEGER NOT NULL DEFAULT 0,
                 likes         INTEGER NOT NULL DEFAULT 0,
                 comments      INTEGER NOT NULL DEFAULT 0,
                 shares        INTEGER NOT NULL DEFAULT 0,
                 saves         INTEGER NOT NULL DEFAULT 0,
                 source        TEXT NOT NULL DEFAULT 'manual'
             );
             -- (submission_id, id DESC) is what makes 'the LATEST snapshot per
             -- submission' — the basis of every summary, leaderboard and accrual —
             -- an index seek rather than a scan.
             CREATE INDEX IF NOT EXISTS idx_metric_snapshots_submission
                 ON metric_snapshots(submission_id, id DESC);

             CREATE TABLE IF NOT EXISTS payouts (
                 id            TEXT PRIMARY KEY,
                 campaign_id   TEXT NOT NULL,
                 creator_id    TEXT NOT NULL,
                 -- NULL for a campaign-level bonus/manual adjustment.
                 submission_id TEXT,
                 amount_cents  INTEGER NOT NULL DEFAULT 0,
                 status        TEXT NOT NULL DEFAULT 'accrued',
                 reason        TEXT NOT NULL DEFAULT '',
                 accrued_at    TEXT NOT NULL,
                 approved_at   TEXT,
                 paid_at       TEXT,
                 created_at    TEXT NOT NULL,
                 updated_at    TEXT NOT NULL
             );
             -- One accrual row per submission: the pass RE-COMPUTES amount_cents
             -- in place as views grow. Without this a re-refresh doubles spend.
             CREATE UNIQUE INDEX IF NOT EXISTS idx_payouts_submission
                 ON payouts(submission_id)
                 WHERE submission_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_payouts_campaign
                 ON payouts(campaign_id, status);
             -- Serves both the per-creator cap and the leaderboard.
             CREATE INDEX IF NOT EXISTS idx_payouts_creator
                 ON payouts(creator_id, campaign_id, status);",
        )
        .context("initializing ugc schema")?;
        Self::migrate_submission_dedup_key(conn)?;
        Ok(())
    }

    /// Give every submission a nonempty, label-independent `dedup_key` and
    /// enforce it unique per `campaign_id` with NO predicate.
    ///
    /// Before this column, deduplication relied on `external_post_id` alone and
    /// the partial index exempted empty ids — so an unparseable URL could be
    /// submitted (and approved, and accrued) any number of times. An interim key
    /// was then scoped per `(campaign_id, platform)`, which still let the same
    /// link through once per free-text platform label; that index
    /// (`idx_submissions_dedup`) is dropped here and every key recomputed.
    ///
    /// Version 2 also re-derives every row's `external_post_id` under the current
    /// rules of [`crate::api::parse_post_id`]: an id is only kept when the URL's
    /// host belongs to the row's platform label, so a legacy `youtube` row on
    /// `evil.example/watch?v=X` stops refreshing (and being priced from) real
    /// YouTube video `X` that a `youtu.be/X` row now owns; percent-encoded ids
    /// (`youtu.be/%61bc`) gain their decoded id. A re-derived id that another row
    /// in the campaign already holds unchanged is left empty (that row keeps it).
    ///
    /// Rows that ALREADY duplicate an earlier row's post in the same campaign
    /// (created while either gap was open, including under a different platform
    /// label) are kept, reviewable and payable exactly as before, under a unique
    /// `legacy:<id>` key: stripping or merging them would silently drop recorded
    /// state, so they are left for an operator to reconcile. Only new writes are
    /// held to the constraint. Runs once per [`SUBMISSION_IDENTITY_VERSION`]
    /// (`PRAGMA user_version`).
    fn migrate_submission_dedup_key(conn: &Connection) -> Result<()> {
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .context("reading user_version")?;
        if version >= SUBMISSION_IDENTITY_VERSION {
            return Ok(());
        }
        let has_column = conn
            .prepare("SELECT 1 FROM pragma_table_info('submissions') WHERE name = 'dedup_key'")?
            .exists([])?;
        let tx = conn
            .unchecked_transaction()
            .context("starting dedup_key migration")?;
        if !has_column {
            tx.execute(
                "ALTER TABLE submissions ADD COLUMN dedup_key TEXT NOT NULL DEFAULT ''",
                [],
            )
            .context("adding submissions.dedup_key")?;
        }
        // Every identity index would reject recomputed values mid-pass; both
        // current ones are recreated below.
        tx.execute_batch(
            "DROP INDEX IF EXISTS idx_submissions_dedup;
             DROP INDEX IF EXISTS idx_submissions_identity;
             DROP INDEX IF EXISTS idx_submissions_post;",
        )
        .context("dropping submission identity indexes")?;
        let rows: Vec<(String, String, String, String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT id, campaign_id, platform, post_url, external_post_id
                 FROM submissions ORDER BY created_at, id",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let reparse = |platform: &str, post_url: &str| {
            url::Url::parse(post_url.trim())
                .ok()
                .and_then(|u| crate::api::parse_post_id(platform, &u))
                .unwrap_or_default()
        };
        // Rows whose id survives the re-derivation unchanged keep it first.
        let mut held_ids: std::collections::HashSet<(String, String, String)> = rows
            .iter()
            .filter(|(_, _, platform, url, old)| !old.is_empty() && reparse(platform, url) == *old)
            .map(|(_, campaign, platform, _, old)| {
                (campaign.clone(), platform.clone(), old.clone())
            })
            .collect();
        let mut seen: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        for (id, campaign_id, platform, post_url, old_post_id) in rows {
            let mut post_id = reparse(&platform, &post_url);
            if !post_id.is_empty()
                && post_id != old_post_id
                && !held_ids.insert((campaign_id.clone(), platform.clone(), post_id.clone()))
            {
                post_id = String::new();
            }
            let key = submission_dedup_key(&post_url);
            let key = if seen.insert((campaign_id, key.clone())) {
                key
            } else {
                format!("legacy:{id}")
            };
            tx.execute(
                "UPDATE submissions SET dedup_key = ?2, external_post_id = ?3 WHERE id = ?1",
                params![id, key, post_id],
            )?;
        }
        tx.execute_batch(&format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_post
                 ON submissions(campaign_id, platform, external_post_id)
                 WHERE external_post_id <> '';
             CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_identity
                 ON submissions(campaign_id, dedup_key);
             PRAGMA user_version = {SUBMISSION_IDENTITY_VERSION};"
        ))
        .context("creating submission identity indexes")?;
        tx.commit().context("committing dedup_key migration")?;
        Ok(())
    }

    /// Broadcast a change to SSE subscribers. A send error just means nobody is
    /// listening — not a failure.
    pub fn broadcast(&self, event: UgcEvent) {
        let _ = self.tx.send(event);
    }

    /// Subscribe to live changes.
    pub fn subscribe(&self) -> broadcast::Receiver<UgcEvent> {
        self.tx.subscribe()
    }

    // ---- campaigns --------------------------------------------------------

    /// Insert or replace a campaign definition, then broadcast it. The
    /// denormalised columns are rewritten from the blob on every write.
    pub async fn upsert_campaign(&self, campaign: &Campaign) -> Result<()> {
        let json = serde_json::to_string(campaign).context("serializing campaign")?;
        {
            let conn = self.conn.lock().await;
            conn.execute(
                "INSERT INTO campaigns
                   (id, json, status, brand, budget_cents, starts_at, ends_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                   json = ?2, status = ?3, brand = ?4, budget_cents = ?5,
                   starts_at = ?6, ends_at = ?7, updated_at = ?9",
                params![
                    campaign.id,
                    json,
                    campaign.status.as_str(),
                    campaign.brand,
                    campaign.budget_cents,
                    campaign.starts_at,
                    campaign.ends_at,
                    campaign.created_at,
                    campaign.updated_at,
                ],
            )
            .context("upserting campaign")?;
        }
        self.broadcast(UgcEvent::CampaignChanged {
            campaign: Box::new(campaign.clone()),
        });
        Ok(())
    }

    /// Fetch a campaign by id.
    pub async fn get_campaign(&self, id: &str) -> Result<Option<Campaign>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM campaigns WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading campaign")?;
        match json {
            Some(j) => Ok(Some(
                serde_json::from_str(&j).context("deserializing campaign")?,
            )),
            None => Ok(None),
        }
    }

    /// List campaigns, newest-updated first. `status = None` lists all — the API
    /// maps an unrecognised `?status=` to `None` rather than to an empty list.
    pub async fn list_campaigns(&self, status: Option<CampaignStatus>) -> Result<Vec<Campaign>> {
        let conn = self.conn.lock().await;
        let mut out = Vec::new();
        match status {
            Some(s) => {
                let mut stmt = conn.prepare(
                    "SELECT json FROM campaigns WHERE status = ?1 ORDER BY updated_at DESC",
                )?;
                let rows = stmt.query_map(params![s.as_str()], |row| row.get::<_, String>(0))?;
                for row in rows {
                    if let Ok(c) = serde_json::from_str::<Campaign>(&row?) {
                        out.push(c);
                    }
                }
            }
            None => {
                let mut stmt =
                    conn.prepare("SELECT json FROM campaigns ORDER BY updated_at DESC")?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                for row in rows {
                    if let Ok(c) = serde_json::from_str::<Campaign>(&row?) {
                        out.push(c);
                    }
                }
            }
        }
        Ok(out)
    }

    /// How many campaigns exist. The health probe's cheap "is the store
    /// readable?" assertion.
    pub async fn count_campaigns(&self) -> Result<i64> {
        let conn = self.conn.lock().await;
        let n = conn
            .query_row("SELECT COUNT(*) FROM campaigns", [], |row| {
                row.get::<_, i64>(0)
            })
            .context("counting campaigns")?;
        Ok(n)
    }

    /// Delete a campaign and everything hanging off it. There are no SQLite
    /// foreign keys, so the cascade is explicit — and snapshots go **before**
    /// submissions, because the subquery that finds them needs those rows to
    /// still exist.
    ///
    /// Refuses with [`DeleteOutcome::PaidPayoutsRetained`], changing nothing,
    /// when any payout of this campaign (by `campaign_id` or through one of its
    /// submissions) is paid. The check and the cascade share one transaction under
    /// the connection lock, so a payout marked paid concurrently is either seen
    /// here or lands after the rows are gone — never deleted.
    pub async fn delete_campaign(&self, id: &str) -> Result<DeleteOutcome> {
        let outcome = {
            let mut conn = self.conn.lock().await;
            let tx = conn.transaction().context("starting campaign delete")?;
            let paid: i64 = tx.query_row(
                "SELECT COUNT(*) FROM payouts WHERE status = 'paid' AND (campaign_id = ?1
                   OR submission_id IN (SELECT id FROM submissions WHERE campaign_id = ?1))",
                params![id],
                |row| row.get(0),
            )?;
            if paid > 0 {
                return Ok(DeleteOutcome::PaidPayoutsRetained { paid });
            }
            let n = tx.execute("DELETE FROM campaigns WHERE id = ?1", params![id])?;
            tx.execute(
                "DELETE FROM metric_snapshots WHERE submission_id IN
                   (SELECT id FROM submissions WHERE campaign_id = ?1)",
                params![id],
            )?;
            tx.execute(
                "DELETE FROM payouts WHERE status <> 'paid' AND (campaign_id = ?1
                   OR submission_id IN (SELECT id FROM submissions WHERE campaign_id = ?1))",
                params![id],
            )?;
            tx.execute(
                "DELETE FROM submissions WHERE campaign_id = ?1",
                params![id],
            )?;
            tx.commit().context("committing campaign delete")?;
            if n > 0 {
                DeleteOutcome::Deleted
            } else {
                DeleteOutcome::NotFound
            }
        };
        if outcome == DeleteOutcome::Deleted {
            self.broadcast(UgcEvent::Deleted {
                entity: "campaign".to_string(),
                id: id.to_string(),
            });
        }
        Ok(outcome)
    }

    // ---- creators ---------------------------------------------------------

    /// Insert or replace a creator, rewriting the denormalised sort/search
    /// columns from the blob.
    pub async fn upsert_creator(&self, creator: &Creator) -> Result<()> {
        let json = serde_json::to_string(creator).context("serializing creator")?;
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO creators
               (id, json, display_name, contact_email, created_at, updated_at)
             VALUES (?1, ryu_seal('creators.json', ?1, ?2),
                     ryu_seal('creators.display_name', ?1, ?3),
                     ryu_seal('creators.contact_email', ?1, ?4), ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
               json = excluded.json, display_name = excluded.display_name,
               contact_email = excluded.contact_email, updated_at = ?6",
            params![
                creator.id,
                json,
                creator.display_name,
                creator.contact_email,
                creator.created_at,
                creator.updated_at,
            ],
        )
        .context("upserting creator")?;
        Ok(())
    }

    /// Fetch a creator by id.
    pub async fn get_creator(&self, id: &str) -> Result<Option<Creator>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT ryu_open('creators.json', id, json) FROM creators WHERE id = ?1",
                params![id],
                |row| opened(row, 0),
            )
            .optional()
            .context("reading creator")?;
        match json {
            Some(j) => Ok(Some(
                serde_json::from_str(&j).context("deserializing creator")?,
            )),
            None => Ok(None),
        }
    }

    /// List the roster by display name. `q` matches display name or contact
    /// email, case-insensitively. Both are sealed, so the roster (small) is
    /// opened, filtered and sorted in memory; a creator that does not open is
    /// left out and counted.
    pub async fn list_creators(&self, q: Option<&str>) -> Result<Vec<Creator>> {
        let needle = q
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase);
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT ryu_open('creators.json', id, json) FROM creators")?;
        let rows = stmt.query_map([], |row| opened(row, 0))?;
        let mut out = Vec::new();
        for row in rows {
            let json = match row {
                Ok(json) => json,
                Err(error) if is_unreadable(&error) => continue,
                Err(error) => return Err(error.into()),
            };
            let Ok(creator) = serde_json::from_str::<Creator>(&json) else {
                continue;
            };
            let matches = needle.as_deref().map_or(true, |needle| {
                creator.display_name.to_lowercase().contains(needle)
                    || creator
                        .contact_email
                        .as_deref()
                        .is_some_and(|email| email.to_lowercase().contains(needle))
            });
            if matches {
                out.push(creator);
            }
        }
        out.sort_by(|a, b| {
            a.display_name
                .to_lowercase()
                .cmp(&b.display_name.to_lowercase())
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(out)
    }

    /// How many creators exist (the overview count).
    pub async fn count_creators(&self) -> Result<i64> {
        let conn = self.conn.lock().await;
        let n = conn
            .query_row("SELECT COUNT(*) FROM creators", [], |row| {
                row.get::<_, i64>(0)
            })
            .context("counting creators")?;
        Ok(n)
    }

    /// How many submissions a creator has, across all campaigns. The delete
    /// endpoint 409s on a non-zero count unless `?force=true`.
    pub async fn count_submissions_for_creator(&self, creator_id: &str) -> Result<i64> {
        let conn = self.conn.lock().await;
        let n = conn
            .query_row(
                "SELECT COUNT(*) FROM submissions WHERE creator_id = ?1",
                params![creator_id],
                |row| row.get::<_, i64>(0),
            )
            .context("counting creator submissions")?;
        Ok(n)
    }

    /// Delete a creator. When `cascade` is false only the roster row goes — the
    /// caller is expected to have checked [`Self::count_submissions_for_creator`]
    /// first. When true, their submissions, those submissions' snapshots and
    /// their unpaid payouts go too (snapshots first, same reason as the campaign
    /// cascade) — unless any of their payouts is paid, in which case the whole
    /// delete is refused with [`DeleteOutcome::PaidPayoutsRetained`] and nothing
    /// changes. A non-cascading delete never touches a payout.
    pub async fn delete_creator(&self, id: &str, cascade: bool) -> Result<DeleteOutcome> {
        let outcome = {
            let mut conn = self.conn.lock().await;
            let tx = conn.transaction().context("starting creator delete")?;
            if cascade {
                // `force` never reaches past a paid payout: the settlement record
                // outlives the roster row, so the whole delete is refused instead.
                let paid: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM payouts WHERE status = 'paid' AND (creator_id = ?1
                       OR submission_id IN (SELECT id FROM submissions WHERE creator_id = ?1))",
                    params![id],
                    |row| row.get(0),
                )?;
                if paid > 0 {
                    return Ok(DeleteOutcome::PaidPayoutsRetained { paid });
                }
            }
            let n = tx.execute("DELETE FROM creators WHERE id = ?1", params![id])?;
            if cascade {
                tx.execute(
                    "DELETE FROM metric_snapshots WHERE submission_id IN
                       (SELECT id FROM submissions WHERE creator_id = ?1)",
                    params![id],
                )?;
                tx.execute(
                    "DELETE FROM payouts WHERE status <> 'paid' AND (creator_id = ?1
                       OR submission_id IN (SELECT id FROM submissions WHERE creator_id = ?1))",
                    params![id],
                )?;
                tx.execute("DELETE FROM submissions WHERE creator_id = ?1", params![id])?;
            }
            tx.commit().context("committing creator delete")?;
            if n > 0 {
                DeleteOutcome::Deleted
            } else {
                DeleteOutcome::NotFound
            }
        };
        if outcome == DeleteOutcome::Deleted {
            self.broadcast(UgcEvent::Deleted {
                entity: "creator".to_string(),
                id: id.to_string(),
            });
        }
        Ok(outcome)
    }

    /// A creator's cross-campaign totals: submissions by state plus accrued/paid
    /// money.
    pub async fn creator_totals(&self, creator_id: &str) -> Result<CreatorTotals> {
        let conn = self.conn.lock().await;
        let mut totals = CreatorTotals::default();
        {
            let mut stmt = conn.prepare(
                "SELECT status, COUNT(*) FROM submissions WHERE creator_id = ?1 GROUP BY status",
            )?;
            let rows = stmt.query_map(params![creator_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, count) = row?;
                totals
                    .submissions
                    .add(SubmissionStatus::from_db(&status), count);
            }
        }
        {
            let mut stmt = conn.prepare(
                "SELECT status, COALESCE(SUM(amount_cents), 0) FROM payouts
                 WHERE creator_id = ?1 GROUP BY status",
            )?;
            let rows = stmt.query_map(params![creator_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, cents) = row?;
                match PayoutStatus::from_db(&status) {
                    PayoutStatus::Accrued => totals.accrued_cents += cents,
                    PayoutStatus::Paid => totals.paid_cents += cents,
                    PayoutStatus::Approved => {}
                }
            }
        }
        Ok(totals)
    }

    // ---- submissions ------------------------------------------------------

    /// Record a new submission.
    ///
    /// Returns [`WriteOutcome::DuplicatePost`] when the
    /// `(campaign_id, dedup_key)` or `(campaign_id, platform, external_post_id)`
    /// unique index rejects it — those indexes are what stop one post being paid
    /// twice (including a post whose URL did not parse into an id, keyed by its
    /// canonical URL, and the same link resent under another platform label), so
    /// the caller must surface it as a 409 rather than retrying.
    pub async fn insert_submission(&self, s: &Submission) -> Result<WriteOutcome> {
        let outcome = {
            let conn = self.conn.lock().await;
            let res = conn.execute(
                "INSERT INTO submissions
                   (id, campaign_id, creator_id, platform, post_url, external_post_id,
                    status, submitted_at, reviewed_at, rejection_reason, created_at, updated_at,
                    dedup_key)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    s.id,
                    s.campaign_id,
                    s.creator_id,
                    s.platform,
                    s.post_url,
                    s.external_post_id,
                    s.status.as_str(),
                    s.submitted_at,
                    s.reviewed_at,
                    s.rejection_reason,
                    s.created_at,
                    s.updated_at,
                    submission_dedup_key(&s.post_url),
                ],
            );
            match res {
                Ok(_) => WriteOutcome::Written,
                Err(err) if is_constraint_violation(&err) => WriteOutcome::DuplicatePost,
                Err(err) => return Err(err).context("inserting submission"),
            }
        };
        if outcome == WriteOutcome::Written {
            self.broadcast(UgcEvent::SubmissionChanged {
                submission: Box::new(s.clone()),
            });
        }
        Ok(outcome)
    }

    /// Rewrite a submission's correctable fields (platform / post url / parsed
    /// post id). Status is deliberately NOT touched here — review is its own
    /// endpoint so an edit can never silently approve a post.
    ///
    /// An edit that collides with an existing post in the same campaign returns
    /// [`WriteOutcome::DuplicatePost`].
    pub async fn update_submission(&self, s: &Submission) -> Result<WriteOutcome> {
        let outcome = {
            let conn = self.conn.lock().await;
            let res = conn.execute(
                "UPDATE submissions SET
                   platform = ?2, post_url = ?3, external_post_id = ?4, updated_at = ?5,
                   dedup_key = ?6
                 WHERE id = ?1",
                params![
                    s.id,
                    s.platform,
                    s.post_url,
                    s.external_post_id,
                    s.updated_at,
                    submission_dedup_key(&s.post_url),
                ],
            );
            match res {
                Ok(0) => WriteOutcome::NotFound,
                Ok(_) => WriteOutcome::Written,
                Err(err) if is_constraint_violation(&err) => WriteOutcome::DuplicatePost,
                Err(err) => return Err(err).context("updating submission"),
            }
        };
        if outcome == WriteOutcome::Written {
            self.broadcast(UgcEvent::SubmissionChanged {
                submission: Box::new(s.clone()),
            });
        }
        Ok(outcome)
    }

    /// Move a submission to a new review state, stamping `reviewed_at` and
    /// `rejection_reason` as given. Returns the stored row, so the caller can
    /// gate its hook event on the transition it actually observed.
    pub async fn set_submission_status(
        &self,
        id: &str,
        status: SubmissionStatus,
        reviewed_at: Option<&str>,
        rejection_reason: Option<&str>,
        updated_at: &str,
    ) -> Result<Option<Submission>> {
        {
            let conn = self.conn.lock().await;
            let n = conn
                .execute(
                    "UPDATE submissions SET
                       status = ?2, reviewed_at = ?3, rejection_reason = ?4, updated_at = ?5
                     WHERE id = ?1",
                    params![
                        id,
                        status.as_str(),
                        reviewed_at,
                        rejection_reason,
                        updated_at
                    ],
                )
                .context("updating submission status")?;
            if n == 0 {
                return Ok(None);
            }
        }
        let updated = self.get_submission(id).await?;
        if let Some(s) = &updated {
            self.broadcast(UgcEvent::SubmissionChanged {
                submission: Box::new(s.clone()),
            });
        }
        Ok(updated)
    }

    /// Fetch a submission by id.
    pub async fn get_submission(&self, id: &str) -> Result<Option<Submission>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {SUBMISSION_COLUMNS} FROM submissions WHERE id = ?1");
        let row = conn
            .query_row(&sql, params![id], map_submission)
            .optional()
            .context("reading submission")?;
        Ok(row)
    }

    /// Find a submission by the post it points at — the pre-check the create
    /// endpoint uses so a 409 can name the existing row.
    pub async fn find_submission_by_post(
        &self,
        campaign_id: &str,
        platform: &str,
        external_post_id: &str,
    ) -> Result<Option<Submission>> {
        if external_post_id.trim().is_empty() {
            // Unparseable-URL rows are excluded from the unique index on purpose,
            // so "" can never identify one post.
            return Ok(None);
        }
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {SUBMISSION_COLUMNS} FROM submissions
             WHERE campaign_id = ?1 AND platform = ?2 AND external_post_id = ?3"
        );
        let row = conn
            .query_row(
                &sql,
                params![campaign_id, platform, external_post_id],
                map_submission,
            )
            .optional()
            .context("looking up submission by post")?;
        Ok(row)
    }

    /// List submissions newest-first, every filter field ANDed in SQL.
    pub async fn list_submissions(&self, filter: &SubmissionFilter) -> Result<Vec<Submission>> {
        // No alias: `submissions` is the only table in this query.
        let (where_sql, args) = submission_where(filter, None);
        let sql = format!(
            "SELECT {SUBMISSION_COLUMNS} FROM submissions{where_sql}
             ORDER BY submitted_at DESC, id DESC LIMIT {}",
            clamp_limit(filter.limit)
        );
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(args.iter()), map_submission)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// The same list with each row's latest snapshot **and its payout money**
    /// joined on.
    ///
    /// The correlated `MAX(id)` sub-select is what rides
    /// `idx_metric_snapshots_submission` — the index exists precisely so
    /// "latest per submission" is a seek and not a scan.
    ///
    /// The payouts join needs no such sub-select: `idx_payouts_submission` is
    /// UNIQUE on `submission_id`, so a submission has at most one payout row and
    /// the join cannot fan a submission out into duplicate rows. Campaign-level
    /// rows (`submission_id IS NULL`) never match `p.submission_id = s.id`, so
    /// they stay out of the per-post figure — they belong to the campaign's
    /// spend, not to any one post.
    ///
    /// Without this join the panel's "Accrued" column is permanently blank: only
    /// the single-submission read used to carry a payout, and re-fetching one per
    /// row is a query per row.
    pub async fn list_submissions_with_metrics(
        &self,
        filter: &SubmissionFilter,
    ) -> Result<Vec<SubmissionWithMetrics>> {
        let (where_sql, args) = submission_where(filter, Some("s"));
        let sql = format!(
            "SELECT {},
                    ms.id, ms.submission_id, ms.captured_at, ms.views, ms.likes,
                    ms.comments, ms.shares, ms.saves, ms.source,
                    p.amount_cents, p.status
             FROM submissions s
             LEFT JOIN metric_snapshots ms
                    ON ms.id = (SELECT MAX(id) FROM metric_snapshots
                                 WHERE submission_id = s.id)
             LEFT JOIN payouts p ON p.submission_id = s.id{}
             ORDER BY s.submitted_at DESC, s.id DESC LIMIT {}",
            prefixed(SUBMISSION_COLUMNS, "s"),
            where_sql,
            clamp_limit(filter.limit)
        );
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(args.iter()), |row| {
            let submission = map_submission(row)?;
            // A LEFT JOIN with no match yields NULL in the snapshot's id column.
            let latest = match row.get::<_, Option<i64>>(12)? {
                Some(_) => Some(map_snapshot_at(row, 12)?),
                None => None,
            };
            // Same story for the payout: both columns are NOT NULL in the table,
            // so a NULL here means "no payout row", never "a payout worth 0".
            let accrued_cents = row.get::<_, Option<i64>>(21)?;
            let payout_status = row.get::<_, Option<String>>(22)?;
            Ok(SubmissionWithMetrics {
                submission,
                latest,
                accrued_cents,
                payout_status,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Delete a submission plus its snapshots and its payout row. Refused with
    /// [`DeleteOutcome::PaidPayoutsRetained`] (nothing changed) once that payout
    /// is `paid` — removing it would corrupt the campaign's spend. The check runs
    /// in the delete's own transaction, so it cannot race a mark-paid.
    pub async fn delete_submission(&self, id: &str) -> Result<DeleteOutcome> {
        let outcome = {
            let mut conn = self.conn.lock().await;
            let tx = conn.transaction().context("starting submission delete")?;
            let paid: i64 = tx.query_row(
                "SELECT COUNT(*) FROM payouts WHERE submission_id = ?1 AND status = 'paid'",
                params![id],
                |row| row.get(0),
            )?;
            if paid > 0 {
                return Ok(DeleteOutcome::PaidPayoutsRetained { paid });
            }
            tx.execute(
                "DELETE FROM metric_snapshots WHERE submission_id = ?1",
                params![id],
            )?;
            tx.execute(
                "DELETE FROM payouts WHERE submission_id = ?1 AND status <> 'paid'",
                params![id],
            )?;
            let n = tx.execute("DELETE FROM submissions WHERE id = ?1", params![id])?;
            tx.commit().context("committing submission delete")?;
            if n > 0 {
                DeleteOutcome::Deleted
            } else {
                DeleteOutcome::NotFound
            }
        };
        if outcome == DeleteOutcome::Deleted {
            self.broadcast(UgcEvent::Deleted {
                entity: "submission".to_string(),
                id: id.to_string(),
            });
        }
        Ok(outcome)
    }

    // ---- metric snapshots -------------------------------------------------

    /// Append a snapshot, returning its generated id.
    pub async fn insert_snapshot(&self, m: &MetricSnapshot) -> Result<i64> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO metric_snapshots
               (submission_id, captured_at, views, likes, comments, shares, saves, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                m.submission_id,
                m.captured_at,
                m.views,
                m.likes,
                m.comments,
                m.shares,
                m.saves,
                m.source.as_str(),
            ],
        )
        .context("inserting metric snapshot")?;
        Ok(conn.last_insert_rowid())
    }

    /// The submission's most recent counters — the figure every payout is priced
    /// against.
    pub async fn latest_snapshot(&self, submission_id: &str) -> Result<Option<MetricSnapshot>> {
        let conn = self.conn.lock().await;
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM metric_snapshots
             WHERE submission_id = ?1 ORDER BY id DESC LIMIT 1"
        );
        let row = conn
            .query_row(&sql, params![submission_id], |r| map_snapshot_at(r, 0))
            .optional()
            .context("reading latest snapshot")?;
        Ok(row)
    }

    /// A submission's metric history, newest first.
    pub async fn list_snapshots(
        &self,
        submission_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MetricSnapshot>> {
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM metric_snapshots
             WHERE submission_id = ?1 ORDER BY id DESC LIMIT {}",
            clamp_limit(limit)
        );
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![submission_id], |r| map_snapshot_at(r, 0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    // ---- payouts ----------------------------------------------------------

    /// Insert or re-price a payout row. Re-pricing reuses the existing row's id
    /// (read it back with [`Self::payout_for_submission`] first) — inserting a
    /// second row for the same submission is rejected by
    /// `idx_payouts_submission`, which is what keeps a re-refresh from doubling
    /// the campaign's spend.
    ///
    /// Re-pricing never touches a row that has moved past `accrued`: the
    /// conflict update is guarded on the stored status, so an approve or paid
    /// transition that lands while an accrual pass is computing is kept rather
    /// than reverted to `accrued` (the same SQL-side guard
    /// [`Self::delete_unpaid_payout_for_submission`] uses). Returns `false` when
    /// that guard skipped the write; nothing is broadcast then.
    pub async fn upsert_payout(&self, p: &Payout) -> Result<bool> {
        {
            let conn = self.conn.lock().await;
            let written = conn
                .execute(
                    "INSERT INTO payouts
                   (id, campaign_id, creator_id, submission_id, amount_cents, status, reason,
                    accrued_at, approved_at, paid_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(id) DO UPDATE SET
                   amount_cents = ?5, status = ?6, reason = ?7,
                   approved_at = ?9, paid_at = ?10, updated_at = ?12
                 WHERE payouts.status = 'accrued'",
                    params![
                        p.id,
                        p.campaign_id,
                        p.creator_id,
                        p.submission_id,
                        p.amount_cents,
                        p.status.as_str(),
                        p.reason,
                        p.accrued_at,
                        p.approved_at,
                        p.paid_at,
                        p.created_at,
                        p.updated_at,
                    ],
                )
                .context("upserting payout")?;
            if written == 0 {
                return Ok(false);
            }
        }
        self.broadcast(UgcEvent::PayoutChanged {
            payout: Box::new(p.clone()),
        });
        Ok(true)
    }

    /// Fetch a payout by id.
    pub async fn get_payout(&self, id: &str) -> Result<Option<Payout>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {PAYOUT_COLUMNS} FROM payouts WHERE id = ?1");
        let row = conn
            .query_row(&sql, params![id], map_payout)
            .optional()
            .context("reading payout")?;
        Ok(row)
    }

    /// The payout row attached to a submission, if the accrual pass made one.
    pub async fn payout_for_submission(&self, submission_id: &str) -> Result<Option<Payout>> {
        let conn = self.conn.lock().await;
        let sql = format!("SELECT {PAYOUT_COLUMNS} FROM payouts WHERE submission_id = ?1");
        let row = conn
            .query_row(&sql, params![submission_id], map_payout)
            .optional()
            .context("reading submission payout")?;
        Ok(row)
    }

    /// List payouts newest-first, every filter field ANDed in SQL.
    pub async fn list_payouts(&self, filter: &PayoutFilter) -> Result<Vec<Payout>> {
        let mut clauses: Vec<&str> = Vec::new();
        let mut args: Vec<String> = Vec::new();
        if let Some(v) = filter.campaign_id.as_deref() {
            clauses.push("campaign_id = ?");
            args.push(v.to_string());
        }
        if let Some(v) = filter.creator_id.as_deref() {
            clauses.push("creator_id = ?");
            args.push(v.to_string());
        }
        if let Some(v) = filter.status {
            clauses.push("status = ?");
            args.push(v.as_str().to_string());
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT {PAYOUT_COLUMNS} FROM payouts{where_sql}
             ORDER BY accrued_at DESC, id DESC LIMIT {}",
            clamp_limit(filter.limit)
        );
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(args.iter()), map_payout)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Move a payout to a new state, stamping the matching timestamp. Returns
    /// the stored row so the caller can gate on the transition it observed.
    ///
    /// This does **not** enforce the `accrued -> approved -> paid` ladder — the
    /// store stays dumb, the same way `MonitorStore` does. Refusing to mark an
    /// `accrued` row paid (money never skips approval) and no-opping a repeated
    /// approve belong to the API layer, which owns the 409 and the hook event.
    pub async fn set_payout_status(
        &self,
        id: &str,
        status: PayoutStatus,
        stamp: &str,
    ) -> Result<Option<Payout>> {
        {
            let conn = self.conn.lock().await;
            // Only the column belonging to the target state is written, so an
            // approve never clears a paid stamp and vice versa.
            let sql = match status {
                PayoutStatus::Accrued => {
                    "UPDATE payouts SET status = ?2, updated_at = ?3 WHERE id = ?1"
                }
                PayoutStatus::Approved => {
                    "UPDATE payouts SET status = ?2, approved_at = ?3, updated_at = ?3 WHERE id = ?1"
                }
                PayoutStatus::Paid => {
                    "UPDATE payouts SET status = ?2, paid_at = ?3, updated_at = ?3 WHERE id = ?1"
                }
            };
            let n = conn
                .execute(sql, params![id, status.as_str(), stamp])
                .context("updating payout status")?;
            if n == 0 {
                return Ok(None);
            }
        }
        let updated = self.get_payout(id).await?;
        if let Some(p) = &updated {
            self.broadcast(UgcEvent::PayoutChanged {
                payout: Box::new(p.clone()),
            });
        }
        Ok(updated)
    }

    /// Drop a submission's payout row **unless it has already been paid**. The
    /// reject path calls this: un-accruing money is fine, un-paying it is not.
    /// Returns true when a row went.
    pub async fn delete_unpaid_payout_for_submission(&self, submission_id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn
            .execute(
                "DELETE FROM payouts WHERE submission_id = ?1 AND status <> 'paid'",
                params![submission_id],
            )
            .context("deleting unpaid payout")?;
        Ok(n > 0)
    }

    /// Money committed to a campaign — **every** payout row, whatever its state,
    /// because accrued money is already promised and the budget is spent against
    /// it. `exclude_submission_id` skips the row currently being re-priced.
    pub async fn campaign_committed_cents(
        &self,
        campaign_id: &str,
        exclude_submission_id: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock().await;
        let cents = conn
            .query_row(
                "SELECT COALESCE(SUM(amount_cents), 0) FROM payouts
                 WHERE campaign_id = ?1
                   AND (?2 IS NULL OR submission_id IS NULL OR submission_id <> ?2)",
                params![campaign_id, exclude_submission_id],
                |row| row.get::<_, i64>(0),
            )
            .context("summing campaign payouts")?;
        Ok(cents)
    }

    /// The same sum scoped to one creator on one campaign — the per-creator cap's
    /// input.
    pub async fn creator_committed_cents(
        &self,
        campaign_id: &str,
        creator_id: &str,
        exclude_submission_id: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock().await;
        let cents = conn
            .query_row(
                "SELECT COALESCE(SUM(amount_cents), 0) FROM payouts
                 WHERE campaign_id = ?1 AND creator_id = ?2
                   AND (?3 IS NULL OR submission_id IS NULL OR submission_id <> ?3)",
                params![campaign_id, creator_id, exclude_submission_id],
                |row| row.get::<_, i64>(0),
            )
            .context("summing creator payouts")?;
        Ok(cents)
    }

    /// Everything the accrual pass needs for one submission, in one round-trip.
    /// `None` when the submission (or its campaign) is gone.
    ///
    /// **Read one submission, write its payout, then read the next.** The two
    /// `_committed_excluding_this` sums are a snapshot of the money committed at
    /// call time, so a campaign-wide refresh that gathered every submission's
    /// inputs up front and only then wrote them would price each post against
    /// the same stale total and overshoot `budget_cents` by every post but one.
    pub async fn accrual_inputs(&self, submission_id: &str) -> Result<Option<AccrualInputs>> {
        let Some(submission) = self.get_submission(submission_id).await? else {
            return Ok(None);
        };
        let Some(campaign) = self.get_campaign(&submission.campaign_id).await? else {
            return Ok(None);
        };
        let latest = self.latest_snapshot(submission_id).await?;
        let existing_payout = self.payout_for_submission(submission_id).await?;
        let creator_committed_excluding_this = self
            .creator_committed_cents(
                &submission.campaign_id,
                &submission.creator_id,
                Some(submission_id),
            )
            .await?;
        let campaign_committed_excluding_this = self
            .campaign_committed_cents(&submission.campaign_id, Some(submission_id))
            .await?;
        Ok(Some(AccrualInputs {
            submission,
            campaign,
            latest,
            existing_payout,
            creator_committed_excluding_this,
            campaign_committed_excluding_this,
        }))
    }

    // ---- derived reads ----------------------------------------------------

    /// Spend vs budget for one campaign.
    pub async fn campaign_summary(&self, campaign_id: &str) -> Result<Option<CampaignSummary>> {
        let Some(campaign) = self.get_campaign(campaign_id).await? else {
            return Ok(None);
        };
        let conn = self.conn.lock().await;

        let mut submissions = SubmissionCounts::default();
        {
            let mut stmt = conn.prepare(
                "SELECT status, COUNT(*) FROM submissions WHERE campaign_id = ?1 GROUP BY status",
            )?;
            let rows = stmt.query_map(params![campaign_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, count) = row?;
                submissions.add(SubmissionStatus::from_db(&status), count);
            }
        }

        // Counters come from each submission's LATEST snapshot, never the sum of
        // every snapshot — otherwise a refreshed post counts its views twice.
        let (total_views, total_likes, total_comments) = conn
            .query_row(
                "SELECT COALESCE(SUM(ms.views), 0),
                        COALESCE(SUM(ms.likes), 0),
                        COALESCE(SUM(ms.comments), 0)
                 FROM submissions s
                 JOIN metric_snapshots ms
                   ON ms.id = (SELECT MAX(id) FROM metric_snapshots WHERE submission_id = s.id)
                 WHERE s.campaign_id = ?1",
                params![campaign_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .context("summing campaign metrics")?;

        let (mut accrued_cents, mut approved_cents, mut paid_cents) = (0i64, 0i64, 0i64);
        {
            let mut stmt = conn.prepare(
                "SELECT status, COALESCE(SUM(amount_cents), 0) FROM payouts
                 WHERE campaign_id = ?1 GROUP BY status",
            )?;
            let rows = stmt.query_map(params![campaign_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, cents) = row?;
                match PayoutStatus::from_db(&status) {
                    PayoutStatus::Accrued => accrued_cents += cents,
                    PayoutStatus::Approved => approved_cents += cents,
                    PayoutStatus::Paid => paid_cents += cents,
                }
            }
        }

        let creators = conn
            .query_row(
                "SELECT COUNT(DISTINCT creator_id) FROM submissions WHERE campaign_id = ?1",
                params![campaign_id],
                |row| row.get::<_, i64>(0),
            )
            .context("counting campaign creators")?;

        let committed_cents = accrued_cents + approved_cents + paid_cents;
        let remaining_cents = if campaign.budget_cents > 0 {
            Some((campaign.budget_cents - committed_cents).max(0))
        } else {
            // Uncapped: `None` so the panel shows "unlimited", not "0 left".
            None
        };

        Ok(Some(CampaignSummary {
            budget_cents: campaign.budget_cents,
            accrued_cents,
            approved_cents,
            paid_cents,
            committed_cents,
            remaining_cents,
            total_views,
            total_likes,
            total_comments,
            submissions,
            creators,
        }))
    }

    /// Creators on one campaign, ranked by latest-snapshot views.
    ///
    /// Payout money is aggregated in a **second** query and merged in Rust:
    /// joining `payouts` into the views `GROUP BY` would fan the rows out and
    /// multiply the view total.
    ///
    /// The sort keys after `views` are not decoration — `ORDER BY views DESC`
    /// alone leaves tied creators in whatever order SQLite happens to emit, so
    /// the list would shuffle between reloads.
    pub async fn campaign_leaderboard(
        &self,
        campaign_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<LeaderboardRow>> {
        let conn = self.conn.lock().await;

        let mut money: BTreeMap<String, (i64, i64)> = BTreeMap::new();
        {
            let mut stmt = conn.prepare(
                "SELECT creator_id, status, COALESCE(SUM(amount_cents), 0) FROM payouts
                 WHERE campaign_id = ?1 GROUP BY creator_id, status",
            )?;
            let rows = stmt.query_map(params![campaign_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            for row in rows {
                let (creator_id, status, cents) = row?;
                let entry = money.entry(creator_id).or_insert((0, 0));
                match PayoutStatus::from_db(&status) {
                    PayoutStatus::Accrued => entry.0 += cents,
                    PayoutStatus::Paid => entry.1 += cents,
                    PayoutStatus::Approved => {}
                }
            }
        }

        let sql = format!(
            "SELECT s.creator_id,
                    COALESCE(ryu_open('creators.display_name', c.id, c.display_name), '') AS creator_name,
                    COALESCE(SUM(latest.views), 0) AS views,
                    SUM(CASE WHEN s.status IN ('approved', 'paid') THEN 1 ELSE 0 END)
             FROM submissions s
             LEFT JOIN creators c ON c.id = s.creator_id
             LEFT JOIN metric_snapshots latest
                    ON latest.id = (SELECT MAX(id) FROM metric_snapshots
                                     WHERE submission_id = s.id)
             WHERE s.campaign_id = ?1
             GROUP BY s.creator_id
             ORDER BY views DESC, creator_name COLLATE NOCASE ASC,
                      s.creator_id ASC
             LIMIT {}",
            clamp_limit(limit)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![campaign_id], |row| {
            Ok(LeaderboardRow {
                creator_id: row.get(0)?,
                display_name: {
                    let name: String = row.get(1)?;
                    if name == crate::seal_sql::UNREADABLE {
                        String::new()
                    } else {
                        name
                    }
                },
                views: row.get(2)?,
                approved_submissions: row.get(3)?,
                accrued_cents: 0,
                paid_cents: 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            let mut row = row?;
            if let Some((accrued, paid)) = money.get(&row.creator_id) {
                row.accrued_cents = *accrued;
                row.paid_cents = *paid;
            }
            out.push(row);
        }
        Ok(out)
    }

    /// The dock panel's first paint: counts plus money, across everything.
    pub async fn overview(&self) -> Result<UgcOverview> {
        let campaigns = self.count_campaigns().await?;
        let creators = self.count_creators().await?;
        let conn = self.conn.lock().await;
        let mut submissions = SubmissionCounts::default();
        {
            let mut stmt =
                conn.prepare("SELECT status, COUNT(*) FROM submissions GROUP BY status")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, count) = row?;
                submissions.add(SubmissionStatus::from_db(&status), count);
            }
        }
        let (mut accrued_cents, mut paid_cents) = (0i64, 0i64);
        {
            let mut stmt = conn.prepare(
                "SELECT status, COALESCE(SUM(amount_cents), 0) FROM payouts GROUP BY status",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, cents) = row?;
                match PayoutStatus::from_db(&status) {
                    PayoutStatus::Accrued => accrued_cents += cents,
                    PayoutStatus::Paid => paid_cents += cents,
                    PayoutStatus::Approved => {}
                }
            }
        }
        Ok(UgcOverview {
            campaigns,
            creators,
            submissions,
            accrued_cents,
            paid_cents,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Row mappers + small SQL helpers
// ─────────────────────────────────────────────────────────────────────────────

/// `PRAGMA user_version` once submission identities (`dedup_key`, and
/// `external_post_id` re-derived under the host-gated parser) are current.
const SUBMISSION_IDENTITY_VERSION: i64 = 2;

/// The per-campaign identity of the post a submission's URL names: never empty,
/// and derived from the URL ALONE.
///
/// `id:<platform>:<post id>` when the URL's host is a curated platform and the
/// URL is that platform's post shape ([`crate::api::post_identity`]), so two
/// spellings of one post — `watch?v=X` and `youtu.be/X` — collide; else
/// `url:<canonical url>`, so an unparseable link submitted twice collides too.
///
/// The caller's platform LABEL is deliberately not an input: it is free text, and
/// keying on it let the same link be resubmitted (and paid) once per label.
#[must_use]
pub fn submission_dedup_key(post_url: &str) -> String {
    let trimmed = post_url.trim();
    if let Some((platform, id)) = url::Url::parse(trimmed)
        .ok()
        .and_then(|u| crate::api::post_identity(&u))
    {
        return format!("id:{platform}:{id}");
    }
    format!("url:{}", canonical_post_url(trimmed))
}

/// Query parameters that only track how a link was shared and never select a
/// different post. Everything else in the query is kept: it can be the post's
/// identity (YouTube's `v=`).
const TRACKING_QUERY_PARAMS: &[&str] = &[
    "fbclid", "gclid", "igsh", "igshid", "si", "feature", "ref_src", "ref_url",
];

/// Normalise a screened post URL for deduplication: drop the fragment, the
/// default port, a trailing dot / `www.` / `m.` on the host, trailing path
/// slashes and share-tracking parameters, and sort the remaining query pairs.
/// Path and query values keep their case — post ids are case-sensitive. An
/// unparseable string (only legacy rows can hold one) is used trimmed as-is.
#[must_use]
pub fn canonical_post_url(raw: &str) -> String {
    let trimmed = raw.trim();
    let Ok(mut url) = url::Url::parse(trimmed) else {
        return trimmed.to_string();
    };
    url.set_fragment(None);
    let _ = url.set_port(None);
    if let Some(host) = url.host_str() {
        let mut host = host.trim_end_matches('.').to_ascii_lowercase();
        for alias in ["www.", "m."] {
            if let Some(rest) = host.strip_prefix(alias) {
                host = rest.to_string();
            }
        }
        let _ = url.set_host(Some(&host));
    }
    // Empty segments go (a trailing slash, or `//post/abc` for `/post/abc`), and
    // percent-escapes of unreserved characters are decoded (`%61bc` is `abc`,
    // RFC 3986 §6.2.2.2) so a respelling of one path is one key.
    let segments: Vec<String> = url
        .path_segments()
        .map(|s| {
            s.filter(|p| !p.is_empty())
                .map(normalize_percent_escapes)
                .collect()
        })
        .unwrap_or_default();
    url.set_path(&format!("/{}", segments.join("/")));
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| {
            let key = k.to_ascii_lowercase();
            !key.is_empty()
                && !key.starts_with("utm_")
                && !TRACKING_QUERY_PARAMS.contains(&key.as_str())
        })
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    if pairs.is_empty() {
        url.set_query(None);
    } else {
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url.to_string()
}

/// Decode percent-escapes of RFC 3986 unreserved characters and upper-case the
/// hex of every other escape, so equivalent spellings of a path segment compare
/// equal without changing which resource it names.
fn normalize_percent_escapes(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = (
                char::from(bytes[i + 1]).to_digit(16),
                char::from(bytes[i + 2]).to_digit(16),
            );
            if let (Some(hi), Some(lo)) = hex {
                let decoded = char::from(u8::try_from(hi * 16 + lo).unwrap_or(0));
                if decoded.is_ascii_alphanumeric() || matches!(decoded, '-' | '.' | '_' | '~') {
                    out.push(decoded);
                } else {
                    out.push('%');
                    out.push(char::from(bytes[i + 1]).to_ascii_uppercase());
                    out.push(char::from(bytes[i + 2]).to_ascii_uppercase());
                }
                i += 3;
                continue;
            }
        }
        out.push(char::from(bytes[i]));
        i += 1;
    }
    out
}

/// Is this the unique-index rejection that means "already submitted"? rusqlite
/// surfaces it as a `SqliteFailure` with `ConstraintViolation`; anything else is
/// a real error and is propagated.
fn is_constraint_violation(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(e, _) if e.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

/// Qualify a comma-separated column list with a table alias, so the joined query
/// and the plain one can share `SUBMISSION_COLUMNS` and never drift.
fn prefixed(columns: &str, alias: &str) -> String {
    columns
        .split(',')
        .map(|c| format!("{alias}.{}", c.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build the `WHERE` fragment + bound values for a [`SubmissionFilter`],
/// qualifying every column with `alias` when the caller joins other tables.
///
/// The joined read **must** pass an alias. `payouts` carries `campaign_id`,
/// `creator_id` and `status` of its own, so a bare `status = ?` is ambiguous the
/// moment that table is in the query and SQLite refuses the whole statement —
/// it is not a filter that quietly reads the wrong column, it is a hard error.
/// (`metric_snapshots` shares none of these names, which is why the join got
/// away with unqualified columns before payouts joined it.)
fn submission_where(filter: &SubmissionFilter, alias: Option<&str>) -> (String, Vec<String>) {
    let col = |name: &str| match alias {
        Some(a) => format!("{a}.{name} = ?"),
        None => format!("{name} = ?"),
    };
    let mut clauses: Vec<String> = Vec::new();
    let mut args: Vec<String> = Vec::new();
    if let Some(v) = filter.campaign_id.as_deref() {
        clauses.push(col("campaign_id"));
        args.push(v.to_string());
    }
    if let Some(v) = filter.creator_id.as_deref() {
        clauses.push(col("creator_id"));
        args.push(v.to_string());
    }
    if let Some(v) = filter.status {
        clauses.push(col("status"));
        args.push(v.as_str().to_string());
    }
    if let Some(v) = filter.platform.as_deref() {
        clauses.push(col("platform"));
        args.push(v.trim().to_ascii_lowercase());
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    (where_sql, args)
}

fn map_submission(row: &rusqlite::Row) -> rusqlite::Result<Submission> {
    Ok(Submission {
        id: row.get(0)?,
        campaign_id: row.get(1)?,
        creator_id: row.get(2)?,
        platform: row.get(3)?,
        post_url: row.get(4)?,
        external_post_id: row.get(5)?,
        status: SubmissionStatus::from_db(&row.get::<_, String>(6)?),
        submitted_at: row.get(7)?,
        reviewed_at: row.get(8)?,
        rejection_reason: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

/// Map a snapshot whose columns start at `base` — `0` for a plain read, `12` for
/// the submission-joined one.
fn map_snapshot_at(row: &rusqlite::Row, base: usize) -> rusqlite::Result<MetricSnapshot> {
    Ok(MetricSnapshot {
        id: row.get(base)?,
        submission_id: row.get(base + 1)?,
        captured_at: row.get(base + 2)?,
        views: row.get(base + 3)?,
        likes: row.get(base + 4)?,
        comments: row.get(base + 5)?,
        shares: row.get(base + 6)?,
        saves: row.get(base + 7)?,
        source: MetricSource::from_db(&row.get::<_, String>(base + 8)?),
    })
}

fn map_payout(row: &rusqlite::Row) -> rusqlite::Result<Payout> {
    Ok(Payout {
        id: row.get(0)?,
        campaign_id: row.get(1)?,
        creator_id: row.get(2)?,
        submission_id: row.get(3)?,
        amount_cents: row.get(4)?,
        status: PayoutStatus::from_db(&row.get::<_, String>(5)?),
        reason: row.get(6)?,
        accrued_at: row.get(7)?,
        approved_at: row.get(8)?,
        paid_at: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path() -> PathBuf {
        // One directory per store: the standalone sealing root lives beside it.
        std::env::temp_dir()
            .join(format!("ryu-ugc-test-{}", uuid::Uuid::new_v4().simple()))
            .join("ugc.db")
    }

    fn keys(n: u8) -> PluginKeys {
        use base64::Engine as _;
        PluginKeys::sealing(
            ryu_crypto::PluginCipher::from_subkey_b64(
                PLUGIN_ID,
                &base64::engine::general_purpose::STANDARD.encode([n; 32]),
            )
            .unwrap(),
            crate::seal_keys::KeySource::Delivered,
        )
    }

    fn disk_bytes(path: &std::path::Path) -> Vec<u8> {
        let mut bytes = std::fs::read(path).unwrap_or_default();
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        bytes.extend(std::fs::read(PathBuf::from(wal)).unwrap_or_default());
        bytes
    }

    fn on_disk(path: &std::path::Path, needle: &str) -> bool {
        disk_bytes(path)
            .windows(needle.len())
            .any(|w| w == needle.as_bytes())
    }

    fn secret_creator(id: &str) -> Creator {
        let mut c = creator(id, "Zebra Creator");
        c.contact_email = Some("quokka-inbox@example.test".into());
        c.payout_handle = Some("paypal:wombat-payout-marker".into());
        c.notes = Some("numbat-notes-marker".into());
        c
    }

    const NEEDLES: [&str; 4] = [
        "Zebra Creator",
        "quokka-inbox",
        "wombat-payout-marker",
        "numbat-notes-marker",
    ];

    #[tokio::test]
    async fn creators_are_sealed_on_disk_and_the_roster_still_searches() {
        let path = temp_path();
        let store = UgcStore::open_with_keys(path.clone(), &keys(1)).unwrap();
        store.upsert_creator(&secret_creator("c1")).await.unwrap();
        store.upsert_creator(&creator("c2", "adam")).await.unwrap();
        let roster = store.list_creators(None).await.unwrap();
        assert_eq!(
            roster.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            ["c2", "c1"]
        );
        assert_eq!(
            store.list_creators(Some("QUOKKA")).await.unwrap()[0].id,
            "c1"
        );
        assert_eq!(store.list_creators(Some("zebra")).await.unwrap().len(), 1);
        let got = store.get_creator("c1").await.unwrap().unwrap();
        assert_eq!(got.notes.as_deref(), Some("numbat-notes-marker"));
        drop(store);
        for needle in NEEDLES {
            assert!(!on_disk(&path, needle), "{needle} on disk");
        }
        let conn = Connection::open(&path).unwrap();
        let cipher = keys(1).content_cipher(STORE).unwrap();
        ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap())
            .unwrap();
        let indexes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN ('idx_creators_name', 'idx_creators_email')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 0);
    }

    #[tokio::test]
    async fn legacy_creators_are_sealed_once_and_still_read() {
        let path = temp_path();
        {
            // A pre-sealing store: plaintext rows and the old email index.
            let store = UgcStore::open_with_keys(path.clone(), &PluginKeys::compat()).unwrap();
            store.upsert_creator(&secret_creator("c1")).await.unwrap();
            store
                .conn
                .lock()
                .await
                .execute_batch("CREATE INDEX idx_creators_email ON creators(contact_email);")
                .unwrap();
        }
        assert!(on_disk(&path, "numbat-notes-marker"));
        let store = UgcStore::open_with_keys(path.clone(), &keys(2)).unwrap();
        assert_eq!(
            store.get_creator("c1").await.unwrap().unwrap().display_name,
            "Zebra Creator"
        );
        drop(store);
        for needle in NEEDLES {
            assert!(!on_disk(&path, needle), "{needle} on disk after migration");
        }
        let raw = |path: &std::path::Path| -> String {
            Connection::open(path)
                .unwrap()
                .query_row("SELECT json FROM creators", [], |row| row.get(0))
                .unwrap()
        };
        let first = raw(&path);
        drop(UgcStore::open_with_keys(path.clone(), &keys(2)).unwrap());
        assert_eq!(raw(&path), first);
    }

    #[tokio::test]
    async fn a_copied_creator_is_skipped_and_other_keys_refuse_writes() {
        let path = temp_path();
        let store = UgcStore::open_with_keys(path.clone(), &keys(3)).unwrap();
        store.upsert_creator(&secret_creator("c1")).await.unwrap();
        store.upsert_creator(&creator("c2", "adam")).await.unwrap();
        store
            .conn
            .lock()
            .await
            .execute(
                "UPDATE creators SET json = (SELECT json FROM creators WHERE id = 'c1') WHERE id = 'c2'",
                [],
            )
            .unwrap();
        let roster = store.list_creators(None).await.unwrap();
        assert_eq!(roster.len(), 1);
        assert!(store.get_creator("c2").await.is_err());
        assert_eq!(store.seal_health()["unreadable_rows"], 2);
        drop(store);
        for other in [keys(4), PluginKeys::compat()] {
            let wrong = UgcStore::open_with_keys(path.clone(), &other).unwrap();
            assert_eq!(wrong.seal_health()["state"], "unhealthy");
            assert!(wrong.upsert_creator(&creator("c3", "eve")).await.is_err());
        }
    }

    fn temp_store() -> UgcStore {
        UgcStore::open(temp_path()).expect("open temp store")
    }

    fn now() -> String {
        chrono::Utc::now().to_rfc3339()
    }

    fn campaign(id: &str, payout: PayoutRule) -> Campaign {
        Campaign {
            id: id.into(),
            brand: "Acme".into(),
            brief: "post a clip".into(),
            status: CampaignStatus::Active,
            platforms: vec!["youtube".into()],
            required_hashtags: vec!["acme".into()],
            required_mentions: vec![],
            starts_at: None,
            ends_at: None,
            budget_cents: 0,
            payout,
            bonus_tiers: vec![],
            max_payout_per_creator_cents: 0,
            created_at: now(),
            updated_at: now(),
        }
    }

    fn creator(id: &str, name: &str) -> Creator {
        let mut handles = BTreeMap::new();
        handles.insert("youtube".to_string(), format!("@{name}"));
        Creator {
            id: id.into(),
            display_name: name.into(),
            handles,
            contact_email: Some(format!("{name}@example.test")),
            payout_handle: None,
            notes: None,
            created_at: now(),
            updated_at: now(),
        }
    }

    fn submission(id: &str, campaign_id: &str, creator_id: &str, post_id: &str) -> Submission {
        Submission {
            id: id.into(),
            campaign_id: campaign_id.into(),
            creator_id: creator_id.into(),
            platform: "youtube".into(),
            post_url: format!("https://youtu.be/{post_id}"),
            external_post_id: post_id.into(),
            status: SubmissionStatus::Pending,
            submitted_at: now(),
            reviewed_at: None,
            rejection_reason: None,
            created_at: now(),
            updated_at: now(),
        }
    }

    fn snapshot(submission_id: &str, views: i64) -> MetricSnapshot {
        MetricSnapshot {
            id: 0,
            submission_id: submission_id.into(),
            captured_at: now(),
            views,
            likes: 10,
            comments: 2,
            shares: 1,
            saves: 0,
            source: MetricSource::Composio,
        }
    }

    fn payout(
        id: &str,
        campaign_id: &str,
        creator_id: &str,
        sub: Option<&str>,
        cents: i64,
    ) -> Payout {
        Payout {
            id: id.into(),
            campaign_id: campaign_id.into(),
            creator_id: creator_id.into(),
            submission_id: sub.map(str::to_string),
            amount_cents: cents,
            status: PayoutStatus::Accrued,
            reason: "test".into(),
            accrued_at: now(),
            approved_at: None,
            paid_at: None,
            created_at: now(),
            updated_at: now(),
        }
    }

    // ---- schema -----------------------------------------------------------

    #[tokio::test]
    async fn schema_is_idempotent_across_reopens() {
        let path = temp_path();
        let first = UgcStore::open(path.clone()).expect("first open");
        first
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        drop(first);
        // Re-opening runs the whole CREATE ... IF NOT EXISTS batch again over a
        // populated file; it must neither error nor lose the row.
        let second = UgcStore::open(path).expect("second open");
        assert_eq!(second.count_campaigns().await.unwrap(), 1);
    }

    // ---- CRUD -------------------------------------------------------------

    #[tokio::test]
    async fn campaign_roundtrips_filters_and_cascades() {
        let store = temp_store();
        let mut c = campaign("c1", PayoutRule::Cpm { cpm_cents: 250 });
        store.upsert_campaign(&c).await.unwrap();

        let read = store.get_campaign("c1").await.unwrap().unwrap();
        assert_eq!(read.brand, "Acme");
        assert_eq!(read.payout, PayoutRule::Cpm { cpm_cents: 250 });

        // The denormalised status column is rewritten from the blob every write.
        c.status = CampaignStatus::Paused;
        c.updated_at = now();
        store.upsert_campaign(&c).await.unwrap();
        assert_eq!(store.list_campaigns(None).await.unwrap().len(), 1);
        assert!(store
            .list_campaigns(Some(CampaignStatus::Active))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .list_campaigns(Some(CampaignStatus::Paused))
                .await
                .unwrap()
                .len(),
            1
        );

        store.upsert_creator(&creator("u1", "ada")).await.unwrap();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        store.insert_snapshot(&snapshot("s1", 1000)).await.unwrap();
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 250))
            .await
            .unwrap();

        assert_eq!(
            store.delete_campaign("c1").await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(store.get_campaign("c1").await.unwrap().is_none());
        assert!(store.get_submission("s1").await.unwrap().is_none());
        assert!(store.latest_snapshot("s1").await.unwrap().is_none());
        assert!(store
            .list_payouts(&PayoutFilter::default())
            .await
            .unwrap()
            .is_empty());
        // The creator survives — deleting a campaign does not delete the roster.
        assert!(store.get_creator("u1").await.unwrap().is_some());
        assert_eq!(
            store.delete_campaign("c1").await.unwrap(),
            DeleteOutcome::NotFound
        );
    }

    #[tokio::test]
    async fn creator_search_matches_name_or_email() {
        let store = temp_store();
        store.upsert_creator(&creator("u1", "ada")).await.unwrap();
        store.upsert_creator(&creator("u2", "grace")).await.unwrap();

        assert_eq!(store.list_creators(None).await.unwrap().len(), 2);
        // Sorted by display name.
        assert_eq!(store.list_creators(None).await.unwrap()[0].id, "u1");
        assert_eq!(store.list_creators(Some("GRA")).await.unwrap().len(), 1);
        assert_eq!(
            store.list_creators(Some("grace@example")).await.unwrap()[0].id,
            "u2"
        );
        assert!(store
            .list_creators(Some("nobody"))
            .await
            .unwrap()
            .is_empty());
        // An all-whitespace query is "no filter", not "match nothing".
        assert_eq!(store.list_creators(Some("   ")).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn creator_delete_cascades_only_when_asked() {
        let store = temp_store();
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        store.upsert_creator(&creator("u1", "ada")).await.unwrap();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        store.insert_snapshot(&snapshot("s1", 500)).await.unwrap();

        assert_eq!(store.count_submissions_for_creator("u1").await.unwrap(), 1);
        // Non-cascading delete leaves the submissions behind (the API 409s first).
        assert_eq!(
            store.delete_creator("u1", false).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(store.get_submission("s1").await.unwrap().is_some());

        store.upsert_creator(&creator("u1", "ada")).await.unwrap();
        assert_eq!(
            store.delete_creator("u1", true).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(store.get_submission("s1").await.unwrap().is_none());
        assert!(store.latest_snapshot("s1").await.unwrap().is_none());
    }

    #[test]
    fn canonical_post_url_keeps_identity_and_drops_noise() {
        assert_eq!(
            canonical_post_url("https://WWW.YouTube.com:443/watch?v=AbC&feature=share&t=1#x"),
            "https://youtube.com/watch?t=1&v=AbC"
        );
        // Identity-bearing query values and path case are preserved.
        assert_ne!(
            canonical_post_url("https://youtube.com/watch?v=AbC"),
            canonical_post_url("https://youtube.com/watch?v=abc")
        );
        assert_eq!(
            canonical_post_url("https://m.youtube.com/feed/x/"),
            canonical_post_url("https://youtube.com./feed/x")
        );
    }

    /// The dedup key is a function of the URL alone: every spelling of one post
    /// maps to one key, and nothing about the caller's platform label can change
    /// it (the function does not even take one).
    #[test]
    fn submission_dedup_key_names_the_post_not_the_label() {
        for url in [
            "https://youtu.be/abc123",
            "https://www.youtube.com/watch?v=abc123",
            "https://m.youtube.com/watch?v=abc123&x=1",
            "https://youtube.com/shorts/abc123",
            "https://music.youtube.com/watch?v=abc123",
        ] {
            assert_eq!(submission_dedup_key(url), "id:youtube:abc123", "{url}");
        }
        assert_eq!(
            submission_dedup_key("https://twitter.com/a/status/99"),
            submission_dedup_key("https://x.com/b/status/99")
        );
        // A non-curated host is never mistaken for a curated platform's post...
        assert!(submission_dedup_key("https://evil.example/watch?v=abc123").starts_with("url:"));
        assert!(submission_dedup_key("https://notyoutube.com/watch?v=abc123").starts_with("url:"));
        // ...and an unparseable curated link falls back to its canonical URL.
        assert_eq!(
            submission_dedup_key("https://www.youtube.com/feed/subscriptions/#top"),
            "url:https://youtube.com/feed/subscriptions"
        );
    }

    /// A database written before `dedup_key` existed: rows are backfilled, rows
    /// that already duplicate each other are KEPT (under a legacy key), and new
    /// writes of the same link are refused from then on.
    #[tokio::test]
    async fn legacy_databases_are_backfilled_without_losing_duplicate_rows() {
        let dir = std::env::temp_dir().join(format!("ryu-ugc-legacy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(DB_FILE_NAME);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE submissions (
                     id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, creator_id TEXT NOT NULL,
                     platform TEXT NOT NULL, post_url TEXT NOT NULL,
                     external_post_id TEXT NOT NULL DEFAULT '',
                     status TEXT NOT NULL DEFAULT 'pending', submitted_at TEXT NOT NULL,
                     reviewed_at TEXT, rejection_reason TEXT,
                     created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                 CREATE UNIQUE INDEX idx_submissions_post
                     ON submissions(campaign_id, platform, external_post_id)
                     WHERE external_post_id <> '';
                 INSERT INTO submissions VALUES
                   ('s1','c1','u1','youtube','https://www.youtube.com/feed/x','','approved','t1',NULL,NULL,'t1','t1'),
                   ('s2','c1','u2','youtube','https://www.youtube.com/feed/x','','approved','t2',NULL,NULL,'t2','t2'),
                   ('s3','c1','u1','youtube','https://youtu.be/vid1','vid1','pending','t3',NULL,NULL,'t3','t3');",
            )
            .unwrap();
        }
        let store = UgcStore::open(path.clone()).unwrap();
        for id in ["s1", "s2", "s3"] {
            assert!(
                store.get_submission(id).await.unwrap().is_some(),
                "{id} kept"
            );
        }
        let mut again = submission("s4", "c1", "u3", "");
        again.post_url = "https://youtube.com/feed/x".into();
        assert_eq!(
            store.insert_submission(&again).await.unwrap(),
            WriteOutcome::DuplicatePost
        );
        assert_eq!(
            store
                .insert_submission(&submission("s5", "c1", "u3", "vid1"))
                .await
                .unwrap(),
            WriteOutcome::DuplicatePost
        );
        drop(store);
        // Re-opening is idempotent.
        let reopened = UgcStore::open(path).unwrap();
        assert!(reopened.get_submission("s2").await.unwrap().is_some());
        let conn = reopened.conn.lock().await;
        let keys: Vec<String> = conn
            .prepare("SELECT dedup_key FROM submissions ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            keys,
            vec![
                "url:https://youtube.com/feed/x".to_string(),
                "legacy:s2".to_string(),
                "id:youtube:vid1".to_string(),
            ]
        );
    }

    /// A database written by the interim per-label key (`idx_submissions_dedup`
    /// on `(campaign_id, platform, dedup_key)`): the per-label index is dropped,
    /// every key is recomputed from the URL alone, and cross-label duplicates
    /// that slipped in are KEPT under a legacy key while new writes of that post
    /// under ANY label are refused.
    #[tokio::test]
    async fn interim_per_label_keys_are_recomputed_and_cross_label_duplicates_kept() {
        let dir = std::env::temp_dir().join(format!("ryu-ugc-interim-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(DB_FILE_NAME);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE submissions (
                     id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, creator_id TEXT NOT NULL,
                     platform TEXT NOT NULL, post_url TEXT NOT NULL,
                     external_post_id TEXT NOT NULL DEFAULT '',
                     dedup_key TEXT NOT NULL DEFAULT '',
                     status TEXT NOT NULL DEFAULT 'pending', submitted_at TEXT NOT NULL,
                     reviewed_at TEXT, rejection_reason TEXT,
                     created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                 CREATE UNIQUE INDEX idx_submissions_post
                     ON submissions(campaign_id, platform, external_post_id)
                     WHERE external_post_id <> '';
                 CREATE UNIQUE INDEX idx_submissions_dedup
                     ON submissions(campaign_id, platform, dedup_key);
                 INSERT INTO submissions
                   (id, campaign_id, creator_id, platform, post_url, external_post_id,
                    dedup_key, status, submitted_at, created_at, updated_at) VALUES
                   ('s1','c1','u1','youtube','https://youtu.be/abc123','abc123','id:abc123','approved','t1','t1','t1'),
                   ('s2','c1','u1','other','https://youtu.be/abc123','','url:https://youtu.be/abc123','approved','t2','t2','t2'),
                   ('s3','c1','u2','youtube','https://www.youtube.com/feed/x','','url:https://youtube.com/feed/x','pending','t3','t3','t3'),
                   ('s4','c1','u2','other','https://www.youtube.com/feed/x','','url:https://youtube.com/feed/x','pending','t4','t4','t4');",
            )
            .unwrap();
        }
        let store = UgcStore::open(path.clone()).unwrap();
        for id in ["s1", "s2", "s3", "s4"] {
            assert!(
                store.get_submission(id).await.unwrap().is_some(),
                "{id} kept"
            );
        }
        for (i, (platform, url)) in [
            ("youtube", "https://youtube.com/watch?v=abc123"),
            ("tiktok", "https://youtu.be/abc123"),
            ("myspace", "https://youtube.com/feed/x/"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut again = submission(&format!("n{i}"), "c1", "u3", "");
            again.platform = platform.into();
            again.post_url = url.into();
            assert_eq!(
                store.insert_submission(&again).await.unwrap(),
                WriteOutcome::DuplicatePost,
                "{platform} {url}"
            );
        }
        drop(store);
        let reopened = UgcStore::open(path).unwrap();
        let conn = reopened.conn.lock().await;
        let keys: Vec<String> = conn
            .prepare("SELECT dedup_key FROM submissions ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            keys,
            vec![
                "id:youtube:abc123".to_string(),
                "legacy:s2".to_string(),
                "url:https://youtube.com/feed/x".to_string(),
                "legacy:s4".to_string(),
            ]
        );
        let old_index = conn
            .prepare("SELECT 1 FROM sqlite_master WHERE name = 'idx_submissions_dedup'")
            .unwrap()
            .exists([])
            .unwrap();
        assert!(!old_index, "the per-label index is gone");
    }

    /// A database written by the label-independent key before the host gate
    /// (`idx_submissions_identity` present, `user_version` 0): a legacy `youtube`
    /// row on `evil.example/watch?v=abc123` stored `abc123` as its metrics id but
    /// a `url:` dedup key, so `youtu.be/abc123` under another label was accepted
    /// beside it. The migration re-derives every id under the host-gated parser
    /// (the foreign row stops claiming abc123; a percent-encoded row gains its
    /// decoded id), recomputes keys with the normalised URL, keeps every row, and
    /// from then on refuses the post under any label.
    #[tokio::test]
    async fn label_host_mismatched_ids_are_rederived_and_the_post_is_then_unique() {
        let dir = std::env::temp_dir().join(format!("ryu-ugc-hostgate-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(DB_FILE_NAME);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE submissions (
                     id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, creator_id TEXT NOT NULL,
                     platform TEXT NOT NULL, post_url TEXT NOT NULL,
                     external_post_id TEXT NOT NULL DEFAULT '',
                     dedup_key TEXT NOT NULL DEFAULT '',
                     status TEXT NOT NULL DEFAULT 'pending', submitted_at TEXT NOT NULL,
                     reviewed_at TEXT, rejection_reason TEXT,
                     created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                 CREATE UNIQUE INDEX idx_submissions_post
                     ON submissions(campaign_id, platform, external_post_id)
                     WHERE external_post_id <> '';
                 CREATE UNIQUE INDEX idx_submissions_identity
                     ON submissions(campaign_id, dedup_key);
                 INSERT INTO submissions
                   (id, campaign_id, creator_id, platform, post_url, external_post_id,
                    dedup_key, status, submitted_at, created_at, updated_at) VALUES
                   ('s1','c1','u1','youtube','https://evil.example/watch?v=abc123','abc123','url:https://evil.example/watch?v=abc123','approved','t1','t1','t1'),
                   ('s2','c1','u1','youtube','https://youtu.be/%64ef456','','url:https://youtu.be/%64ef456','pending','t2','t2','t2'),
                   ('s3','c1','u2','youtube','https://youtu.be/ghi789','ghi789','id:youtube:ghi789','pending','t3','t3','t3'),
                   ('s4','c1','u2','other','https://youtu.be/%67hi789','','url:https://youtu.be/%67hi789','pending','t4','t4','t4');",
            )
            .unwrap();
        }
        let store = UgcStore::open(path.clone()).unwrap();
        let read = |id: &'static str| {
            let store = &store;
            async move { store.get_submission(id).await.unwrap().unwrap() }
        };
        assert_eq!(
            read("s1").await.external_post_id,
            "",
            "foreign host keeps no YouTube id"
        );
        assert_eq!(
            read("s2").await.external_post_id,
            "def456",
            "decoded id gained"
        );
        assert_eq!(
            read("s3").await.external_post_id,
            "ghi789",
            "unchanged id kept"
        );
        assert_eq!(
            read("s4").await.external_post_id,
            "",
            "uncurated label parses no id"
        );
        // youtu.be/abc123 now owns abc123, under any label, exactly once...
        let mut real = submission("n1", "c1", "u3", "abc123");
        real.platform = "other".into();
        real.external_post_id = String::new();
        assert_eq!(
            store.insert_submission(&real).await.unwrap(),
            WriteOutcome::Written
        );
        let mut again = submission("n2", "c1", "u3", "abc123");
        assert_eq!(
            store.insert_submission(&again).await.unwrap(),
            WriteOutcome::DuplicatePost
        );
        // ...and the respelled legacy posts are held by their first row.
        again.id = "n3".into();
        again.post_url = "https://youtu.be/def456".into();
        again.external_post_id = "def456".into();
        assert_eq!(
            store.insert_submission(&again).await.unwrap(),
            WriteOutcome::DuplicatePost
        );
        drop(store);
        let reopened = UgcStore::open(path).unwrap();
        let conn = reopened.conn.lock().await;
        let keys: Vec<String> = conn
            .prepare("SELECT dedup_key FROM submissions WHERE id LIKE 's%' ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            keys,
            vec![
                "url:https://evil.example/watch?v=abc123".to_string(),
                "id:youtube:def456".to_string(),
                "id:youtube:ghi789".to_string(),
                "legacy:s4".to_string(),
            ]
        );
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SUBMISSION_IDENTITY_VERSION);
    }

    #[test]
    fn canonical_post_url_folds_encoded_and_doubled_path_spellings() {
        let base = canonical_post_url("https://blog.example/post/abc");
        for url in [
            "https://blog.example/post/%61bc",
            "https://blog.example//post/abc",
            "https://blog.example/post//abc/",
            "https://blog.example/%70ost/%61bc",
        ] {
            assert_eq!(canonical_post_url(url), base, "{url}");
        }
        // Escapes of reserved characters still name a different path; only their
        // hex case is folded.
        assert_ne!(
            canonical_post_url("https://blog.example/a%2Fb"),
            canonical_post_url("https://blog.example/a/b")
        );
        assert_eq!(
            canonical_post_url("https://blog.example/a%2fb"),
            canonical_post_url("https://blog.example/a%2Fb")
        );
        // Path case is still identity.
        assert_ne!(canonical_post_url("https://blog.example/post/ABC"), base);
    }

    /// Paid payouts survive every cascade: the parent delete is refused inside
    /// its transaction and changes nothing — including a campaign-level paid
    /// adjustment with no submission, and a paid row only reachable through a
    /// submission whose payout names another creator/campaign id.
    #[tokio::test]
    async fn no_cascade_deletes_a_paid_payout() {
        let store = temp_store();
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        store.upsert_creator(&creator("u1", "ada")).await.unwrap();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        store
            .insert_submission(&submission("s2", "c1", "u1", "vid2"))
            .await
            .unwrap();
        store.insert_snapshot(&snapshot("s1", 500)).await.unwrap();
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 500))
            .await
            .unwrap();
        store
            .upsert_payout(&payout("p2", "c1", "u1", Some("s2"), 300))
            .await
            .unwrap();
        let at = now();
        store
            .set_payout_status("p1", PayoutStatus::Approved, &at)
            .await
            .unwrap();
        store
            .set_payout_status("p1", PayoutStatus::Paid, &at)
            .await
            .unwrap();

        let refused = DeleteOutcome::PaidPayoutsRetained { paid: 1 };
        assert_eq!(store.delete_campaign("c1").await.unwrap(), refused);
        assert_eq!(store.delete_creator("u1", true).await.unwrap(), refused);
        assert_eq!(store.delete_submission("s1").await.unwrap(), refused);
        // Nothing moved — not the parents, not the unpaid sibling, not snapshots.
        assert!(store.get_campaign("c1").await.unwrap().is_some());
        assert!(store.get_creator("u1").await.unwrap().is_some());
        assert!(store.get_submission("s1").await.unwrap().is_some());
        assert!(store.get_submission("s2").await.unwrap().is_some());
        assert!(store.latest_snapshot("s1").await.unwrap().is_some());
        assert_eq!(
            store.get_payout("p1").await.unwrap().unwrap().status,
            PayoutStatus::Paid
        );
        assert!(store.get_payout("p2").await.unwrap().is_some());

        // A non-forced creator delete never touches payouts, so it is allowed
        // and the paid row stays.
        assert_eq!(
            store.delete_creator("u1", false).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(store.get_payout("p1").await.unwrap().is_some());

        // A paid campaign-level adjustment (no submission) blocks too.
        store
            .upsert_campaign(&campaign("c2", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        store
            .upsert_payout(&payout("p3", "c2", "u2", None, 50))
            .await
            .unwrap();
        store
            .set_payout_status("p3", PayoutStatus::Approved, &at)
            .await
            .unwrap();
        store
            .set_payout_status("p3", PayoutStatus::Paid, &at)
            .await
            .unwrap();
        assert_eq!(store.delete_campaign("c2").await.unwrap(), refused);
        store.upsert_creator(&creator("u2", "bob")).await.unwrap();
        assert_eq!(store.delete_creator("u2", true).await.unwrap(), refused);
        assert!(store.get_payout("p3").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn duplicate_post_is_rejected_but_blank_post_ids_are_not() {
        let store = temp_store();
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        assert_eq!(
            store
                .insert_submission(&submission("s1", "c1", "u1", "vid1"))
                .await
                .unwrap(),
            WriteOutcome::Written
        );
        // Same post, same campaign, different row id => the unique index bites.
        assert_eq!(
            store
                .insert_submission(&submission("s2", "c1", "u2", "vid1"))
                .await
                .unwrap(),
            WriteOutcome::DuplicatePost
        );
        // The same post in a DIFFERENT campaign is a legitimate second submission.
        store
            .upsert_campaign(&campaign("c2", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        assert_eq!(
            store
                .insert_submission(&submission("s3", "c2", "u1", "vid1"))
                .await
                .unwrap(),
            WriteOutcome::Written
        );
        // Unparseable URLs (empty id) fall back to their canonical URL: two
        // DIFFERENT links coexist and stay reviewable by hand...
        let mut blank_a = submission("s4", "c1", "u1", "");
        blank_a.post_url = "https://www.youtube.com/feed/subscriptions".into();
        let mut blank_b = submission("s5", "c1", "u1", "");
        blank_b.post_url = "https://www.youtube.com/feed/history".into();
        assert_eq!(
            store.insert_submission(&blank_a).await.unwrap(),
            WriteOutcome::Written
        );
        assert_eq!(
            store.insert_submission(&blank_b).await.unwrap(),
            WriteOutcome::Written
        );
        // ...but the SAME unparseable link (even respelled with a fragment,
        // tracking params or a trailing slash, or by another creator) is the
        // same post and can never be a second payable row.
        for (i, url) in [
            "https://www.youtube.com/feed/subscriptions",
            "https://youtube.com/feed/subscriptions/#top",
            "https://WWW.YouTube.com/feed/subscriptions?utm_source=x&si=abc",
        ]
        .into_iter()
        .enumerate()
        {
            let mut again = submission(&format!("dup{i}"), "c1", "u2", "");
            again.post_url = url.into();
            assert_eq!(
                store.insert_submission(&again).await.unwrap(),
                WriteOutcome::DuplicatePost,
                "{url}"
            );
        }
        // An edit that respells another row into an existing link collides too.
        let mut edited = blank_b.clone();
        edited.post_url = "https://youtube.com/feed/subscriptions".into();
        assert_eq!(
            store.update_submission(&edited).await.unwrap(),
            WriteOutcome::DuplicatePost
        );
        // ...and "" never resolves to one post.
        assert!(store
            .find_submission_by_post("c1", "youtube", "")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .find_submission_by_post("c1", "youtube", "vid1")
                .await
                .unwrap()
                .unwrap()
                .id,
            "s1"
        );
    }

    #[tokio::test]
    async fn submission_update_reports_missing_rows_and_collisions() {
        let store = temp_store();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        store
            .insert_submission(&submission("s2", "c1", "u1", "vid2"))
            .await
            .unwrap();

        let mut edit = submission("s2", "c1", "u1", "vid3");
        assert_eq!(
            store.update_submission(&edit).await.unwrap(),
            WriteOutcome::Written
        );
        // Editing s2 onto s1's post is the same double-pay hazard as inserting it.
        edit.external_post_id = "vid1".into();
        assert_eq!(
            store.update_submission(&edit).await.unwrap(),
            WriteOutcome::DuplicatePost
        );
        assert_eq!(
            store
                .update_submission(&submission("absent", "c1", "u1", "vid9"))
                .await
                .unwrap(),
            WriteOutcome::NotFound
        );
        // The edit never touches review state.
        assert_eq!(
            store.get_submission("s2").await.unwrap().unwrap().status,
            SubmissionStatus::Pending
        );
    }

    #[tokio::test]
    async fn submission_filters_and_latest_metrics_join() {
        let store = temp_store();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        let mut other = submission("s2", "c1", "u2", "vid2");
        other.platform = "tiktok".into();
        store.insert_submission(&other).await.unwrap();
        store
            .insert_submission(&submission("s3", "c2", "u1", "vid3"))
            .await
            .unwrap();

        store
            .set_submission_status("s1", SubmissionStatus::Approved, Some(&now()), None, &now())
            .await
            .unwrap();

        let by_campaign = SubmissionFilter {
            campaign_id: Some("c1".into()),
            ..Default::default()
        };
        assert_eq!(store.list_submissions(&by_campaign).await.unwrap().len(), 2);

        let by_platform = SubmissionFilter {
            campaign_id: Some("c1".into()),
            platform: Some("TikTok".into()),
            ..Default::default()
        };
        assert_eq!(
            store.list_submissions(&by_platform).await.unwrap()[0].id,
            "s2"
        );

        let by_status = SubmissionFilter {
            status: Some(SubmissionStatus::Approved),
            ..Default::default()
        };
        assert_eq!(store.list_submissions(&by_status).await.unwrap().len(), 1);

        let by_creator = SubmissionFilter {
            creator_id: Some("u1".into()),
            ..Default::default()
        };
        assert_eq!(store.list_submissions(&by_creator).await.unwrap().len(), 2);

        // Two snapshots: only the newest one is joined on.
        store.insert_snapshot(&snapshot("s1", 100)).await.unwrap();
        store.insert_snapshot(&snapshot("s1", 4200)).await.unwrap();
        let joined = store
            .list_submissions_with_metrics(&by_campaign)
            .await
            .unwrap();
        assert_eq!(joined.len(), 2);
        let s1 = joined.iter().find(|r| r.submission.id == "s1").unwrap();
        assert_eq!(s1.latest.as_ref().unwrap().views, 4200);
        // A submission with no snapshot yet joins to nothing, it does not vanish.
        let s2 = joined.iter().find(|r| r.submission.id == "s2").unwrap();
        assert!(s2.latest.is_none());

        // History is newest-first and honours the limit.
        let history = store.list_snapshots("s1", Some(1)).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].views, 4200);
        assert_eq!(history[0].source, MetricSource::Composio);
        assert_eq!(store.list_snapshots("s1", None).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn list_joins_payout_money_and_never_fakes_a_zero() {
        let store = temp_store();
        // s1 accrues, s2 is paid, s3 has no payout row at all.
        for (sid, post) in [("s1", "vid1"), ("s2", "vid2"), ("s3", "vid3")] {
            store
                .insert_submission(&submission(sid, "c1", "u1", post))
                .await
                .unwrap();
        }
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 10_300))
            .await
            .unwrap();
        let mut paid = payout("p2", "c1", "u1", Some("s2"), 200);
        paid.status = PayoutStatus::Paid;
        store.upsert_payout(&paid).await.unwrap();
        // A campaign-level row belongs to no post and must not attach to one.
        store
            .upsert_payout(&payout("p3", "c1", "u1", None, 999))
            .await
            .unwrap();

        let by_campaign = SubmissionFilter {
            campaign_id: Some("c1".into()),
            ..Default::default()
        };
        let rows = store
            .list_submissions_with_metrics(&by_campaign)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3, "the payouts join must not fan rows out");

        let s1 = rows.iter().find(|r| r.submission.id == "s1").unwrap();
        assert_eq!(s1.accrued_cents, Some(10_300));
        assert_eq!(s1.payout_status.as_deref(), Some("accrued"));

        // Paid money reports its own state, so the panel can tell it from money
        // that is merely committed.
        let s2 = rows.iter().find(|r| r.submission.id == "s2").unwrap();
        assert_eq!(s2.accrued_cents, Some(200));
        assert_eq!(s2.payout_status.as_deref(), Some("paid"));

        // No payout row => None, NOT Some(0). A zero would read as "this post is
        // worth nothing" instead of "nothing has accrued yet".
        let s3 = rows.iter().find(|r| r.submission.id == "s3").unwrap();
        assert_eq!(s3.accrued_cents, None);
        assert_eq!(s3.payout_status, None);

        // A payout genuinely priced at zero is the other fact, and stays distinct.
        store
            .upsert_payout(&payout("p4", "c1", "u1", Some("s3"), 0))
            .await
            .unwrap();
        let rows = store
            .list_submissions_with_metrics(&by_campaign)
            .await
            .unwrap();
        let s3 = rows.iter().find(|r| r.submission.id == "s3").unwrap();
        assert_eq!(s3.accrued_cents, Some(0));
        assert_eq!(s3.payout_status.as_deref(), Some("accrued"));
    }

    #[tokio::test]
    async fn joined_list_filters_are_unambiguous_against_payouts() {
        // `payouts` carries campaign_id, creator_id and status too: an unqualified
        // WHERE would make SQLite refuse the statement outright, so every filter
        // has to be exercised through the joined read, not just the plain one.
        let store = temp_store();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        let mut other = submission("s2", "c1", "u2", "vid2");
        other.platform = "tiktok".into();
        store.insert_submission(&other).await.unwrap();
        store
            .set_submission_status("s1", SubmissionStatus::Approved, Some(&now()), None, &now())
            .await
            .unwrap();
        let mut paid = payout("p1", "c1", "u1", Some("s1"), 500);
        paid.status = PayoutStatus::Paid;
        store.upsert_payout(&paid).await.unwrap();

        // The submission is `approved` while its payout is `paid` — filtering on
        // the wrong table's status column would silently return the wrong row.
        let every_filter = SubmissionFilter {
            campaign_id: Some("c1".into()),
            creator_id: Some("u1".into()),
            status: Some(SubmissionStatus::Approved),
            platform: Some("youtube".into()),
            limit: None,
        };
        let rows = store
            .list_submissions_with_metrics(&every_filter)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].submission.id, "s1");
        assert_eq!(rows[0].payout_status.as_deref(), Some("paid"));

        let paid_submissions = SubmissionFilter {
            status: Some(SubmissionStatus::Paid),
            ..Default::default()
        };
        assert!(store
            .list_submissions_with_metrics(&paid_submissions)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn submission_delete_cascades_snapshots_and_payout() {
        let store = temp_store();
        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        store.insert_snapshot(&snapshot("s1", 100)).await.unwrap();
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 500))
            .await
            .unwrap();

        assert_eq!(
            store.delete_submission("s1").await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(store.latest_snapshot("s1").await.unwrap().is_none());
        assert!(store.payout_for_submission("s1").await.unwrap().is_none());
        assert_eq!(
            store.delete_submission("s1").await.unwrap(),
            DeleteOutcome::NotFound
        );
    }

    #[tokio::test]
    async fn payout_status_stamps_only_its_own_column() {
        let store = temp_store();
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 500))
            .await
            .unwrap();

        let approved = store
            .set_payout_status("p1", PayoutStatus::Approved, "2026-01-01T00:00:00Z")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(approved.status, PayoutStatus::Approved);
        assert_eq!(
            approved.approved_at.as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
        assert!(approved.paid_at.is_none());

        let paid = store
            .set_payout_status("p1", PayoutStatus::Paid, "2026-01-02T00:00:00Z")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paid.status, PayoutStatus::Paid);
        // The approval stamp survives being paid.
        assert_eq!(paid.approved_at.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(paid.paid_at.as_deref(), Some("2026-01-02T00:00:00Z"));

        assert!(store
            .set_payout_status("absent", PayoutStatus::Paid, "x")
            .await
            .unwrap()
            .is_none());

        // Paid money cannot be un-accrued by a reject.
        assert!(!store
            .delete_unpaid_payout_for_submission("s1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn re_pricing_a_payout_updates_in_place() {
        let store = temp_store();
        let mut p = payout("p1", "c1", "u1", Some("s1"), 500);
        store.upsert_payout(&p).await.unwrap();
        p.amount_cents = 900;
        p.updated_at = now();
        store.upsert_payout(&p).await.unwrap();

        let all = store.list_payouts(&PayoutFilter::default()).await.unwrap();
        assert_eq!(all.len(), 1, "re-pricing must not append a second row");
        assert_eq!(all[0].amount_cents, 900);
        // ...and the sums the caps read stay single-counted.
        assert_eq!(
            store.campaign_committed_cents("c1", None).await.unwrap(),
            900
        );
        assert_eq!(
            store
                .campaign_committed_cents("c1", Some("s1"))
                .await
                .unwrap(),
            0,
            "the row being re-priced is excluded from its own cap headroom"
        );
    }

    #[tokio::test]
    async fn re_pricing_never_reverts_an_approved_or_paid_payout() {
        let store = temp_store();
        let mut p = payout("p1", "c1", "u1", Some("s1"), 500);
        assert!(store.upsert_payout(&p).await.unwrap());
        let approved_at = now();
        store
            .set_payout_status("p1", PayoutStatus::Approved, &approved_at)
            .await
            .unwrap()
            .unwrap();

        // An accrual pass that read the row as `accrued` before the approval
        // landed must not undo the approval or move the signed-off amount.
        p.amount_cents = 900;
        p.updated_at = now();
        assert!(!store.upsert_payout(&p).await.unwrap());
        let stored = store.get_payout("p1").await.unwrap().unwrap();
        assert_eq!(stored.status, PayoutStatus::Approved);
        assert_eq!(stored.amount_cents, 500);
        assert_eq!(stored.approved_at.as_deref(), Some(approved_at.as_str()));

        // A paid row is never re-opened for a second payment either.
        let paid_at = now();
        store
            .set_payout_status("p1", PayoutStatus::Paid, &paid_at)
            .await
            .unwrap()
            .unwrap();
        assert!(!store.upsert_payout(&p).await.unwrap());
        let stored = store.get_payout("p1").await.unwrap().unwrap();
        assert_eq!(stored.status, PayoutStatus::Paid);
        assert_eq!(stored.amount_cents, 500);
        assert_eq!(stored.paid_at.as_deref(), Some(paid_at.as_str()));
    }

    #[tokio::test]
    async fn payout_filters_narrow_in_sql() {
        let store = temp_store();
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 100))
            .await
            .unwrap();
        store
            .upsert_payout(&payout("p2", "c1", "u2", Some("s2"), 200))
            .await
            .unwrap();
        let mut paid = payout("p3", "c2", "u1", Some("s3"), 300);
        paid.status = PayoutStatus::Paid;
        store.upsert_payout(&paid).await.unwrap();

        let by_campaign = PayoutFilter {
            campaign_id: Some("c1".into()),
            ..Default::default()
        };
        assert_eq!(store.list_payouts(&by_campaign).await.unwrap().len(), 2);

        let by_creator = PayoutFilter {
            creator_id: Some("u1".into()),
            ..Default::default()
        };
        assert_eq!(store.list_payouts(&by_creator).await.unwrap().len(), 2);

        let by_status = PayoutFilter {
            status: Some(PayoutStatus::Paid),
            ..Default::default()
        };
        assert_eq!(store.list_payouts(&by_status).await.unwrap()[0].id, "p3");

        // A campaign-level row (no submission) is unconstrained by the per-post
        // unique index and still counts toward the campaign total.
        store
            .upsert_payout(&payout("p4", "c1", "u1", None, 50))
            .await
            .unwrap();
        store
            .upsert_payout(&payout("p5", "c1", "u1", None, 25))
            .await
            .unwrap();
        assert_eq!(
            store.campaign_committed_cents("c1", None).await.unwrap(),
            375
        );
        assert_eq!(
            store
                .creator_committed_cents("c1", "u1", None)
                .await
                .unwrap(),
            175
        );
    }

    // ---- derived reads ----------------------------------------------------

    #[tokio::test]
    async fn campaign_summary_prices_off_the_latest_snapshot_only() {
        let store = temp_store();
        let mut c = campaign("c1", PayoutRule::Cpm { cpm_cents: 250 });
        c.budget_cents = 100_000;
        store.upsert_campaign(&c).await.unwrap();
        store.upsert_creator(&creator("u1", "ada")).await.unwrap();
        store.upsert_creator(&creator("u2", "grace")).await.unwrap();

        store
            .insert_submission(&submission("s1", "c1", "u1", "vid1"))
            .await
            .unwrap();
        store
            .insert_submission(&submission("s2", "c1", "u2", "vid2"))
            .await
            .unwrap();
        store
            .set_submission_status("s1", SubmissionStatus::Approved, Some(&now()), None, &now())
            .await
            .unwrap();

        // Three readings of the same post; only the last one counts.
        store.insert_snapshot(&snapshot("s1", 1_000)).await.unwrap();
        store.insert_snapshot(&snapshot("s1", 5_000)).await.unwrap();
        store
            .insert_snapshot(&snapshot("s1", 41_200))
            .await
            .unwrap();
        store.insert_snapshot(&snapshot("s2", 800)).await.unwrap();

        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 10_300))
            .await
            .unwrap();
        let mut paid = payout("p2", "c1", "u2", Some("s2"), 200);
        paid.status = PayoutStatus::Paid;
        store.upsert_payout(&paid).await.unwrap();

        let sum = store.campaign_summary("c1").await.unwrap().unwrap();
        assert_eq!(
            sum.total_views, 42_000,
            "latest per submission, not the sum"
        );
        assert_eq!(sum.total_likes, 20);
        assert_eq!(sum.total_comments, 4);
        assert_eq!(sum.accrued_cents, 10_300);
        assert_eq!(sum.paid_cents, 200);
        assert_eq!(sum.committed_cents, 10_500);
        assert_eq!(sum.remaining_cents, Some(89_500));
        assert_eq!(sum.submissions.approved, 1);
        assert_eq!(sum.submissions.pending, 1);
        assert_eq!(sum.submissions.total(), 2);
        assert_eq!(sum.creators, 2);

        assert!(store.campaign_summary("absent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn uncapped_campaign_reports_no_remaining_rather_than_zero() {
        let store = temp_store();
        // budget_cents = 0 is "uncapped", so "remaining" is not a number at all.
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 9_999))
            .await
            .unwrap();
        let sum = store.campaign_summary("c1").await.unwrap().unwrap();
        assert_eq!(sum.budget_cents, 0);
        assert_eq!(sum.committed_cents, 9_999);
        assert_eq!(sum.remaining_cents, None);
    }

    #[tokio::test]
    async fn leaderboard_ranks_by_latest_views_and_breaks_ties_stably() {
        let store = temp_store();
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Cpm { cpm_cents: 100 }))
            .await
            .unwrap();
        store.upsert_creator(&creator("u1", "zoe")).await.unwrap();
        store.upsert_creator(&creator("u2", "ada")).await.unwrap();
        store.upsert_creator(&creator("u3", "bob")).await.unwrap();

        for (sid, uid, post) in [("s1", "u1", "v1"), ("s2", "u2", "v2"), ("s3", "u3", "v3")] {
            store
                .insert_submission(&submission(sid, "c1", uid, post))
                .await
                .unwrap();
            store
                .set_submission_status(sid, SubmissionStatus::Approved, Some(&now()), None, &now())
                .await
                .unwrap();
        }
        // u3 leads; u1 and u2 tie on views, so the tie-break must decide.
        store.insert_snapshot(&snapshot("s1", 5_000)).await.unwrap();
        store.insert_snapshot(&snapshot("s2", 5_000)).await.unwrap();
        store.insert_snapshot(&snapshot("s3", 9_000)).await.unwrap();

        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 500))
            .await
            .unwrap();
        let mut paid = payout("p3", "c1", "u3", Some("s3"), 900);
        paid.status = PayoutStatus::Paid;
        store.upsert_payout(&paid).await.unwrap();

        let board = store.campaign_leaderboard("c1", None).await.unwrap();
        assert_eq!(board.len(), 3);
        assert_eq!(board[0].creator_id, "u3");
        assert_eq!(board[0].views, 9_000);
        assert_eq!(board[0].paid_cents, 900);
        assert_eq!(board[0].accrued_cents, 0);
        // Tied on views => ordered by display name ("ada" before "zoe"), which is
        // deterministic where a bare `ORDER BY views DESC` would not be.
        assert_eq!(board[1].creator_id, "u2");
        assert_eq!(board[2].creator_id, "u1");
        assert_eq!(board[2].accrued_cents, 500);
        assert_eq!(board[1].approved_submissions, 1);

        // Running it again gives the identical order.
        let again = store.campaign_leaderboard("c1", None).await.unwrap();
        let ids: Vec<_> = again.iter().map(|r| r.creator_id.clone()).collect();
        assert_eq!(ids, vec!["u3", "u2", "u1"]);

        assert_eq!(
            store
                .campaign_leaderboard("c1", Some(1))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn overview_and_creator_totals_count_across_campaigns() {
        let store = temp_store();
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        store
            .upsert_campaign(&campaign("c2", PayoutRule::Flat { flat_cents: 100 }))
            .await
            .unwrap();
        store.upsert_creator(&creator("u1", "ada")).await.unwrap();

        store
            .insert_submission(&submission("s1", "c1", "u1", "v1"))
            .await
            .unwrap();
        store
            .insert_submission(&submission("s2", "c2", "u1", "v2"))
            .await
            .unwrap();
        store
            .set_submission_status("s2", SubmissionStatus::Approved, Some(&now()), None, &now())
            .await
            .unwrap();
        store
            .upsert_payout(&payout("p1", "c2", "u1", Some("s2"), 100))
            .await
            .unwrap();

        let overview = store.overview().await.unwrap();
        assert_eq!(overview.campaigns, 2);
        assert_eq!(overview.creators, 1);
        assert_eq!(overview.submissions.pending, 1);
        assert_eq!(overview.submissions.approved, 1);
        assert_eq!(overview.accrued_cents, 100);
        assert_eq!(overview.paid_cents, 0);

        let totals = store.creator_totals("u1").await.unwrap();
        assert_eq!(totals.submissions.total(), 2);
        assert_eq!(totals.accrued_cents, 100);
    }

    #[tokio::test]
    async fn accrual_inputs_gather_everything_and_price_the_submission() {
        let store = temp_store();
        let mut c = campaign("c1", PayoutRule::Cpm { cpm_cents: 250 });
        c.budget_cents = 20_000;
        c.max_payout_per_creator_cents = 12_000;
        store.upsert_campaign(&c).await.unwrap();
        store
            .insert_submission(&submission("s1", "c1", "u1", "v1"))
            .await
            .unwrap();

        // Pending work is worth nothing — unreviewed posts must not eat budget.
        store
            .insert_snapshot(&snapshot("s1", 41_200))
            .await
            .unwrap();
        let pending = store.accrual_inputs("s1").await.unwrap().unwrap();
        assert_eq!(pending.views(), 41_200);
        assert_eq!(pending.amount_cents(), 0);

        store
            .set_submission_status("s1", SubmissionStatus::Approved, Some(&now()), None, &now())
            .await
            .unwrap();
        let approved = store.accrual_inputs("s1").await.unwrap().unwrap();
        assert_eq!(approved.amount_cents(), 10_300);
        assert!(approved.existing_payout.is_none());

        // A second post from the same creator runs into the per-creator ceiling.
        store
            .upsert_payout(&payout("p1", "c1", "u1", Some("s1"), 10_300))
            .await
            .unwrap();
        store
            .insert_submission(&submission("s2", "c1", "u1", "v2"))
            .await
            .unwrap();
        store
            .set_submission_status("s2", SubmissionStatus::Approved, Some(&now()), None, &now())
            .await
            .unwrap();
        store
            .insert_snapshot(&snapshot("s2", 41_200))
            .await
            .unwrap();
        let second = store.accrual_inputs("s2").await.unwrap().unwrap();
        assert_eq!(second.creator_committed_excluding_this, 10_300);
        assert_eq!(
            second.amount_cents(),
            1_700,
            "12 000c cap minus 10 300c already committed"
        );

        assert!(store.accrual_inputs("absent").await.unwrap().is_none());
    }

    // ---- pure payout math -------------------------------------------------

    #[test]
    fn cpm_pricing_is_integer_cents_and_floors() {
        let cpm = PayoutRule::Cpm { cpm_cents: 250 };
        assert_eq!(payout_for(0, &cpm, &[]), 0);
        // 999 views at 250c/1k is 249.75c — it floors to 249c, it does not become
        // a float and it does not round up.
        assert_eq!(payout_for(999, &cpm, &[]), 249);
        assert_eq!(payout_for(1_000, &cpm, &[]), 250);
        assert_eq!(payout_for(41_200, &cpm, &[]), 10_300);
        // Nonsense inputs degrade to 0 rather than producing negative money.
        assert_eq!(payout_for(-5, &cpm, &[]), 0);
        assert_eq!(
            payout_for(1_000, &PayoutRule::Cpm { cpm_cents: -250 }, &[]),
            0
        );
        // An absurd view count saturates instead of wrapping negative.
        assert!(payout_for(i64::MAX, &cpm, &[]) > 0);
    }

    #[test]
    fn flat_pricing_ignores_views() {
        let flat = PayoutRule::Flat { flat_cents: 5_000 };
        assert_eq!(payout_for(0, &flat, &[]), 5_000);
        assert_eq!(payout_for(9_000_000, &flat, &[]), 5_000);
        assert_eq!(payout_for(10, &PayoutRule::Flat { flat_cents: -1 }, &[]), 0);
    }

    #[test]
    fn one_bonus_tier_applies_at_exactly_its_threshold() {
        let tiers = [
            BonusTier {
                views: 10_000,
                bonus_cents: 1_000,
            },
            BonusTier {
                views: 50_000,
                bonus_cents: 5_000,
            },
        ];
        let flat = PayoutRule::Flat { flat_cents: 100 };
        assert_eq!(
            payout_for(9_999, &flat, &tiers),
            100,
            "below the first tier"
        );
        assert_eq!(
            payout_for(10_000, &flat, &tiers),
            1_100,
            "a threshold is met at exactly its view count"
        );
        assert_eq!(payout_for(10_001, &flat, &tiers), 1_100);
        assert_eq!(payout_for(49_999, &flat, &tiers), 1_100);
        // The highest MET tier wins outright: 5 000c, not 1 000 + 5 000.
        assert_eq!(payout_for(50_000, &flat, &tiers), 5_100);
        assert_eq!(payout_for(120_000, &flat, &tiers), 5_100);
    }

    #[test]
    fn tiers_out_of_order_still_pick_the_highest_met_threshold() {
        // The API validates that tiers increase, but the math must not depend on
        // it — a hand-edited campaign blob can arrive in any order.
        let tiers = [
            BonusTier {
                views: 50_000,
                bonus_cents: 5_000,
            },
            BonusTier {
                views: 10_000,
                bonus_cents: 1_000,
            },
        ];
        let flat = PayoutRule::Flat { flat_cents: 0 };
        assert_eq!(payout_for(10_000, &flat, &tiers), 1_000);
        assert_eq!(payout_for(50_000, &flat, &tiers), 5_000);
    }

    #[test]
    fn caps_clamp_and_zero_means_uncapped() {
        // No ceilings at all: 0 / 0 must not clamp to zero.
        assert_eq!(clamp_payout(10_300, 0, 0, 0, 0), 10_300);
        assert_eq!(clamp_payout(10_300, 99_999, 0, 99_999, 0), 10_300);

        // Per-creator ceiling: 12 000c cap, 10 300c already committed elsewhere.
        assert_eq!(clamp_payout(10_300, 10_300, 12_000, 0, 0), 1_700);
        // Exactly at the ceiling => nothing more.
        assert_eq!(clamp_payout(10_300, 12_000, 12_000, 0, 0), 0);
        // Over the ceiling (a lowered cap) never produces negative money.
        assert_eq!(clamp_payout(10_300, 20_000, 12_000, 0, 0), 0);

        // Campaign budget: the tighter of the two ceilings wins.
        assert_eq!(clamp_payout(10_300, 0, 12_000, 9_000, 10_000), 1_000);
        assert_eq!(clamp_payout(10_300, 0, 0, 10_000, 10_000), 0);
        // A negative raw amount is floored, not propagated.
        assert_eq!(clamp_payout(-1, 0, 0, 0, 0), 0);
    }

    #[test]
    fn status_strings_roundtrip_and_unknown_degrades() {
        for s in [
            CampaignStatus::Draft,
            CampaignStatus::Active,
            CampaignStatus::Paused,
            CampaignStatus::Ended,
        ] {
            assert_eq!(CampaignStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(
            CampaignStatus::parse("  ACTIVE "),
            Some(CampaignStatus::Active)
        );
        // An unknown filter value is "no filter", which the list endpoint turns
        // into "all" rather than an empty list.
        assert!(CampaignStatus::parse("mystery").is_none());

        for s in [
            SubmissionStatus::Pending,
            SubmissionStatus::Approved,
            SubmissionStatus::Rejected,
            SubmissionStatus::Paid,
        ] {
            assert_eq!(SubmissionStatus::from_db(s.as_str()), s);
        }
        assert_eq!(
            SubmissionStatus::from_db("mystery"),
            SubmissionStatus::Pending
        );

        for s in [
            PayoutStatus::Accrued,
            PayoutStatus::Approved,
            PayoutStatus::Paid,
        ] {
            assert_eq!(PayoutStatus::from_db(s.as_str()), s);
        }
        assert_eq!(PayoutStatus::from_db("mystery"), PayoutStatus::Accrued);

        for s in [MetricSource::Manual, MetricSource::Composio] {
            assert_eq!(MetricSource::from_db(s.as_str()), s);
        }
        // Unknown never claims an automated read happened.
        assert_eq!(MetricSource::from_db("mystery"), MetricSource::Manual);
    }

    #[test]
    fn limits_are_clamped_server_side() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(-9)), 1);
        assert_eq!(clamp_limit(Some(10)), 10);
        assert_eq!(clamp_limit(Some(9_000)), MAX_LIMIT);
    }

    #[tokio::test]
    async fn writes_broadcast_to_live_subscribers() {
        let store = temp_store();
        let mut rx = store.subscribe();
        store
            .upsert_campaign(&campaign("c1", PayoutRule::Flat { flat_cents: 1 }))
            .await
            .unwrap();
        assert!(matches!(
            rx.try_recv().expect("campaign event"),
            UgcEvent::CampaignChanged { .. }
        ));
    }
}
