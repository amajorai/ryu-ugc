//! HTTP API for the UGC campaign tracker (`/api/ugc/*`).
//!
//! CRUD over campaigns and the creator roster, submission intake with review,
//! metric snapshots (curated-Composio refresh or hand-entered), payout approval
//! and settlement, and the three derived reads the dock panel paints from (overview,
//! campaign summary, creator leaderboard). Provider execution is routed through
//! Ryu's managed Gateway; this satellite accepts no provider keys.
//!
//! The router is built with its own state ([`UgcCtx`]) inside this crate so it
//! returns a state-less, mergeable `Router<()>`. Its paths are **relative** to
//! `/api/ugc` (the sidecar shell nests them at that prefix, which is also the
//! manifest's `http.mount`/`public_mount`, so the ext-proxy forwards
//! `/api/ugc/*` unchanged), while the `#[utoipa::path]` annotations keep the full
//! external paths.
//!
//! ‼ **The manifest's `http.routes[]` is enforcing, not advisory.** Core's
//! `resolve_route` 404s any path no declared route matches, *before* the sidecar
//! is reached. So this router and `manifest.json` must stay in lockstep, down to
//! the `:id` spelling — and the bare `"/"` route matters, because the public
//! mount registers the bare prefix and the `/*rest` wildcard as two separate axum
//! routes.
//!
//! # Two guards live here rather than in the store
//!
//! - [`screen_post_url`] is the SSRF screen for a submitted post URL, vendored
//!   from `apps-store/monitors/backend/src/net_guard.rs` (this crate must not
//!   depend on monitors). Only its *screening* half: we never fetch a post URL,
//!   so there is no resolve-and-pin step and no DNS in the request path.
//! - [`parse_post_id`] turns a post URL into the platform-native id, then runs it
//!   through [`crate::composio::id_segment_is_safe`]. That id is the ONE dynamic
//!   value this app ever hands to a Composio action, so an id that is not a plain
//!   segment is discarded (stored as `""`) rather than forwarded — the submission
//!   stays recordable and reviewable by hand, it just cannot auto-refresh.

use std::collections::BTreeMap;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;

use crate::{
    composio, new_id, now_iso, BonusTier, Campaign, CampaignStatus, Creator, DeleteOutcome,
    PayoutFilter, PayoutRule, PayoutStatus, RefreshError, RefreshOutcome, Submission,
    SubmissionFilter, SubmissionRefreshReport, SubmissionStatus, SubmissionWithMetrics, UgcEngine,
    UgcStore, WriteOutcome,
};

/// The Core-signed grant of the agent a request was forwarded for, if one is.
/// Core stamps it (and strips any caller copy); a metric refresh hands it back
/// on its provider call, so Core applies that agent's account access there.
fn agent_grant(headers: &HeaderMap) -> Option<String> {
    headers
        .get(ryu_app_events::HDR_AGENT_GRANT)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Router state for the UGC HTTP surface: the [`UgcEngine`] (which owns the store
/// and the inverted [`crate::UgcHost`]).
#[derive(Clone)]
pub struct UgcCtx {
    pub engine: UgcEngine,
}

impl UgcCtx {
    #[must_use]
    pub fn new(engine: UgcEngine) -> Self {
        Self { engine }
    }
}

/// Build the `/api/ugc/*` router with its own state baked in, returning a
/// state-less `Router<()>` the host nests at `/api/ugc`.
///
/// Static segments are registered **before** any `:id` route so they match first
/// — `/platforms` would otherwise be swallowed by nothing here, but the habit is
/// what keeps a later `/campaigns/archived` from being read as a campaign id.
///
/// `/health` is deliberately absent: it must answer *before* the shared-secret
/// gate (Core's pre-auth readiness probe), so the sidecar shell registers
/// [`health`] outside this nest.
pub fn routes(ctx: UgcCtx) -> Router<()> {
    Router::new()
        .route("/", get(overview))
        .route("/platforms", get(list_platforms))
        .route("/campaigns", get(list_campaigns).post(create_campaign))
        .route("/campaigns/:id/summary", get(campaign_summary))
        .route("/campaigns/:id/leaderboard", get(campaign_leaderboard))
        .route("/campaigns/:id/submissions", get(campaign_submissions))
        .route("/campaigns/:id/refresh", post(refresh_campaign))
        .route(
            "/campaigns/:id",
            get(get_campaign)
                .put(update_campaign)
                .delete(delete_campaign),
        )
        .route("/creators", get(list_creators).post(create_creator))
        .route(
            "/creators/:id",
            get(get_creator).put(update_creator).delete(delete_creator),
        )
        .route(
            "/submissions",
            get(list_submissions).post(create_submission),
        )
        .route("/submissions/:id/review", post(review_submission))
        .route(
            "/submissions/:id/metrics",
            get(list_metrics).post(record_metrics),
        )
        .route("/submissions/:id/refresh", post(refresh_submission))
        .route(
            "/submissions/:id",
            get(get_submission)
                .put(update_submission)
                .delete(delete_submission),
        )
        .route("/payouts", get(list_payouts))
        .route("/payouts/:id/approve", post(approve_payout))
        .route("/payouts/:id/paid", post(mark_payout_paid))
        .route("/payouts/:id", get(get_payout))
        .with_state(ctx)
}

/// The OpenAPI sub-document for the UGC surface. The `#[utoipa::path]`
/// annotations keep their absolute `/api/ugc/...` paths even though the router
/// registers relative segments (the quests/monitors split: openapi = absolute,
/// routes = relative).
pub fn openapi() -> utoipa::openapi::OpenApi {
    <UgcApiDoc as utoipa::OpenApi>::openapi()
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
    approve_payout,
    campaign_leaderboard,
    campaign_submissions,
    campaign_summary,
    create_campaign,
    create_creator,
    create_submission,
    delete_campaign,
    delete_creator,
    delete_submission,
    get_campaign,
    get_creator,
    get_payout,
    get_submission,
    list_campaigns,
    list_creators,
    list_metrics,
    list_payouts,
    list_platforms,
    list_submissions,
    mark_payout_paid,
    overview,
    record_metrics,
    refresh_campaign,
    refresh_submission,
    review_submission,
    update_campaign,
    update_creator,
    update_submission,
    ),
    // Every write body, listed explicitly. utoipa 5 also auto-collects whatever is
    // reachable from `paths(...)`, so this is belt-and-braces — but a bare list is
    // greppable and survives an edit to the annotations above, and a body type that
    // silently stops being registered yields a `$ref` Core cannot resolve, which
    // means an LLM tool with ZERO visible arguments.
    components(schemas(
        BonusTier,
        CampaignBody,
        CreatorBody,
        MetricsBody,
        PayoutRule,
        ReviewBody,
        SubmissionBody,
        SubmissionEditBody,
    ))
)]
struct UgcApiDoc;

// ─────────────────────────────────────────────────────────────────────────────
// Health (registered by the sidecar shell, OUTSIDE the bearer gate)
// ─────────────────────────────────────────────────────────────────────────────

/// Loopback readiness probe. Asserts the store is readable (a cheap count) so
/// health confirms DB readiness and not just process liveness, and returns no
/// campaign data.
///
/// Lives here so the response shape has one definition, but is registered by
/// `main.rs` rather than by [`routes`]: Core probes it *before* stamping the
/// shared-secret bearer, so it cannot sit inside the gated nest.
pub async fn health(store: UgcStore) -> (StatusCode, Json<Value>) {
    match store.count_campaigns().await {
        Ok(campaigns) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "campaignCount": campaigns, "sealing": store.seal_health() })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false, "error": e.to_string() })),
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overview + the curated platform map
// ─────────────────────────────────────────────────────────────────────────────

/// `GET /api/ugc` — the dock panel's first paint.
#[utoipa::path(
    get,
    path = "/api/ugc",
    tag = "UGC",
    summary = "counts and money across every campaign — the panel's first paint.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn overview(State(ctx): State<UgcCtx>) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.overview().await {
        Ok(o) => (
            StatusCode::OK,
            Json(serde_json::to_value(o).unwrap_or_default()),
        ),
        Err(e) => internal(&e.to_string()),
    }
}

/// `GET /api/ugc/platforms` — the curated platform → Composio action map, served
/// verbatim.
///
/// This endpoint is why an unverified action id is *correctable* rather than
/// mysterious: the operator sees exactly which action and which response
/// selectors are in use for each platform, so a wrong row is a one-line fix in
/// one table instead of a hunt.
#[utoipa::path(
    get,
    path = "/api/ugc/platforms",
    tag = "UGC",
    summary = "the curated platform -> Composio action map, with its response selectors.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_platforms(State(ctx): State<UgcCtx>) -> (StatusCode, Json<Value>) {
    // This is provider status from Ryu's Gateway, not a credential check in the
    // satellite. A configured provider can still report a per-platform connection
    // requirement during refresh.
    let composio_configured = ctx.engine.managed_provider_configured("composio").await;
    (
        StatusCode::OK,
        Json(json!({
            "platforms": composio::PLATFORM_METRIC_SOURCES,
            "composio_configured": composio_configured,
        })),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Campaigns
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for creating/updating a campaign.
///
/// `payout` is typed, so a rule that is neither `cpm` nor `flat` is rejected by
/// the JSON extractor with serde's own "unknown variant `x`, expected `cpm` or
/// `flat`" message — a better error than anything a hand-rolled check would
/// produce.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct CampaignBody {
    pub brand: String,
    #[serde(default)]
    pub brief: String,
    /// `draft`/`active`/`paused`/`ended`. Absent keeps the current status (or
    /// `draft` on create).
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub platforms: Vec<String>,
    #[serde(default)]
    pub required_hashtags: Vec<String>,
    #[serde(default)]
    pub required_mentions: Vec<String>,
    #[serde(default)]
    pub starts_at: Option<String>,
    #[serde(default)]
    pub ends_at: Option<String>,
    /// 0 = uncapped.
    #[serde(default)]
    pub budget_cents: i64,
    /// How an approved post is priced: `{"type":"cpm","cpm_cents":250}` or
    /// `{"type":"flat","flat_cents":5000}`.
    // Inlined rather than left as a `$ref`. Core's importer resolves a `$ref` that
    // sits directly under `properties` (`openapi_import::spec_to_api`), so this one
    // would have survived — but only that one hop is promised, and it is a courtesy
    // of the consumer rather than a property of this document. Inlining makes the
    // two rules legible to anything that reads the spec, resolver or not.
    #[serde(default)]
    #[schema(inline)]
    pub payout: PayoutRule,
    /// Bonuses unlocked at view thresholds. Not summed — the highest met tier wins.
    // Inlining is load-bearing here, not belt-and-braces: an un-inlined `$ref` would
    // sit under `properties.bonus_tiers.items`, one hop past where the importer
    // stops looking, leaving the model an opaque pointer for the whole tier ladder.
    #[serde(default)]
    #[schema(inline)]
    pub bonus_tiers: Vec<BonusTier>,
    /// 0 = uncapped.
    #[serde(default)]
    pub max_payout_per_creator_cents: i64,
}

/// Validate a campaign body. Everything here is a rule money depends on, which is
/// why none of it is left to the store.
fn validate_campaign(body: &CampaignBody) -> Result<(), String> {
    if body.brand.trim().is_empty() {
        return Err("brand is required".to_string());
    }
    match body.payout {
        PayoutRule::Cpm { cpm_cents } if cpm_cents < 0 => {
            return Err("cpm_cents cannot be negative".to_string())
        }
        PayoutRule::Flat { flat_cents } if flat_cents < 0 => {
            return Err("flat_cents cannot be negative".to_string())
        }
        _ => {}
    }
    if body.budget_cents < 0 {
        return Err("budget_cents cannot be negative (use 0 for uncapped)".to_string());
    }
    if body.max_payout_per_creator_cents < 0 {
        return Err(
            "max_payout_per_creator_cents cannot be negative (use 0 for uncapped)".to_string(),
        );
    }
    // Strictly increasing thresholds: the accrual pass pays ONE tier — the
    // highest met — so a duplicate or out-of-order threshold makes the ladder
    // ambiguous to the person writing it, even though the code would cope.
    let mut previous: Option<i64> = None;
    for tier in &body.bonus_tiers {
        if tier.views < 0 || tier.bonus_cents < 0 {
            return Err("bonus tier views and bonus_cents cannot be negative".to_string());
        }
        if let Some(prev) = previous {
            if tier.views <= prev {
                return Err(format!(
                    "bonus tiers must increase strictly by view threshold ({} follows {prev})",
                    tier.views
                ));
            }
        }
        previous = Some(tier.views);
    }
    Ok(())
}

/// Fold a validated body onto a campaign record, preserving identity/timestamps.
fn apply_campaign(body: CampaignBody, mut campaign: Campaign) -> Campaign {
    campaign.status = body
        .status
        .as_deref()
        .and_then(CampaignStatus::parse)
        .unwrap_or(campaign.status);
    campaign.brand = body.brand.trim().to_string();
    campaign.brief = body.brief;
    campaign.platforms = body
        .platforms
        .iter()
        .map(|p| normalize_platform(p))
        .collect();
    campaign.required_hashtags = body.required_hashtags;
    campaign.required_mentions = body.required_mentions;
    campaign.starts_at = body.starts_at;
    campaign.ends_at = body.ends_at;
    campaign.budget_cents = body.budget_cents;
    campaign.payout = body.payout;
    campaign.bonus_tiers = body.bonus_tiers;
    campaign.max_payout_per_creator_cents = body.max_payout_per_creator_cents;
    campaign.updated_at = now_iso();
    campaign
}

/// Query params for the campaign list.
#[derive(Debug, Default, Deserialize)]
pub struct CampaignQuery {
    #[serde(default)]
    pub status: Option<String>,
}

/// `GET /api/ugc/campaigns` — list campaigns, optionally one status.
#[utoipa::path(
    get,
    path = "/api/ugc/campaigns",
    tag = "UGC",
    summary = "list campaigns, optionally filtered to one status.",
    params(("status" = Option<String>, Query, description = "draft|active|paused|ended")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_campaigns(
    State(ctx): State<UgcCtx>,
    Query(q): Query<CampaignQuery>,
) -> (StatusCode, Json<Value>) {
    // An unrecognised `?status=` means "no filter", never an empty list: an empty
    // list reads to the user as "you have no campaigns", which is a lie.
    let status = q.status.as_deref().and_then(CampaignStatus::parse);
    match ctx.engine.store.list_campaigns(status).await {
        Ok(campaigns) => (StatusCode::OK, Json(json!({ "campaigns": campaigns }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `POST /api/ugc/campaigns` — create a campaign.
#[utoipa::path(
    post,
    path = "/api/ugc/campaigns",
    tag = "UGC",
    summary = "create a campaign (brief, platforms, budget, payout rule, bonus tiers).",
    request_body = CampaignBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_campaign(
    State(ctx): State<UgcCtx>,
    Json(body): Json<CampaignBody>,
) -> (StatusCode, Json<Value>) {
    if let Err(e) = validate_campaign(&body) {
        return bad_request(&e);
    }
    let now = now_iso();
    let blank = Campaign {
        id: new_id("cmp"),
        brand: String::new(),
        brief: String::new(),
        status: CampaignStatus::Draft,
        platforms: Vec::new(),
        required_hashtags: Vec::new(),
        required_mentions: Vec::new(),
        starts_at: None,
        ends_at: None,
        budget_cents: 0,
        payout: PayoutRule::default(),
        bonus_tiers: Vec::new(),
        max_payout_per_creator_cents: 0,
        created_at: now.clone(),
        updated_at: now,
    };
    let campaign = apply_campaign(body, blank);
    match ctx.engine.store.upsert_campaign(&campaign).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "campaign": campaign }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `GET /api/ugc/campaigns/{id}` — one campaign's full record.
#[utoipa::path(
    get,
    path = "/api/ugc/campaigns/{id}",
    tag = "UGC",
    summary = "one campaign's full record, including its payout rule and bonus tiers.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_campaign(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.get_campaign(&id).await {
        Ok(Some(c)) => (StatusCode::OK, Json(json!({ "campaign": c }))),
        Ok(None) => not_found("campaign"),
        Err(e) => internal(&e.to_string()),
    }
}

/// `PUT /api/ugc/campaigns/{id}` — update a campaign.
#[utoipa::path(
    put,
    path = "/api/ugc/campaigns/{id}",
    tag = "UGC",
    summary = "update a campaign; reports how much money the new rule can and cannot re-price.",
    params(("id" = String, Path)),
    request_body = CampaignBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_campaign(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Json(body): Json<CampaignBody>,
) -> (StatusCode, Json<Value>) {
    if let Err(e) = validate_campaign(&body) {
        return bad_request(&e);
    }
    let existing = match ctx.engine.store.get_campaign(&id).await {
        Ok(Some(c)) => c,
        Ok(None) => return not_found("campaign"),
        Err(e) => return internal(&e.to_string()),
    };
    let campaign = apply_campaign(body, existing);
    if let Err(e) = ctx.engine.store.upsert_campaign(&campaign).await {
        return internal(&e.to_string());
    }
    // Changing the rule does NOT retroactively re-price money that has already
    // been signed off. Say which, in cents, so the operator does not have to
    // guess whether the edit moved what they were looking at.
    let summary = ctx.engine.store.campaign_summary(&id).await.ok().flatten();
    let (reprices, frozen) = summary.as_ref().map_or((0, 0), |s| {
        (s.accrued_cents, s.approved_cents + s.paid_cents)
    });
    (
        StatusCode::OK,
        Json(json!({
            "campaign": campaign,
            "repricing": {
                "accrued_cents": reprices,
                "frozen_cents": frozen,
                "note": "accrued payouts are re-priced on the next accrual pass; approved and paid payouts keep the amount that was signed off",
            }
        })),
    )
}

/// `DELETE /api/ugc/campaigns/{id}` — delete a campaign and everything under it.
#[utoipa::path(
    delete,
    path = "/api/ugc/campaigns/{id}",
    tag = "UGC",
    summary = "delete a campaign, cascading its submissions, their snapshots and its payouts; 409s while any of its payouts is paid.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_campaign(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.delete_campaign(&id).await {
        Ok(DeleteOutcome::Deleted) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(DeleteOutcome::NotFound) => not_found("campaign"),
        Ok(DeleteOutcome::PaidPayoutsRetained { paid }) => paid_payouts_retained(
            paid,
            "this campaign has paid payouts; they are its spend record and are never deleted — end the campaign instead",
        ),
        Err(e) => internal(&e.to_string()),
    }
}

/// `GET /api/ugc/campaigns/{id}/summary` — spend vs budget.
#[utoipa::path(
    get,
    path = "/api/ugc/campaigns/{id}/summary",
    tag = "UGC",
    summary = "spend vs budget, counter totals from each submission's LATEST snapshot.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn campaign_summary(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.campaign_summary(&id).await {
        Ok(Some(s)) => (
            StatusCode::OK,
            Json(serde_json::to_value(s).unwrap_or_default()),
        ),
        Ok(None) => not_found("campaign"),
        Err(e) => internal(&e.to_string()),
    }
}

/// Query params for the reads that page.
#[derive(Debug, Default, Deserialize)]
pub struct LimitQuery {
    /// Parsed leniently and clamped server-side — see [`parse_limit`].
    #[serde(default)]
    pub limit: Option<String>,
}

/// `GET /api/ugc/campaigns/{id}/leaderboard` — creators ranked by views.
#[utoipa::path(
    get,
    path = "/api/ugc/campaigns/{id}/leaderboard",
    tag = "UGC",
    summary = "creators on this campaign ranked by latest-snapshot views.",
    params(("id" = String, Path), ("limit" = Option<i64>, Query)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn campaign_leaderboard(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Query(q): Query<LimitQuery>,
) -> (StatusCode, Json<Value>) {
    match ctx
        .engine
        .store
        .campaign_leaderboard(&id, parse_limit(q.limit.as_deref()))
        .await
    {
        Ok(rows) => (StatusCode::OK, Json(json!({ "leaderboard": rows }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// Query params for the submission lists.
#[derive(Debug, Default, Deserialize)]
pub struct SubmissionQuery {
    #[serde(default)]
    pub campaign_id: Option<String>,
    #[serde(default)]
    pub creator_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub limit: Option<String>,
}

impl SubmissionQuery {
    fn into_filter(self) -> SubmissionFilter {
        SubmissionFilter {
            campaign_id: self.campaign_id,
            creator_id: self.creator_id,
            // Unknown status ⇒ no filter, same rule as the campaign list.
            status: self.status.as_deref().and_then(SubmissionStatus::parse),
            platform: self
                .platform
                .as_deref()
                .map(normalize_platform)
                .filter(|p| !p.is_empty()),
            limit: parse_limit(self.limit.as_deref()),
        }
    }
}

/// `GET /api/ugc/campaigns/{id}/submissions` — this campaign's submissions with
/// each one's latest counters and accrued money joined on.
///
/// `accrued_cents` / `payout_status` are `null` on a submission with no payout
/// row — "nothing has accrued yet", which is not the same fact as a payout
/// genuinely priced at 0.
#[utoipa::path(
    get,
    path = "/api/ugc/campaigns/{id}/submissions",
    tag = "UGC",
    summary = "this campaign's submissions, each with its latest metric snapshot and accrued payout.",
    params(("id" = String, Path), ("status" = Option<String>, Query)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn campaign_submissions(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Query(q): Query<SubmissionQuery>,
) -> (StatusCode, Json<Value>) {
    let mut filter = q.into_filter();
    // The path segment wins: this route is scoped to one campaign whatever the
    // query string says.
    filter.campaign_id = Some(id);
    match ctx
        .engine
        .store
        .list_submissions_with_metrics(&filter)
        .await
    {
        Ok(rows) => (StatusCode::OK, Json(json!({ "submissions": rows }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `POST /api/ugc/campaigns/{id}/refresh` — refresh every approved submission.
#[utoipa::path(
    post,
    path = "/api/ugc/campaigns/{id}/refresh",
    tag = "UGC",
    summary = "refresh every approved submission's metrics, then re-run accrual.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn refresh_campaign(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.get_campaign(&id).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found("campaign"),
        Err(e) => return internal(&e.to_string()),
    }
    match ctx
        .engine
        .refresh_campaign_for(&id, agent_grant(&headers).as_deref())
        .await
    {
        // 200 even when individual submissions failed: one platform being down
        // must never fail the batch or discard the snapshots that did land. The
        // report is serialized verbatim — `results` carries one line per approved
        // submission in the same shape the single-submission route answers with,
        // and `counts` splits `needs_connection` out of `error` so the panel can
        // say "link these accounts" instead of "3 failed".
        Ok(report) => (
            StatusCode::OK,
            Json(serde_json::to_value(report).unwrap_or_default()),
        ),
        Err(e) => internal(&e),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Creators
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for creating/updating a creator.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct CreatorBody {
    pub display_name: String,
    /// Platform key → handle. Keys are lowercased so they match
    /// `submissions.platform`.
    #[serde(default)]
    pub handles: BTreeMap<String, String>,
    #[serde(default)]
    pub contact_email: Option<String>,
    #[serde(default)]
    pub payout_handle: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

fn apply_creator(body: CreatorBody, mut creator: Creator) -> Creator {
    creator.display_name = body.display_name.trim().to_string();
    creator.handles = body
        .handles
        .into_iter()
        .map(|(k, v)| (normalize_platform(&k), v))
        .collect();
    creator.contact_email = body
        .contact_email
        .map(|e| e.trim().to_string())
        .filter(|e| !e.is_empty());
    creator.payout_handle = body.payout_handle;
    creator.notes = body.notes;
    creator.updated_at = now_iso();
    creator
}

/// Query params for the roster list.
#[derive(Debug, Default, Deserialize)]
pub struct CreatorQuery {
    #[serde(default)]
    pub q: Option<String>,
}

/// `GET /api/ugc/creators` — the roster.
#[utoipa::path(
    get,
    path = "/api/ugc/creators",
    tag = "UGC",
    summary = "the creator roster, optionally matched by name or contact email.",
    params(("q" = Option<String>, Query)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_creators(
    State(ctx): State<UgcCtx>,
    Query(q): Query<CreatorQuery>,
) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.list_creators(q.q.as_deref()).await {
        Ok(creators) => (StatusCode::OK, Json(json!({ "creators": creators }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `POST /api/ugc/creators` — add a creator.
#[utoipa::path(
    post,
    path = "/api/ugc/creators",
    tag = "UGC",
    summary = "add a creator to the roster.",
    request_body = CreatorBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_creator(
    State(ctx): State<UgcCtx>,
    Json(body): Json<CreatorBody>,
) -> (StatusCode, Json<Value>) {
    if body.display_name.trim().is_empty() {
        return bad_request("display_name is required");
    }
    let now = now_iso();
    let blank = Creator {
        id: new_id("crt"),
        display_name: String::new(),
        handles: BTreeMap::new(),
        contact_email: None,
        payout_handle: None,
        notes: None,
        created_at: now.clone(),
        updated_at: now,
    };
    let creator = apply_creator(body, blank);
    match ctx.engine.store.upsert_creator(&creator).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "creator": creator }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `GET /api/ugc/creators/{id}` — one creator plus their cross-campaign totals.
#[utoipa::path(
    get,
    path = "/api/ugc/creators/{id}",
    tag = "UGC",
    summary = "one creator, with submissions by state and accrued/paid money across campaigns.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_creator(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let creator = match ctx.engine.store.get_creator(&id).await {
        Ok(Some(c)) => c,
        Ok(None) => return not_found("creator"),
        Err(e) => return internal(&e.to_string()),
    };
    match ctx.engine.store.creator_totals(&id).await {
        Ok(totals) => (
            StatusCode::OK,
            Json(json!({ "creator": creator, "totals": totals })),
        ),
        Err(e) => internal(&e.to_string()),
    }
}

/// `PUT /api/ugc/creators/{id}` — update a creator.
#[utoipa::path(
    put,
    path = "/api/ugc/creators/{id}",
    tag = "UGC",
    summary = "update a creator's name, handles, contact or payout details.",
    params(("id" = String, Path)),
    request_body = CreatorBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_creator(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Json(body): Json<CreatorBody>,
) -> (StatusCode, Json<Value>) {
    if body.display_name.trim().is_empty() {
        return bad_request("display_name is required");
    }
    let existing = match ctx.engine.store.get_creator(&id).await {
        Ok(Some(c)) => c,
        Ok(None) => return not_found("creator"),
        Err(e) => return internal(&e.to_string()),
    };
    let creator = apply_creator(body, existing);
    match ctx.engine.store.upsert_creator(&creator).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "creator": creator }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// Query params for the creator delete.
#[derive(Debug, Default, Deserialize)]
pub struct ForceQuery {
    #[serde(default)]
    pub force: Option<String>,
}

/// `DELETE /api/ugc/creators/{id}` — remove a creator.
#[utoipa::path(
    delete,
    path = "/api/ugc/creators/{id}",
    tag = "UGC",
    summary = "remove a creator; 409s while they still have submissions unless force=true, and always while any of their payouts is paid.",
    params(("id" = String, Path), ("force" = Option<bool>, Query)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_creator(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Query(q): Query<ForceQuery>,
) -> (StatusCode, Json<Value>) {
    let force = q.force.as_deref().is_some_and(is_truthy);
    if !force {
        match ctx.engine.store.count_submissions_for_creator(&id).await {
            // Refusing by default is the point: cascading here would silently
            // delete paid payout rows and corrupt every campaign they belong to.
            Ok(n) if n > 0 => {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({
                        "error": format!("creator still has {n} submission(s); pass ?force=true to delete them and their unpaid payouts too"),
                        "submissions": n,
                    })),
                )
            }
            Ok(_) => {}
            Err(e) => return internal(&e.to_string()),
        }
    }
    // `force` cascades unpaid rows only: the store refuses the whole delete when
    // any of this creator's payouts is paid, so a manager cannot erase a
    // settlement record that only `ugc.payouts` could have created.
    match ctx.engine.store.delete_creator(&id, force).await {
        Ok(DeleteOutcome::Deleted) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(DeleteOutcome::NotFound) => not_found("creator"),
        Ok(DeleteOutcome::PaidPayoutsRetained { paid }) => paid_payouts_retained(
            paid,
            "this creator has paid payouts; they are campaign spend records and are never deleted, even with force=true",
        ),
        Err(e) => internal(&e.to_string()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Submissions
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for recording a submission.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct SubmissionBody {
    pub campaign_id: String,
    pub creator_id: String,
    pub platform: String,
    pub post_url: String,
}

/// `GET /api/ugc/submissions` — cross-campaign submission list, in the same
/// row shape [`campaign_submissions`] serves (latest snapshot + accrued money).
#[utoipa::path(
    get,
    path = "/api/ugc/submissions",
    tag = "UGC",
    summary = "cross-campaign submissions, newest first, filtered in SQL, with each one's accrued payout.",
    params(
        ("campaign_id" = Option<String>, Query),
        ("creator_id" = Option<String>, Query),
        ("status" = Option<String>, Query, description = "pending|approved|rejected|paid"),
        ("platform" = Option<String>, Query),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_submissions(
    State(ctx): State<UgcCtx>,
    Query(q): Query<SubmissionQuery>,
) -> (StatusCode, Json<Value>) {
    match ctx
        .engine
        .store
        .list_submissions_with_metrics(&q.into_filter())
        .await
    {
        Ok(rows) => (StatusCode::OK, Json(json!({ "submissions": rows }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `POST /api/ugc/submissions` — record a submission.
#[utoipa::path(
    post,
    path = "/api/ugc/submissions",
    tag = "UGC",
    summary = "record a creator's post against a campaign; a duplicate post is a 409.",
    request_body = SubmissionBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_submission(
    State(ctx): State<UgcCtx>,
    Json(body): Json<SubmissionBody>,
) -> (StatusCode, Json<Value>) {
    let platform = normalize_platform(&body.platform);
    if platform.is_empty() {
        return bad_request("platform is required");
    }
    let url = match screen_post_url(&body.post_url) {
        Ok(u) => u,
        Err(e) => return bad_request(&e),
    };
    // A curated label on a foreign host would be priced as one post and
    // deduplicated as another — see `check_label_matches_host`.
    if let Err(e) = check_label_matches_host(&platform, &url) {
        return bad_request(&e);
    }
    // Referential checks the schema does not do (there are no SQLite foreign
    // keys): without them a typo'd id silently creates a submission that belongs
    // to nothing and can never be paid.
    match ctx.engine.store.get_campaign(&body.campaign_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return bad_request("no campaign with that campaign_id"),
        Err(e) => return internal(&e.to_string()),
    }
    match ctx.engine.store.get_creator(&body.creator_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return bad_request("no creator with that creator_id"),
        Err(e) => return internal(&e.to_string()),
    }

    let now = now_iso();
    let submission = Submission {
        id: new_id("sub"),
        campaign_id: body.campaign_id,
        creator_id: body.creator_id,
        external_post_id: parse_post_id(&platform, &url).unwrap_or_default(),
        platform,
        post_url: url.to_string(),
        status: SubmissionStatus::Pending,
        submitted_at: now.clone(),
        reviewed_at: None,
        rejection_reason: None,
        created_at: now.clone(),
        updated_at: now,
    };
    match ctx.engine.create_submission(&submission).await {
        Ok(WriteOutcome::Written) => (StatusCode::OK, Json(json!({ "submission": submission }))),
        // The unique index is the only thing stopping one post being paid twice,
        // so this can never be a retry or a second row.
        Ok(WriteOutcome::DuplicatePost) => duplicate_post(),
        Ok(WriteOutcome::NotFound) => not_found("submission"),
        Err(e) => internal(&e),
    }
}

/// `GET /api/ugc/submissions/{id}` — one submission, its latest snapshot and its
/// payout row.
#[utoipa::path(
    get,
    path = "/api/ugc/submissions/{id}",
    tag = "UGC",
    summary = "one submission with its latest metric snapshot and its payout row, if any.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_submission(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let submission = match ctx.engine.store.get_submission(&id).await {
        Ok(Some(s)) => s,
        Ok(None) => return not_found("submission"),
        Err(e) => return internal(&e.to_string()),
    };
    let latest = ctx.engine.store.latest_snapshot(&id).await.ok().flatten();
    let payout = ctx
        .engine
        .store
        .payout_for_submission(&id)
        .await
        .ok()
        .flatten();
    // `SubmissionWithMetrics` flattens, so the submission's own fields sit at the
    // top level of `submission` with `latest` beside them; `payout` is a sibling
    // of `submission`, which is why it cannot collide with a submission field.
    //
    // The payout's money is ALSO folded into the row itself, from the row this
    // route already fetched — the list read joins it in, and a single read that
    // left those two keys null would contradict the list for the same submission.
    // The whole payout row stays a sibling because it carries fields (reason, the
    // stamps) the row shape deliberately does not.
    let with_metrics = SubmissionWithMetrics {
        submission,
        latest,
        accrued_cents: payout.as_ref().map(|p| p.amount_cents),
        payout_status: payout.as_ref().map(|p| p.status.as_str().to_string()),
    };
    (
        StatusCode::OK,
        Json(json!({ "submission": with_metrics, "payout": payout })),
    )
}

/// Request body for correcting a submission.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct SubmissionEditBody {
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub post_url: Option<String>,
}

/// `PUT /api/ugc/submissions/{id}` — correct a submission's platform or URL.
///
/// Status is untouched on purpose: review is its own endpoint so an edit can
/// never silently approve a post.
#[utoipa::path(
    put,
    path = "/api/ugc/submissions/{id}",
    tag = "UGC",
    summary = "correct a submission's platform or post url; never changes its review status.",
    params(("id" = String, Path)),
    request_body = SubmissionEditBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_submission(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Json(body): Json<SubmissionEditBody>,
) -> (StatusCode, Json<Value>) {
    let mut submission = match ctx.engine.store.get_submission(&id).await {
        Ok(Some(s)) => s,
        Ok(None) => return not_found("submission"),
        Err(e) => return internal(&e.to_string()),
    };
    if let Some(platform) = body.platform.as_deref() {
        let platform = normalize_platform(platform);
        if platform.is_empty() {
            return bad_request("platform cannot be blank");
        }
        submission.platform = platform;
    }
    if let Some(raw) = body.post_url.as_deref() {
        // The edit path is guarded exactly like create — otherwise the screen is
        // one PUT away from being bypassed.
        match screen_post_url(raw) {
            Ok(url) => {
                submission.external_post_id =
                    parse_post_id(&submission.platform, &url).unwrap_or_default();
                submission.post_url = url.to_string();
            }
            Err(e) => return bad_request(&e),
        }
    } else if body.platform.is_some() {
        // A platform change re-parses the existing URL: the same link yields a
        // different id (or none) under a different platform's rules.
        submission.external_post_id = Url::parse(&submission.post_url)
            .ok()
            .and_then(|u| parse_post_id(&submission.platform, &u))
            .unwrap_or_default();
    }
    if body.platform.is_some() || body.post_url.is_some() {
        // Same rule as create, on the row as it will be stored: a relabel or a
        // respelling must not produce a curated label on a foreign host either.
        if let Ok(url) = Url::parse(&submission.post_url) {
            if let Err(e) = check_label_matches_host(&submission.platform, &url) {
                return bad_request(&e);
            }
        }
    }
    submission.updated_at = now_iso();
    match ctx.engine.store.update_submission(&submission).await {
        Ok(WriteOutcome::Written) => (StatusCode::OK, Json(json!({ "submission": submission }))),
        Ok(WriteOutcome::DuplicatePost) => duplicate_post(),
        Ok(WriteOutcome::NotFound) => not_found("submission"),
        Err(e) => internal(&e.to_string()),
    }
}

/// `DELETE /api/ugc/submissions/{id}` — remove a submission.
#[utoipa::path(
    delete,
    path = "/api/ugc/submissions/{id}",
    tag = "UGC",
    summary = "delete a submission and its snapshots/payout; refuses once the payout is paid.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_submission(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    // Deleting a paid payout would remove money the campaign's spend is
    // reconciled against. This read only names the payout in the 409; the store
    // re-checks inside the delete's own transaction, which is the actual guard.
    match ctx.engine.store.payout_for_submission(&id).await {
        Ok(Some(p)) if p.status == PayoutStatus::Paid => {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "this submission's payout is already paid; deleting it would corrupt the campaign's spend",
                    "payout_id": p.id,
                })),
            )
        }
        Ok(_) => {}
        Err(e) => return internal(&e.to_string()),
    }
    match ctx.engine.store.delete_submission(&id).await {
        Ok(DeleteOutcome::Deleted) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(DeleteOutcome::NotFound) => not_found("submission"),
        Ok(DeleteOutcome::PaidPayoutsRetained { paid }) => paid_payouts_retained(
            paid,
            "this submission's payout is already paid; deleting it would corrupt the campaign's spend",
        ),
        Err(e) => internal(&e.to_string()),
    }
}

/// Request body for a review decision.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ReviewBody {
    /// `approve` or `reject`.
    pub decision: String,
    #[serde(default)]
    pub reason: Option<String>,
}

/// `POST /api/ugc/submissions/{id}/review` — approve or reject.
#[utoipa::path(
    post,
    path = "/api/ugc/submissions/{id}/review",
    tag = "UGC",
    summary = "approve or reject a submission; transition-gated, so a repeat decision is a no-op.",
    params(("id" = String, Path)),
    request_body = ReviewBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn review_submission(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Json(body): Json<ReviewBody>,
) -> (StatusCode, Json<Value>) {
    let approve = match body.decision.trim().to_ascii_lowercase().as_str() {
        "approve" | "approved" => true,
        "reject" | "rejected" => false,
        other => {
            return bad_request(&format!(
                "decision must be 'approve' or 'reject', got '{other}'"
            ))
        }
    };
    match ctx
        .engine
        .review_submission(&id, approve, body.reason)
        .await
    {
        // `changed: false` is a 200, not an error: a double-click in the panel is
        // a user being a user, and the event was already raised the first time.
        Ok(Some(outcome)) => (
            StatusCode::OK,
            Json(json!({
                "submission": outcome.submission,
                "payout": outcome.payout,
                "changed": outcome.changed,
            })),
        ),
        Ok(None) => not_found("submission"),
        Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e }))),
    }
}

/// `GET /api/ugc/submissions/{id}/metrics` — the snapshot history.
#[utoipa::path(
    get,
    path = "/api/ugc/submissions/{id}/metrics",
    tag = "UGC",
    summary = "this submission's metric snapshot history, newest first.",
    params(("id" = String, Path), ("limit" = Option<i64>, Query)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_metrics(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Query(q): Query<LimitQuery>,
) -> (StatusCode, Json<Value>) {
    match ctx
        .engine
        .store
        .list_snapshots(&id, parse_limit(q.limit.as_deref()))
        .await
    {
        Ok(snapshots) => (StatusCode::OK, Json(json!({ "snapshots": snapshots }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// Request body for a hand-entered snapshot.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct MetricsBody {
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
}

/// `POST /api/ugc/submissions/{id}/metrics` — record metrics by hand.
///
/// The escape hatch for a platform with no curated Composio source, and the
/// correction path for a bad automated read.
#[utoipa::path(
    post,
    path = "/api/ugc/submissions/{id}/metrics",
    tag = "UGC",
    summary = "append a hand-entered metric snapshot and re-run this submission's accrual.",
    params(("id" = String, Path)),
    request_body = MetricsBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn record_metrics(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    Json(body): Json<MetricsBody>,
) -> (StatusCode, Json<Value>) {
    if [
        body.views,
        body.likes,
        body.comments,
        body.shares,
        body.saves,
    ]
    .iter()
    .any(|v| *v < 0)
    {
        return bad_request("metric counters cannot be negative");
    }
    let sample = composio::MetricSample {
        views: body.views,
        likes: body.likes,
        comments: body.comments,
        shares: body.shares,
        saves: body.saves,
    };
    match ctx.engine.append_manual_snapshot(&id, sample).await {
        Ok(Some((snapshot, payout))) => (
            StatusCode::OK,
            Json(json!({ "snapshot": snapshot, "payout": payout })),
        ),
        Ok(None) => not_found("submission"),
        Err(e) => internal(&e),
    }
}

/// The body a single-submission refresh answers with.
///
/// It is [`SubmissionRefreshReport`] — the identical `status` / `message` /
/// `connect_url` / `snapshot` line the campaign-wide route puts in its `results`,
/// so one parser reads both — **serialized from the struct** rather than
/// hand-written here, which is what keeps the two routes from drifting.
///
/// `previous` and `payout` ride along as additive siblings because they exist only
/// on this route: the panel's money column reads them after a single refresh, and
/// they are `null` on a `needs_connection`, which wrote nothing and re-priced
/// nothing.
fn refresh_outcome_body(submission_id: &str, outcome: RefreshOutcome) -> Value {
    let (previous, payout) = match &outcome {
        RefreshOutcome::Refreshed {
            previous, payout, ..
        } => (previous.clone(), payout.clone()),
        RefreshOutcome::NeedsConnection { .. } => (None, None),
    };
    let mut body = serde_json::to_value(SubmissionRefreshReport::from_outcome(
        submission_id,
        outcome,
    ))
    .unwrap_or_default();
    if let Value::Object(map) = &mut body {
        map.insert("previous".to_string(), json!(previous));
        map.insert("payout".to_string(), json!(payout));
    }
    body
}

/// The body a failed single-submission refresh answers with, beside its 4xx/5xx.
///
/// Carries the full report line (`status: "error"`) **and** the `error` key every
/// other failing route in this file uses. Same string, twice, on purpose: the
/// `error` key is what the rest of this API's callers already read, and the report
/// shape is what lets the panel parse this route and the campaign route with one
/// function. It is one fact, not two.
fn refresh_error_body(submission_id: &str, error: &RefreshError) -> Value {
    let mut body = serde_json::to_value(SubmissionRefreshReport::from_error(submission_id, error))
        .unwrap_or_default();
    if let Value::Object(map) = &mut body {
        map.insert("error".to_string(), json!(error.to_string()));
    }
    body
}

/// `POST /api/ugc/submissions/{id}/refresh` — refresh through Composio.
#[utoipa::path(
    post,
    path = "/api/ugc/submissions/{id}/refresh",
    tag = "UGC",
    summary = "refresh this submission's metrics through the curated Composio action.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn refresh_submission(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    match ctx
        .engine
        .refresh_submission_for(&id, agent_grant(&headers).as_deref())
        .await
    {
        // BOTH outcomes are 200s. An account the operator has not linked yet is
        // `status: "needs_connection"` with a `connect_url` to send them to — not a
        // 502, because nothing is broken, and above all not a snapshot, because
        // there is no reading (five zeroes would re-price a live payout to nothing
        // on the next accrual pass).
        Ok(outcome) => (StatusCode::OK, Json(refresh_outcome_body(&id, outcome))),
        Err(e) => {
            let code = match &e {
                RefreshError::NotFound => StatusCode::NOT_FOUND,
                // A permanent fact about this row (no curated source / no parseable
                // post id) is the caller's problem to fix; an upstream failure is
                // not.
                RefreshError::Unsupported(_) => StatusCode::BAD_REQUEST,
                RefreshError::Upstream(_) => StatusCode::BAD_GATEWAY,
                RefreshError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (code, Json(refresh_error_body(&id, &e)))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Payouts
// ─────────────────────────────────────────────────────────────────────────────

/// Query params for the payout list.
#[derive(Debug, Default, Deserialize)]
pub struct PayoutQuery {
    #[serde(default)]
    pub campaign_id: Option<String>,
    #[serde(default)]
    pub creator_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<String>,
}

/// `GET /api/ugc/payouts` — list payouts.
#[utoipa::path(
    get,
    path = "/api/ugc/payouts",
    tag = "UGC",
    summary = "list payouts, filtered by campaign, creator or status.",
    params(
        ("campaign_id" = Option<String>, Query),
        ("creator_id" = Option<String>, Query),
        ("status" = Option<String>, Query, description = "accrued|approved|paid"),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_payouts(
    State(ctx): State<UgcCtx>,
    Query(q): Query<PayoutQuery>,
) -> (StatusCode, Json<Value>) {
    let filter = PayoutFilter {
        campaign_id: q.campaign_id,
        creator_id: q.creator_id,
        status: q.status.as_deref().and_then(PayoutStatus::parse),
        limit: parse_limit(q.limit.as_deref()),
    };
    match ctx.engine.store.list_payouts(&filter).await {
        Ok(payouts) => (StatusCode::OK, Json(json!({ "payouts": payouts }))),
        Err(e) => internal(&e.to_string()),
    }
}

/// `GET /api/ugc/payouts/{id}` — one payout with its context.
#[utoipa::path(
    get,
    path = "/api/ugc/payouts/{id}",
    tag = "UGC",
    summary = "one payout row with its campaign/creator/submission context and accrual reason.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_payout(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let payout = match ctx.engine.store.get_payout(&id).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found("payout"),
        Err(e) => return internal(&e.to_string()),
    };
    let campaign = ctx
        .engine
        .store
        .get_campaign(&payout.campaign_id)
        .await
        .ok()
        .flatten();
    let creator = ctx
        .engine
        .store
        .get_creator(&payout.creator_id)
        .await
        .ok()
        .flatten();
    let submission = match payout.submission_id.as_deref() {
        Some(sid) => ctx.engine.store.get_submission(sid).await.ok().flatten(),
        None => None,
    };
    (
        StatusCode::OK,
        Json(json!({
            "payout": payout,
            "campaign": campaign,
            "creator": creator,
            "submission": submission,
        })),
    )
}

/// `POST /api/ugc/payouts/{id}/approve` — accrued → approved.
#[utoipa::path(
    post,
    path = "/api/ugc/payouts/{id}/approve",
    tag = "UGC",
    summary = "move an accrued payout to approved; re-approving is a no-op, a paid row is a 409.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn approve_payout(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let payout = match ctx.engine.store.get_payout(&id).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found("payout"),
        Err(e) => return internal(&e.to_string()),
    };
    match payout.status {
        PayoutStatus::Approved => {
            return (
                StatusCode::OK,
                Json(json!({ "payout": payout, "changed": false })),
            )
        }
        // Money only moves forward. "Approving" a paid row would suggest the
        // amount is still editable, which it is not.
        PayoutStatus::Paid => {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "this payout has already been paid" })),
            )
        }
        PayoutStatus::Accrued => {}
    }
    match ctx
        .engine
        .store
        .set_payout_status(&id, PayoutStatus::Approved, &now_iso())
        .await
    {
        Ok(Some(p)) => (
            StatusCode::OK,
            Json(json!({ "payout": p, "changed": true })),
        ),
        Ok(None) => not_found("payout"),
        Err(e) => internal(&e.to_string()),
    }
}

/// `POST /api/ugc/payouts/{id}/paid` — approved → paid.
#[utoipa::path(
    post,
    path = "/api/ugc/payouts/{id}/paid",
    tag = "UGC",
    summary = "mark an approved payout paid and flip its submission to paid; refuses an accrued row.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn mark_payout_paid(
    State(ctx): State<UgcCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    let payout = match ctx.engine.store.get_payout(&id).await {
        Ok(Some(p)) => p,
        Ok(None) => return not_found("payout"),
        Err(e) => return internal(&e.to_string()),
    };
    match payout.status {
        PayoutStatus::Paid => {
            return (
                StatusCode::OK,
                Json(json!({ "payout": payout, "changed": false })),
            )
        }
        // Money never skips approval: an accrued amount is still moving as views
        // grow, so paying it would settle a number nobody signed off on.
        PayoutStatus::Accrued => {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "approve this payout before marking it paid" })),
            )
        }
        PayoutStatus::Approved => {}
    }
    let now = now_iso();
    let updated = match ctx
        .engine
        .store
        .set_payout_status(&id, PayoutStatus::Paid, &now)
        .await
    {
        Ok(Some(p)) => p,
        Ok(None) => return not_found("payout"),
        Err(e) => return internal(&e.to_string()),
    };
    // The submission follows the money: `paid` on a submission means "this post
    // has been settled", which is what the campaign's counts report.
    if let Some(sid) = updated.submission_id.as_deref() {
        // `set_submission_status` rewrites `reviewed_at` and `rejection_reason`
        // unconditionally, so the review stamp has to be read back and passed
        // through — settling money must not erase who cleared the post and when.
        // `rejection_reason` stays `None` on purpose: a paid post was approved.
        let reviewed = ctx.engine.store.get_submission(sid).await.ok().flatten();
        let reviewed_at = reviewed.as_ref().and_then(|s| s.reviewed_at.as_deref());
        if let Err(e) = ctx
            .engine
            .store
            .set_submission_status(sid, SubmissionStatus::Paid, reviewed_at, None, &now)
            .await
        {
            return internal(&e.to_string());
        }
    }
    (
        StatusCode::OK,
        Json(json!({ "payout": updated, "changed": true })),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Response helpers
// ─────────────────────────────────────────────────────────────────────────────

fn bad_request(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}

fn not_found(what: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("{what} not found") })),
    )
}

fn internal(msg: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": msg })),
    )
}

/// The one 409 that is a domain rule rather than a race: this post is already in
/// this campaign, and a second row would be paid a second time.
/// 409 for a delete the store refused because it would remove paid payouts.
fn paid_payouts_retained(paid: i64, message: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": message, "paid_payouts": paid })),
    )
}

fn duplicate_post() -> (StatusCode, Json<Value>) {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "this post has already been submitted to this campaign",
        })),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Input helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Platform keys are stored lowercased — they are the lookup key into
/// [`composio::PLATFORM_METRIC_SOURCES`], so `TikTok` and `tiktok` must not be
/// two different platforms.
fn normalize_platform(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

/// Parse `?limit=` leniently. Garbage means "no opinion" (the store's own default
/// applies) rather than a 400 — a paging hint is not worth failing a read over.
/// The value is clamped by [`crate::clamp_limit`] inside the store; nothing here
/// re-derives a cap.
fn parse_limit(raw: Option<&str>) -> Option<i64> {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i64>().ok())
}

/// `?force=` / `?flag=` truthiness, matching what a URL actually carries.
fn is_truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

// ── SSRF screen for a submitted post URL ─────────────────────────────────────
//
// VENDORED from `apps-store/monitors/backend/src/net_guard.rs` (this crate must
// not depend on monitors — every apps-store app is a standalone satellite).
// Only the SCREENING half is vendored: this app never fetches a post URL, so
// there is no resolve-and-pin step, no redirect chain and no DNS in the request
// path. What survives is the part that still matters — a post URL is stored,
// rendered as a link and shown to whoever reviews the submission, so
// `https://169.254.169.254/latest/meta-data/` or `https://localhost:7981/api/...`
// must never be accepted as one.

/// Cloud-metadata hostnames that must never be accepted, in addition to the
/// 169.254.169.254 literal already screened by [`is_blocked_ip`].
const BLOCKED_HOSTS: &[&str] = &[
    "metadata.google.internal",
    "metadata.goog",
    "metadata",
    "localhost",
];

/// Private/internal DNS suffixes. A real creator post never lives under one.
const BLOCKED_HOST_SUFFIXES: &[&str] = &[".internal", ".local", ".localdomain", ".localhost"];

/// SSRF guard for a single IPv4 literal: loopback (127/8), RFC1918 private,
/// link-local (169.254/16 — the cloud metadata endpoint), unspecified, the
/// 0.0.0.0/8 block, broadcast, and CGNAT shared space (100.64/10).
fn is_blocked_ipv4(v4: std::net::Ipv4Addr) -> bool {
    let o = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || o[0] == 0
        || (o[0] == 100 && (o[1] & 0xc0) == 0x40)
}

/// SSRF guard for a single IP literal. Rejects loopback / private / link-local
/// for both families, IPv6 unique-local (fc00::/7) and link-local (fe80::/10),
/// and any IPv4-mapped form of a blocked v4 address.
fn is_blocked_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => is_blocked_ipv4(v4),
        std::net::IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_ipv4(mapped);
            }
            let seg0 = v6.segments()[0];
            (seg0 & 0xfe00) == 0xfc00 || (seg0 & 0xffc0) == 0xfe80
        }
    }
}

/// Screen and normalise a submitted post URL.
///
/// **https only** — unlike monitors, which legitimately watches plain-http
/// pages. Every platform this app supports serves posts over TLS, so `http://`
/// here is either a typo or an attempt to point the reviewer somewhere else.
///
/// # Errors
/// A message written for the person pasting the link, not for a log.
pub fn screen_post_url(raw: &str) -> Result<Url, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("post_url is required".to_string());
    }
    let url = Url::parse(trimmed).map_err(|e| format!("invalid post url: {e}"))?;
    if url.scheme() != "https" {
        return Err("post url must be https".to_string());
    }
    let host = url
        .host_str()
        .ok_or_else(|| "post url has no host".to_string())?;
    if host.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("post url host contains control or whitespace characters".to_string());
    }
    if !host.is_ascii() {
        // Homograph defence: a non-ASCII host is either an IDN (which the `url`
        // crate has already punycoded, so it would be ASCII here) or a trick.
        return Err("non-ASCII post url host is not allowed".to_string());
    }
    let bare = host.strip_suffix('.').unwrap_or(host);
    let lower = bare.to_ascii_lowercase();
    let unbracketed = lower.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = unbracketed.parse::<std::net::IpAddr>() {
        if is_blocked_ip(ip) {
            return Err("post url points at a private or loopback address".to_string());
        }
        return Ok(url);
    }
    if BLOCKED_HOSTS
        .iter()
        .any(|deny| lower == *deny || lower.ends_with(&format!(".{deny}")))
        || BLOCKED_HOST_SUFFIXES
            .iter()
            .any(|suffix| lower.ends_with(suffix))
    {
        return Err("post url points at an internal host".to_string());
    }
    match url::Host::parse(bare) {
        Ok(parsed) if parsed.to_string().eq_ignore_ascii_case(bare) => Ok(url),
        Ok(_) => Err("post url host failed its IDNA round-trip".to_string()),
        Err(e) => Err(format!("invalid post url host: {e}")),
    }
}

/// Extract the platform-native post id from a screened post URL.
///
/// Returns `None` when the URL's HOST does not belong to `platform` (see
/// [`platform_for_host`]), when the URL does not look like a post of that
/// platform, or when the extracted id is not a plain path segment — that id is
/// the ONE dynamic value this app ever hands to a Composio action, so
/// [`composio::id_segment_is_safe`] is the last gate before it is stored.
///
/// The host gate is what keeps the metrics identity (`platform`,
/// `external_post_id`) and the deduplication identity
/// (`store::submission_dedup_key`, derived from the host) the same thing: a
/// `youtube` row on `evil.example/watch?v=X` must never refresh — and be priced
/// from — real YouTube video `X` while deduplicating as an `evil.example` link.
/// Path segments are percent-decoded first, so `youtu.be/%61bc` is `abc`.
///
/// `None` becomes `""` on the row: the submission stays recordable and
/// reviewable, it just cannot auto-refresh. Deduplication does not depend on
/// this id or on the caller's platform label alone — the store also keys every
/// row by the post its URL names ([`post_identity`] / its canonical URL, see
/// `store::submission_dedup_key`), so the same link 409s under any label.
pub fn parse_post_id(platform: &str, url: &Url) -> Option<String> {
    if platform_for_host(url) != Some(platform) {
        return None;
    }
    let segments: Vec<String> = url
        .path_segments()
        .map(|s| {
            s.filter(|p| !p.is_empty())
                .map(percent_decode_segment)
                .collect()
        })
        .unwrap_or_default();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();

    let candidate: Option<String> = match platform {
        "youtube" => {
            if host_is(url, "youtu.be") {
                segments.first().map(|s| (*s).to_string())
            } else if let Some(v) = url
                .query_pairs()
                .find(|(k, _)| k.eq_ignore_ascii_case("v"))
                .map(|(_, v)| v)
            {
                Some(v.into_owned())
            } else {
                // /shorts/<id>, /live/<id>, /embed/<id>, and the legacy /v/<id>
                after_marker(&segments, &["shorts", "live", "embed", "v"])
            }
        }
        // /@handle/video/<id>, or a vm.tiktok.com/<code> short link.
        "tiktok" => after_marker(&segments, &["video", "photo"])
            .or_else(|| (segments.len() == 1).then(|| segments[0].to_string())),
        // /p/<code>, /reel/<code>, /reels/<code>, /tv/<code>
        "instagram" => after_marker(&segments, &["p", "reel", "reels", "tv"]),
        // /<user>/status/<id>
        "x" => after_marker(&segments, &["status", "statuses"]),
        // /posts/<slug>, or /feed/update/urn:li:activity:<n> — the numeric tail
        // of the URN is what is a safe segment, and it is the id the analytics
        // action wants.
        "linkedin" => after_marker(&segments, &["posts", "update", "activity"])
            .map(|s| s.rsplit(':').next().unwrap_or(&s).to_string()),
        _ => None,
    };

    candidate
        .map(|c| c.trim().to_string())
        .filter(|c| composio::id_segment_is_safe(c))
}

/// Percent-decode one URL path segment (`%61bc` -> `abc`). Invalid escapes are
/// kept literally; a decoded byte sequence that is not UTF-8 is replaced, which
/// [`composio::id_segment_is_safe`] then rejects.
fn percent_decode_segment(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(b: u8) -> Option<u8> {
    char::from(b)
        .to_digit(16)
        .and_then(|d| u8::try_from(d).ok())
}

/// Curated post hosts and the platform each belongs to. A host matches exactly
/// or as a dot-suffix (`m.youtube.com`, `vm.tiktok.com`), never as a bare suffix
/// (`notyoutube.com` is not YouTube).
const CURATED_POST_HOSTS: &[(&str, &str)] = &[
    ("youtube.com", "youtube"),
    ("youtu.be", "youtube"),
    ("youtube-nocookie.com", "youtube"),
    ("tiktok.com", "tiktok"),
    ("instagram.com", "instagram"),
    ("instagr.am", "instagram"),
    ("x.com", "x"),
    ("twitter.com", "x"),
    ("linkedin.com", "linkedin"),
    ("lnkd.in", "linkedin"),
];

/// Does this URL's host equal `domain` or sit under it as a subdomain?
fn host_is(url: &Url, domain: &str) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|rest| rest.ends_with('.'))
}

/// The curated platform a post URL's HOST belongs to, whatever platform label
/// the caller chose. `None` for any other host.
///
/// The label on a submission is free text, so it can never be part of what
/// makes two submissions "the same post": the host decides that.
#[must_use]
pub fn platform_for_host(url: &Url) -> Option<&'static str> {
    CURATED_POST_HOSTS
        .iter()
        .find(|(domain, _)| host_is(url, domain))
        .map(|(_, platform)| *platform)
}

/// Refuse a submission whose label names a curated platform while its URL is on
/// another site (`youtube` + `https://evil.example/watch?v=X`).
///
/// Such a row would be priced as one post (a refresh reads the label) while
/// being deduplicated as another (the key reads the host), so the same YouTube
/// video could be submitted once as that row and again as `youtu.be/X` and paid
/// twice. Uncurated labels (`other`, `myspace`, …) carry no metrics source and
/// are accepted on any screened host.
///
/// # Errors
/// A message written for the person pasting the link.
pub fn check_label_matches_host(platform: &str, url: &Url) -> Result<(), String> {
    let curated = CURATED_POST_HOSTS.iter().any(|(_, p)| *p == platform);
    if curated && platform_for_host(url) != Some(platform) {
        return Err(format!(
            "a '{platform}' submission must link to a {platform} post; use another platform \
             label for a link on a different site"
        ));
    }
    Ok(())
}

/// The label-independent identity of the post a URL names: the host's curated
/// platform and that platform's native post id (`("youtube", "abc123")` for both
/// `youtu.be/abc123` and `youtube.com/watch?v=abc123`), or `None` when the host
/// is not curated or the URL is not a post shape.
#[must_use]
pub fn post_identity(url: &Url) -> Option<(&'static str, String)> {
    let platform = platform_for_host(url)?;
    parse_post_id(platform, url).map(|id| (platform, id))
}

/// The segment following the first occurrence of any `marker`.
fn after_marker(segments: &[&str], markers: &[&str]) -> Option<String> {
    segments
        .iter()
        .position(|s| markers.iter().any(|m| s.eq_ignore_ascii_case(m)))
        .and_then(|i| segments.get(i + 1))
        .map(|s| (*s).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composio::{MetricOutcome, MetricSample};
    use crate::testutil::test_engine;
    use crate::{CampaignRefreshReport, MetricSnapshot, MetricSource, PayoutStatus, RefreshCounts};

    fn ctx() -> UgcCtx {
        UgcCtx::new(test_engine())
    }

    fn campaign_body(brand: &str, payout: PayoutRule) -> CampaignBody {
        CampaignBody {
            brand: brand.to_string(),
            brief: "post a clip".to_string(),
            platforms: vec!["YouTube".to_string()],
            payout,
            ..CampaignBody::default()
        }
    }

    fn creator_body(name: &str) -> CreatorBody {
        CreatorBody {
            display_name: name.to_string(),
            ..CreatorBody::default()
        }
    }

    /// Create a campaign + creator and return their ids.
    async fn seed(ctx: &UgcCtx) -> (String, String) {
        let (_, Json(c)) = create_campaign(
            State(ctx.clone()),
            Json(campaign_body("Acme", PayoutRule::Cpm { cpm_cents: 250 })),
        )
        .await;
        let (_, Json(k)) = create_creator(State(ctx.clone()), Json(creator_body("Ada"))).await;
        (
            c["campaign"]["id"].as_str().unwrap().to_string(),
            k["creator"]["id"].as_str().unwrap().to_string(),
        )
    }

    async fn submit(ctx: &UgcCtx, campaign: &str, creator: &str, url: &str) -> (StatusCode, Value) {
        let (code, Json(body)) = create_submission(
            State(ctx.clone()),
            Json(SubmissionBody {
                campaign_id: campaign.to_string(),
                creator_id: creator.to_string(),
                platform: "youtube".to_string(),
                post_url: url.to_string(),
            }),
        )
        .await;
        (code, body)
    }

    // ---- the happy path, resource by resource ------------------------------

    #[tokio::test]
    async fn campaign_creator_submission_review_payout_roundtrip() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;

        // The campaign is listed, and an unknown ?status= lists everything
        // rather than answering "you have none".
        let (_, Json(listed)) = list_campaigns(
            State(ctx.clone()),
            Query(CampaignQuery {
                status: Some("nonsense".into()),
            }),
        )
        .await;
        assert_eq!(listed["campaigns"].as_array().unwrap().len(), 1);

        let (code, body) = submit(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://youtu.be/dQw4w9WgXcQ",
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let submission_id = body["submission"]["id"].as_str().unwrap().to_string();
        assert_eq!(body["submission"]["external_post_id"], "dQw4w9WgXcQ");

        // Hand-entered metrics, then approval, then the accrued payout.
        let (code, _) = record_metrics(
            State(ctx.clone()),
            Path(submission_id.clone()),
            Json(MetricsBody {
                views: 41_200,
                ..MetricsBody::default()
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);

        let (code, Json(reviewed)) = review_submission(
            State(ctx.clone()),
            Path(submission_id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(reviewed["changed"], true);
        // 41 200 views at 250c/1k floors to 10 300c.
        assert_eq!(reviewed["payout"]["amount_cents"], 10_300);
        let payout_id = reviewed["payout"]["id"].as_str().unwrap().to_string();

        // Re-approving is a 200 no-op, not a second payout.
        let (code, Json(again)) = review_submission(
            State(ctx.clone()),
            Path(submission_id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(again["changed"], false);

        // Money never skips approval.
        let (code, _) = mark_payout_paid(State(ctx.clone()), Path(payout_id.clone())).await;
        assert_eq!(code, StatusCode::CONFLICT);
        let (code, _) = approve_payout(State(ctx.clone()), Path(payout_id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        let (code, Json(paid)) =
            mark_payout_paid(State(ctx.clone()), Path(payout_id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(paid["payout"]["status"], "paid");

        // Paying flips the submission, and a paid payout locks the delete path.
        let (_, Json(one)) = get_submission(State(ctx.clone()), Path(submission_id.clone())).await;
        assert_eq!(one["submission"]["status"], "paid");
        // Settling money must not erase the review stamp: `set_submission_status`
        // rewrites that column unconditionally, so the paid transition has to
        // carry it through.
        assert!(
            one["submission"]["reviewed_at"].is_string(),
            "paying kept the approval's reviewed_at: {one}"
        );
        // The flattened shape: the submission's own fields sit at the top level.
        assert_eq!(one["submission"]["latest"]["views"], 41_200);
        assert_eq!(one["submission"]["latest"]["source"], "manual");
        // The money rides the row itself, so the panel's Accrued column fills from
        // the list read alone — and the single read agrees with it rather than
        // being the only place a payout shows up.
        assert_eq!(one["submission"]["accrued_cents"], 10_300);
        assert_eq!(one["submission"]["payout_status"], "paid");
        let (_, Json(listed)) = campaign_submissions(
            State(ctx.clone()),
            Path(campaign_id.clone()),
            Query(SubmissionQuery::default()),
        )
        .await;
        let row = &listed["submissions"][0];
        assert_eq!(row["accrued_cents"], 10_300, "{listed}");
        assert_eq!(row["payout_status"], "paid", "{listed}");

        let (code, _) = delete_submission(State(ctx.clone()), Path(submission_id)).await;
        assert_eq!(code, StatusCode::CONFLICT);

        // And the campaign summary reports the spend.
        let (_, Json(summary)) = campaign_summary(State(ctx), Path(campaign_id)).await;
        assert_eq!(summary["paid_cents"], 10_300);
        assert_eq!(
            summary["remaining_cents"],
            Value::Null,
            "0 budget = uncapped"
        );
    }

    #[tokio::test]
    async fn a_duplicate_post_is_a_409_not_a_second_row() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
        let (code, _) = submit(&ctx, &campaign_id, &creator_id, url).await;
        assert_eq!(code, StatusCode::OK);
        // Same post, a different URL spelling — the parsed id is what collides.
        let (code, body) = submit(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://youtu.be/dQw4w9WgXcQ",
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");
    }

    /// The audit's exploit path: a screened link no platform parser recognises,
    /// submitted twice (by two creators) to one campaign. The second must be a 409
    /// — not a second reviewable row that a manager can approve into a second
    /// payable obligation for the same post.
    #[tokio::test]
    async fn an_unparseable_link_submitted_twice_is_a_409_and_accrues_once() {
        let ctx = ctx();
        let (_, Json(c)) = create_campaign(
            State(ctx.clone()),
            Json(campaign_body("Acme", PayoutRule::Flat { flat_cents: 5000 })),
        )
        .await;
        let campaign_id = c["campaign"]["id"].as_str().unwrap().to_string();
        let (_, Json(a)) = create_creator(State(ctx.clone()), Json(creator_body("Ada"))).await;
        let (_, Json(b)) = create_creator(State(ctx.clone()), Json(creator_body("Bob"))).await;
        let creator_a = a["creator"]["id"].as_str().unwrap().to_string();
        let creator_b = b["creator"]["id"].as_str().unwrap().to_string();

        let url = "https://www.youtube.com/feed/subscriptions";
        let (code, first) = submit(&ctx, &campaign_id, &creator_a, url).await;
        assert_eq!(code, StatusCode::OK, "{first}");
        assert_eq!(first["submission"]["external_post_id"], "", "{first}");
        let (code, body) = submit(&ctx, &campaign_id, &creator_b, url).await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");
        // A cosmetic respelling of the same link is the same post.
        let (code, body) = submit(
            &ctx,
            &campaign_id,
            &creator_b,
            "https://youtube.com/feed/subscriptions/?utm_source=share#top",
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");

        let id = first["submission"]["id"].as_str().unwrap().to_string();
        let (code, _) = review_submission(
            State(ctx.clone()),
            Path(id),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let payouts = ctx
            .engine
            .store
            .list_payouts(&PayoutFilter {
                campaign_id: Some(campaign_id),
                ..PayoutFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(payouts.len(), 1, "one link, one payout obligation");
        assert_eq!(payouts[0].amount_cents, 5000);
    }

    /// The platform label is caller-supplied free text, so it can never decide
    /// whether two submissions are the same post. Resending one link — parsed or
    /// unparsed, respelled or not — under another label (including one the
    /// campaign does not list) is a 409 on create AND on edit, and approving the
    /// survivor accrues exactly one payout.
    #[tokio::test]
    async fn the_same_link_under_another_platform_label_is_a_409_and_accrues_once() {
        let ctx = ctx();
        let (_, Json(c)) = create_campaign(
            State(ctx.clone()),
            Json(campaign_body("Acme", PayoutRule::Flat { flat_cents: 5000 })),
        )
        .await;
        let campaign_id = c["campaign"]["id"].as_str().unwrap().to_string();
        let (_, Json(k)) = create_creator(State(ctx.clone()), Json(creator_body("Ada"))).await;
        let creator_id = k["creator"]["id"].as_str().unwrap().to_string();
        let send = |platform: &str, url: &str| {
            let ctx = ctx.clone();
            let body = SubmissionBody {
                campaign_id: campaign_id.clone(),
                creator_id: creator_id.clone(),
                platform: platform.to_string(),
                post_url: url.to_string(),
            };
            async move {
                let (code, Json(out)) = create_submission(State(ctx), Json(body)).await;
                (code, out)
            }
        };

        let unparsed = "https://www.youtube.com/feed/subscriptions";
        let (code, first) = send("youtube", unparsed).await;
        assert_eq!(code, StatusCode::OK, "{first}");
        for (platform, url) in [
            ("other", unparsed),
            ("myspace", unparsed),
            (
                "OTHER",
                "https://youtube.com/feed/subscriptions/?utm_source=x",
            ),
        ] {
            let (code, body) = send(platform, url).await;
            assert_eq!(code, StatusCode::CONFLICT, "{platform} {url}: {body}");
        }

        let (code, parsed) = send("youtube", "https://youtu.be/abc123").await;
        assert_eq!(code, StatusCode::OK, "{parsed}");
        assert_eq!(parsed["submission"]["external_post_id"], "abc123");
        for (platform, url) in [
            ("other", "https://youtu.be/abc123"),
            ("myspace", "https://youtu.be/abc123"),
            ("other", "https://www.youtube.com/watch?v=abc123"),
            ("blog", "https://m.youtube.com/shorts/abc123"),
        ] {
            let (code, body) = send(platform, url).await;
            assert_eq!(code, StatusCode::CONFLICT, "{platform} {url}: {body}");
        }
        // Another CURATED platform's label on a YouTube link is refused outright
        // (it names a metrics source the link is not on), so it cannot add a row
        // either.
        for (platform, url) in [
            ("tiktok", unparsed),
            ("tiktok", "https://youtu.be/abc123"),
            ("instagram", "https://m.youtube.com/shorts/abc123"),
        ] {
            let (code, body) = send(platform, url).await;
            assert_eq!(code, StatusCode::BAD_REQUEST, "{platform} {url}: {body}");
        }

        // The edit path cannot relabel or respell a row into an existing post.
        let (code, third) = send("other", "https://example.test/post/1").await;
        assert_eq!(code, StatusCode::OK, "{third}");
        let third_id = third["submission"]["id"].as_str().unwrap().to_string();
        let (code, body) = update_submission(
            State(ctx.clone()),
            Path(third_id.clone()),
            Json(SubmissionEditBody {
                platform: Some("other".into()),
                post_url: Some("https://youtube.com/watch?v=abc123".into()),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "{body:?}");
        // Relabelling a row on its own URL is still a legitimate correction.
        let first_id = first["submission"]["id"].as_str().unwrap().to_string();
        let (code, body) = update_submission(
            State(ctx.clone()),
            Path(first_id.clone()),
            Json(SubmissionEditBody {
                platform: Some("other".into()),
                post_url: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{body:?}");

        let parsed_id = parsed["submission"]["id"].as_str().unwrap().to_string();
        for id in [&first_id, &parsed_id] {
            let (code, _) = review_submission(
                State(ctx.clone()),
                Path(id.clone()),
                Json(ReviewBody {
                    decision: "approve".into(),
                    reason: None,
                }),
            )
            .await;
            assert_eq!(code, StatusCode::OK);
        }
        let payouts = ctx
            .engine
            .store
            .list_payouts(&PayoutFilter {
                campaign_id: Some(campaign_id.clone()),
                ..PayoutFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(payouts.len(), 2, "two posts, two payout obligations");
        let mut paid_for: Vec<_> = payouts
            .iter()
            .map(|p| p.submission_id.clone().unwrap())
            .collect();
        paid_for.sort();
        let mut expected = vec![first_id, parsed_id];
        expected.sort();
        assert_eq!(paid_for, expected);
        assert!(payouts.iter().all(|p| p.amount_cents == 5000));
        let (_, Json(listed)) = list_submissions(
            State(ctx),
            Query(SubmissionQuery {
                campaign_id: Some(campaign_id),
                ..SubmissionQuery::default()
            }),
        )
        .await;
        assert_eq!(
            listed["submissions"].as_array().unwrap().len(),
            3,
            "{listed}"
        );
    }

    /// Approve every submission in `campaign_id` and return its payouts.
    async fn approve_all_and_list_payouts(ctx: &UgcCtx, campaign_id: &str) -> Vec<crate::Payout> {
        let rows = ctx
            .engine
            .store
            .list_submissions(&crate::SubmissionFilter {
                campaign_id: Some(campaign_id.to_string()),
                ..crate::SubmissionFilter::default()
            })
            .await
            .unwrap();
        for row in rows {
            let (code, _) = review_submission(
                State(ctx.clone()),
                Path(row.id.clone()),
                Json(ReviewBody {
                    decision: "approve".into(),
                    reason: None,
                }),
            )
            .await;
            assert_eq!(code, StatusCode::OK);
        }
        ctx.engine
            .store
            .list_payouts(&PayoutFilter {
                campaign_id: Some(campaign_id.to_string()),
                ..PayoutFilter::default()
            })
            .await
            .unwrap()
    }

    /// A flat campaign plus one creator, and a sender bound to them.
    async fn flat_campaign(ctx: &UgcCtx) -> (String, String) {
        let (_, Json(c)) = create_campaign(
            State(ctx.clone()),
            Json(campaign_body("Acme", PayoutRule::Flat { flat_cents: 5000 })),
        )
        .await;
        let (_, Json(k)) = create_creator(State(ctx.clone()), Json(creator_body("Ada"))).await;
        (
            c["campaign"]["id"].as_str().unwrap().to_string(),
            k["creator"]["id"].as_str().unwrap().to_string(),
        )
    }

    async fn send_as(
        ctx: &UgcCtx,
        campaign_id: &str,
        creator_id: &str,
        platform: &str,
        url: &str,
    ) -> (StatusCode, Value) {
        let (code, Json(out)) = create_submission(
            State(ctx.clone()),
            Json(SubmissionBody {
                campaign_id: campaign_id.to_string(),
                creator_id: creator_id.to_string(),
                platform: platform.to_string(),
                post_url: url.to_string(),
            }),
        )
        .await;
        (code, out)
    }

    /// Round-2 exploit: a `youtube` row on `evil.example/watch?v=abc123` used to
    /// store `external_post_id = abc123` (so it refreshed and was priced from real
    /// YouTube video abc123) while deduplicating as an `evil.example` link, so
    /// `youtu.be/abc123` under another label was a second row and both paid.
    /// In both orders, and through PUT relabel/respell, one YouTube post now
    /// yields exactly one row that refreshes it and exactly one payout.
    #[tokio::test]
    async fn a_curated_label_on_a_foreign_host_cannot_pay_one_post_twice() {
        let evil = "https://evil.example/watch?v=abc123";
        let real = "https://youtu.be/abc123";
        for order in [
            [("youtube", evil), ("other", real)],
            [("other", real), ("youtube", evil)],
        ] {
            let ctx = ctx();
            let (campaign_id, creator_id) = flat_campaign(&ctx).await;
            let mut accepted = 0;
            for (platform, url) in order {
                let (code, body) = send_as(&ctx, &campaign_id, &creator_id, platform, url).await;
                if platform == "youtube" {
                    assert_eq!(code, StatusCode::BAD_REQUEST, "{platform} {url}: {body}");
                } else {
                    assert_eq!(code, StatusCode::OK, "{platform} {url}: {body}");
                    accepted += 1;
                }
            }
            assert_eq!(accepted, 1);
            // The surviving row, relabelled youtube on its own YouTube link, is the
            // one row that refreshes abc123; any other label still 409s.
            let (code, body) = send_as(&ctx, &campaign_id, &creator_id, "youtube", real).await;
            assert_eq!(code, StatusCode::CONFLICT, "{body}");
            let payouts = approve_all_and_list_payouts(&ctx, &campaign_id).await;
            assert_eq!(
                payouts.len(),
                1,
                "one YouTube post, one payout: {payouts:?}"
            );
            assert_eq!(payouts[0].amount_cents, 5000);
        }

        // PUT: an uncurated-label row on the foreign host is a different link (it
        // never refreshes YouTube), but relabelling it youtube — or respelling a
        // youtube row onto the foreign host — is refused just like create.
        let ctx = ctx();
        let (campaign_id, creator_id) = flat_campaign(&ctx).await;
        let (code, real_row) = send_as(&ctx, &campaign_id, &creator_id, "youtube", real).await;
        assert_eq!(code, StatusCode::OK, "{real_row}");
        assert_eq!(real_row["submission"]["external_post_id"], "abc123");
        let (code, foreign) = send_as(&ctx, &campaign_id, &creator_id, "other", evil).await;
        assert_eq!(code, StatusCode::OK, "{foreign}");
        assert_eq!(
            foreign["submission"]["external_post_id"], "",
            "a foreign host never carries a curated post id: {foreign}"
        );
        let foreign_id = foreign["submission"]["id"].as_str().unwrap().to_string();
        let (code, body) = update_submission(
            State(ctx.clone()),
            Path(foreign_id.clone()),
            Json(SubmissionEditBody {
                platform: Some("youtube".into()),
                post_url: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{body:?}");
        let real_id = real_row["submission"]["id"].as_str().unwrap().to_string();
        let (code, body) = update_submission(
            State(ctx.clone()),
            Path(real_id.clone()),
            Json(SubmissionEditBody {
                platform: None,
                post_url: Some(evil.into()),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{body:?}");
        // Nothing was rewritten by the refused edits.
        let stored = ctx
            .engine
            .store
            .get_submission(&foreign_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (stored.platform.as_str(), stored.external_post_id.as_str()),
            ("other", "")
        );
        let stored = ctx
            .engine
            .store
            .get_submission(&real_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.post_url, real);
        // Exactly one stored row claims YouTube video abc123's metrics.
        let rows = ctx
            .engine
            .store
            .list_submissions(&crate::SubmissionFilter {
                campaign_id: Some(campaign_id.clone()),
                ..crate::SubmissionFilter::default()
            })
            .await
            .unwrap();
        let refreshing: Vec<_> = rows
            .iter()
            .filter(|r| r.external_post_id == "abc123")
            .map(|r| r.id.clone())
            .collect();
        assert_eq!(refreshing, vec![real_id]);
    }

    /// Respellings of one curated post the parser did not use to recognise —
    /// percent-encoded unreserved characters, the legacy `/v/<id>` shape, an
    /// upper-case `V=` key — and, on any host, duplicate slashes and encoded
    /// path characters: each is the same post, a 409 on create and on PUT.
    #[tokio::test]
    async fn percent_encoded_and_legacy_respellings_are_the_same_post() {
        let ctx = ctx();
        let (campaign_id, creator_id) = flat_campaign(&ctx).await;
        let (code, first) = send_as(
            &ctx,
            &campaign_id,
            &creator_id,
            "other",
            "https://youtu.be/abc123",
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{first}");
        for (platform, url) in [
            ("other", "https://youtu.be/%61bc123"),
            ("youtube", "https://youtu.be/%61bc123"),
            ("youtube", "https://www.youtube.com/shorts/%61bc123"),
            ("other", "https://youtube.com/shorts/%61bc123/"),
            ("youtube", "https://www.youtube.com/v/abc123"),
            ("other", "https://www.youtube.com/watch?V=abc123"),
            ("youtube", "https://www.youtube.com//shorts//abc123"),
        ] {
            let (code, body) = send_as(&ctx, &campaign_id, &creator_id, platform, url).await;
            assert_eq!(code, StatusCode::CONFLICT, "{platform} {url}: {body}");
        }
        // A youtube-labelled respelling parses to the real id, not a raw escape.
        assert_eq!(
            parse_post_id("youtube", &Url::parse("https://youtu.be/%61bc123").unwrap()).as_deref(),
            Some("abc123")
        );

        let (code, generic) = send_as(
            &ctx,
            &campaign_id,
            &creator_id,
            "other",
            "https://blog.example/post/abc",
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{generic}");
        for url in [
            "https://blog.example/post/%61bc",
            "https://blog.example//post/abc",
            "https://blog.example/post//abc/",
            "https://blog.example/%70ost/abc#top",
        ] {
            let (code, body) = send_as(&ctx, &campaign_id, &creator_id, "blog", url).await;
            assert_eq!(code, StatusCode::CONFLICT, "{url}: {body}");
        }

        // PUT cannot respell a third row into either post.
        let (code, third) = send_as(
            &ctx,
            &campaign_id,
            &creator_id,
            "other",
            "https://blog.example/post/xyz",
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{third}");
        let third_id = third["submission"]["id"].as_str().unwrap().to_string();
        for url in [
            "https://youtube.com/v/%61bc123",
            "https://blog.example//post/%61bc",
        ] {
            let (code, body) = update_submission(
                State(ctx.clone()),
                Path(third_id.clone()),
                Json(SubmissionEditBody {
                    platform: None,
                    post_url: Some(url.into()),
                }),
            )
            .await;
            assert_eq!(code, StatusCode::CONFLICT, "{url}: {body:?}");
        }

        let payouts = approve_all_and_list_payouts(&ctx, &campaign_id).await;
        assert_eq!(payouts.len(), 3, "three distinct posts: {payouts:?}");
    }

    #[tokio::test]
    async fn a_creator_with_submissions_needs_force_to_delete() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        submit(&ctx, &campaign_id, &creator_id, "https://youtu.be/abc123").await;

        let (code, _) = delete_creator(
            State(ctx.clone()),
            Path(creator_id.clone()),
            Query(ForceQuery::default()),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT);

        let (code, _) = delete_creator(
            State(ctx.clone()),
            Path(creator_id),
            Query(ForceQuery {
                force: Some("true".into()),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
    }

    /// Submit, price, approve and PAY one post; returns (submission id, payout id).
    async fn paid_submission(
        ctx: &UgcCtx,
        campaign: &str,
        creator: &str,
        url: &str,
    ) -> (String, String) {
        let (code, body) = submit(ctx, campaign, creator, url).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let submission_id = body["submission"]["id"].as_str().unwrap().to_string();
        let (code, _) = record_metrics(
            State(ctx.clone()),
            Path(submission_id.clone()),
            Json(MetricsBody {
                views: 41_200,
                ..MetricsBody::default()
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let (_, Json(reviewed)) = review_submission(
            State(ctx.clone()),
            Path(submission_id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        let payout_id = reviewed["payout"]["id"].as_str().unwrap().to_string();
        let (code, _) = approve_payout(State(ctx.clone()), Path(payout_id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        let (code, _) = mark_payout_paid(State(ctx.clone()), Path(payout_id.clone())).await;
        assert_eq!(code, StatusCode::OK);
        (submission_id, payout_id)
    }

    /// A `ugc.manage` route can never erase a paid settlement record: deleting
    /// the campaign, the creator WITH force=true, or the submission all 409 and
    /// leave every row (and the campaign's paid spend) exactly as it was.
    #[tokio::test]
    async fn parent_deletes_never_cascade_into_a_paid_payout() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let (submission_id, payout_id) = paid_submission(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://youtu.be/dQw4w9WgXcQ",
        )
        .await;
        // A second, unpaid submission under the same parents: a refused cascade
        // must not half-delete it either.
        let (_, unpaid) = submit(&ctx, &campaign_id, &creator_id, "https://youtu.be/abc123").await;
        let unpaid_id = unpaid["submission"]["id"].as_str().unwrap().to_string();

        let (code, Json(body)) =
            delete_campaign(State(ctx.clone()), Path(campaign_id.clone())).await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["paid_payouts"], 1, "{body}");

        let (code, Json(body)) = delete_creator(
            State(ctx.clone()),
            Path(creator_id.clone()),
            Query(ForceQuery {
                force: Some("true".into()),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["paid_payouts"], 1, "{body}");

        let (code, _) = delete_submission(State(ctx.clone()), Path(submission_id.clone())).await;
        assert_eq!(code, StatusCode::CONFLICT);

        let (code, Json(payout)) = get_payout(State(ctx.clone()), Path(payout_id)).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(payout["payout"]["status"], "paid", "{payout}");
        assert_eq!(payout["payout"]["amount_cents"], 10_300, "{payout}");
        for (code, _) in [
            get_campaign(State(ctx.clone()), Path(campaign_id.clone())).await,
            get_creator(State(ctx.clone()), Path(creator_id.clone())).await,
            get_submission(State(ctx.clone()), Path(submission_id)).await,
            get_submission(State(ctx.clone()), Path(unpaid_id.clone())).await,
        ] {
            assert_eq!(code, StatusCode::OK);
        }
        let (_, Json(summary)) = campaign_summary(State(ctx.clone()), Path(campaign_id)).await;
        assert_eq!(summary["paid_cents"], 10_300, "{summary}");

        // An unpaid submission is still deletable on its own.
        let (code, _) = delete_submission(State(ctx), Path(unpaid_id)).await;
        assert_eq!(code, StatusCode::OK);
    }

    /// Without paid rows the cascades still work, unpaid payouts included.
    #[tokio::test]
    async fn a_campaign_without_paid_payouts_still_deletes_with_its_children() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let (_, body) = submit(&ctx, &campaign_id, &creator_id, "https://youtu.be/abc123").await;
        let submission_id = body["submission"]["id"].as_str().unwrap().to_string();
        let (_, Json(reviewed)) = review_submission(
            State(ctx.clone()),
            Path(submission_id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        let payout_id = reviewed["payout"]["id"].as_str().unwrap().to_string();
        let (code, _) = approve_payout(State(ctx.clone()), Path(payout_id.clone())).await;
        assert_eq!(code, StatusCode::OK);

        let (code, _) = delete_campaign(State(ctx.clone()), Path(campaign_id)).await;
        assert_eq!(code, StatusCode::OK);
        let (code, _) = get_submission(State(ctx.clone()), Path(submission_id)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, _) = get_payout(State(ctx), Path(payout_id)).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn missing_ids_are_404s() {
        let ctx = ctx();
        for (code, _) in [
            get_campaign(State(ctx.clone()), Path("nope".into())).await,
            get_creator(State(ctx.clone()), Path("nope".into())).await,
            get_submission(State(ctx.clone()), Path("nope".into())).await,
            get_payout(State(ctx.clone()), Path("nope".into())).await,
            campaign_summary(State(ctx.clone()), Path("nope".into())).await,
            refresh_campaign(State(ctx.clone()), Path("nope".into()), HeaderMap::new()).await,
        ] {
            assert_eq!(code, StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn a_submission_naming_an_unknown_campaign_is_a_400() {
        let ctx = ctx();
        let (_, creator_id) = seed(&ctx).await;
        let (code, body) = submit(&ctx, "cmp_nope", &creator_id, "https://youtu.be/abc").await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(
            body["error"].as_str().unwrap().contains("campaign_id"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn refreshing_a_platform_with_no_curated_source_is_a_400() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let (_, body) = submit(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://youtu.be/dQw4w9WgXcQ",
        )
        .await;
        let id = body["submission"]["id"].as_str().unwrap().to_string();
        // Retarget it at a platform the curated map does not cover.
        let (code, _) = update_submission(
            State(ctx.clone()),
            Path(id.clone()),
            Json(SubmissionEditBody {
                platform: Some("myspace".into()),
                post_url: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let (code, Json(body)) =
            refresh_submission(State(ctx), Path(id.clone()), HeaderMap::new()).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(
            body["error"].as_str().unwrap().contains("Composio"),
            "{body}"
        );
        // The failure ALSO carries the report line the campaign route serves, so
        // the panel parses both routes with one function. `error` and `message`
        // are the same string on purpose — one fact, spelled for two readers.
        assert_eq!(body["status"], "error", "{body}");
        assert_eq!(body["submission_id"], id, "{body}");
        assert_eq!(body["message"], body["error"], "{body}");
        assert!(body["snapshot"].is_null(), "{body}");
    }

    #[tokio::test]
    async fn a_bad_payout_rule_or_bonus_ladder_is_a_400() {
        let ctx = ctx();
        let mut body = campaign_body("Acme", PayoutRule::Cpm { cpm_cents: -1 });
        let (code, _) = create_campaign(State(ctx.clone()), Json(body)).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);

        body = campaign_body("Acme", PayoutRule::Flat { flat_cents: 100 });
        body.bonus_tiers = vec![
            BonusTier {
                views: 25_000,
                bonus_cents: 2_000,
            },
            BonusTier {
                views: 10_000,
                bonus_cents: 500,
            },
        ];
        let (code, Json(out)) = create_campaign(State(ctx), Json(body)).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(
            out["error"].as_str().unwrap().contains("increase strictly"),
            "{out}"
        );
    }

    // ---- the SSRF screen ---------------------------------------------------

    #[test]
    fn post_url_screen_rejects_internal_and_non_https_targets() {
        for bad in [
            "http://www.youtube.com/watch?v=abc", // https only
            "ftp://example.com/x",
            "https://169.254.169.254/latest/meta-data/", // cloud metadata
            "https://127.0.0.1:7981/api/agents",
            "https://localhost/x",
            "https://10.0.0.5/x",
            "https://192.168.1.9/x",
            "https://[::1]/x",
            "https://[fd00::1]/x",
            "https://metadata.google.internal/x",
            "https://build-box.internal/x",
            "https://printer.local/x",
            "not a url",
            "   ",
        ] {
            assert!(
                screen_post_url(bad).is_err(),
                "'{bad}' must be rejected by the post-url screen"
            );
        }
    }

    #[test]
    fn post_url_screen_accepts_a_real_post() {
        for good in [
            "https://youtu.be/dQw4w9WgXcQ",
            "https://www.tiktok.com/@creator/video/7301234567890123456",
            "https://www.instagram.com/reel/CxYz-1AbCdE/",
            "https://x.com/creator/status/1750000000000000000",
        ] {
            assert!(screen_post_url(good).is_ok(), "'{good}' must be accepted");
        }
    }

    #[tokio::test]
    async fn a_submission_with_an_internal_post_url_is_refused() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let (code, body) = submit(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://169.254.169.254/latest/meta-data/",
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("private or loopback"),
            "{body}"
        );
    }

    // ---- post-id parsing + the id guard ------------------------------------

    #[test]
    fn post_ids_are_parsed_per_platform() {
        let cases = [
            ("youtube", "https://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
            (
                "youtube",
                "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=30",
                "dQw4w9WgXcQ",
            ),
            (
                "youtube",
                "https://www.youtube.com/shorts/AbC-123_x",
                "AbC-123_x",
            ),
            (
                "tiktok",
                "https://www.tiktok.com/@creator/video/7301234567890123456",
                "7301234567890123456",
            ),
            (
                "instagram",
                "https://www.instagram.com/reel/CxYz-1AbCdE/",
                "CxYz-1AbCdE",
            ),
            (
                "x",
                "https://x.com/creator/status/1750000000000000000",
                "1750000000000000000",
            ),
            (
                "linkedin",
                "https://www.linkedin.com/feed/update/urn:li:activity:7301234567890123456/",
                "7301234567890123456",
            ),
        ];
        for (platform, url, expected) in cases {
            let parsed = parse_post_id(platform, &Url::parse(url).unwrap());
            assert_eq!(parsed.as_deref(), Some(expected), "{platform} {url}");
        }
    }

    #[test]
    fn an_unparseable_url_yields_no_post_id_rather_than_a_rejection() {
        // A real post URL whose shape this app does not know: the submission is
        // still recordable (empty id is excluded from the unique index), it just
        // cannot auto-refresh.
        let url = Url::parse("https://www.youtube.com/feed/subscriptions").unwrap();
        assert!(parse_post_id("youtube", &url).is_none());
        // And an unknown platform never invents one.
        assert!(parse_post_id("myspace", &Url::parse("https://a.test/b").unwrap()).is_none());
    }

    /// The parser only yields a platform's post id for a URL on that platform's
    /// own hosts, so the metrics identity can never name a post the dedup key
    /// (derived from the host) does not.
    #[test]
    fn a_post_id_is_only_parsed_on_the_platforms_own_host() {
        for (platform, url) in [
            ("youtube", "https://evil.example/watch?v=abc123"),
            ("youtube", "https://youtu.be.evil.example/abc123"),
            ("youtube", "https://notyoutu.be/abc123"),
            ("youtube", "https://notyoutube.com/shorts/abc123"),
            ("tiktok", "https://www.youtube.com/@a/video/1"),
            ("x", "https://evil.example/a/status/99"),
            ("linkedin", "https://evil.example/posts/abc"),
        ] {
            assert!(
                parse_post_id(platform, &Url::parse(url).unwrap()).is_none(),
                "{platform} {url}"
            );
        }
        for (platform, url, id) in [
            ("youtube", "https://m.youtu.be/abc123", "abc123"),
            (
                "youtube",
                "https://www.youtube-nocookie.com/embed/abc123",
                "abc123",
            ),
            ("youtube", "https://youtube.com/v/abc123", "abc123"),
            ("youtube", "https://youtube.com/watch?V=abc123", "abc123"),
            ("x", "https://mobile.twitter.com/a/status/99", "99"),
        ] {
            assert_eq!(
                parse_post_id(platform, &Url::parse(url).unwrap()).as_deref(),
                Some(id),
                "{platform} {url}"
            );
        }
        let foreign = Url::parse("https://evil.example/watch?v=abc123").unwrap();
        assert!(check_label_matches_host("youtube", &foreign).is_err());
        assert!(check_label_matches_host("other", &foreign).is_ok());
        assert!(
            check_label_matches_host("linkedin", &Url::parse("https://lnkd.in/abc").unwrap())
                .is_ok()
        );
    }

    #[test]
    fn an_extracted_id_that_is_not_a_safe_segment_is_discarded() {
        // `id_segment_is_safe` is the last gate before the id can reach a
        // Composio action's arguments. A percent-decoded traversal in the id
        // position must not survive it.
        let url = Url::parse("https://x.com/c/status/..%2F..%2Fapi%2Fagents").unwrap();
        assert!(
            parse_post_id("x", &url).is_none(),
            "a traversal-shaped id must be discarded, not stored"
        );
    }

    // ---- small helpers -----------------------------------------------------

    #[test]
    fn limit_is_lenient_and_platforms_are_lowercased() {
        assert_eq!(parse_limit(Some("25")), Some(25));
        assert_eq!(parse_limit(Some("abc")), None);
        assert_eq!(parse_limit(Some("")), None);
        assert_eq!(parse_limit(None), None);
        assert_eq!(normalize_platform("  TikTok "), "tiktok");
        assert!(is_truthy("TRUE") && is_truthy("1") && !is_truthy("0"));
    }

    #[tokio::test]
    async fn platforms_serves_the_curated_map_verbatim() {
        let (code, Json(body)) = list_platforms(State(ctx())).await;
        assert_eq!(code, StatusCode::OK);
        let platforms = body["platforms"].as_array().unwrap();
        assert_eq!(platforms.len(), composio::PLATFORM_METRIC_SOURCES.len());
        assert_eq!(platforms[0]["platform"], "youtube");
        // The action id and its selectors are visible, which is what makes an
        // unverified row correctable instead of mysterious.
        assert!(platforms[0]["action"]
            .as_str()
            .unwrap()
            .starts_with("YOUTUBE_"));
        assert!(!platforms[0]["views"].as_str().unwrap().is_empty());
        // The flag comes from the managed Gateway provider status, not a key held
        // by this sidecar.
        assert!(body["composio_configured"].is_boolean(), "{body}");
    }

    // ---- refresh: the three-way per-submission outcome ----------------------

    /// Seed an approved submission carrying one hand-entered reading, and return
    /// its id with what its payout is currently worth — the "before" the tests
    /// below assert nothing moved from.
    async fn approved_with_metrics(ctx: &UgcCtx) -> (String, i64) {
        let (campaign_id, creator_id) = seed(ctx).await;
        let (_, body) = submit(
            ctx,
            &campaign_id,
            &creator_id,
            "https://youtu.be/dQw4w9WgXcQ",
        )
        .await;
        let id = body["submission"]["id"].as_str().unwrap().to_string();
        let _ = review_submission(
            State(ctx.clone()),
            Path(id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        let _ = record_metrics(
            State(ctx.clone()),
            Path(id.clone()),
            Json(MetricsBody {
                views: 41_200,
                ..MetricsBody::default()
            }),
        )
        .await;
        // 41 200 views at 250c/1k floors to 10 300c.
        let accrued = ctx
            .engine
            .store
            .payout_for_submission(&id)
            .await
            .unwrap()
            .unwrap()
            .amount_cents;
        (id, accrued)
    }

    /// THE money-critical property of the refresh surface: an account the operator
    /// has not linked yet writes NO snapshot and re-prices NO payout, and the body
    /// says so with a link to fix it — never as an error, never as a reading.
    ///
    /// Driven at the engine's write seam rather than through
    /// `POST /submissions/:id/refresh`, because the fetch uses the managed Composio
    /// route: there is no host credential to fake a not-connected answer through,
    /// and a real dispatch is precisely what a test must not do.
    #[tokio::test]
    async fn a_needs_connection_refresh_writes_nothing_and_offers_the_connect_link() {
        let ctx = ctx();
        let (id, before) = approved_with_metrics(&ctx).await;
        let submission = ctx.engine.store.get_submission(&id).await.unwrap().unwrap();

        let outcome = ctx
            .engine
            .apply_metric_outcome(
                &submission,
                None,
                MetricOutcome::NeedsConnection {
                    message: "No active connection for YouTube".to_string(),
                    connect_url: Some("https://composio.dev/connect/abc".to_string()),
                },
            )
            .await
            .expect("not connected is an outcome, never an error");

        // Five zeroes here would drop a live payout to nothing on the next accrual
        // pass, with the panel reporting a successful refresh.
        let (_, Json(history)) = list_metrics(
            State(ctx.clone()),
            Path(id.clone()),
            Query(LimitQuery::default()),
        )
        .await;
        assert_eq!(
            history["snapshots"].as_array().unwrap().len(),
            1,
            "only the hand-entered reading survives: {history}"
        );
        let (_, Json(one)) = get_submission(State(ctx.clone()), Path(id.clone())).await;
        assert_eq!(one["payout"]["amount_cents"], before, "{one}");

        let wire = refresh_outcome_body(&id, outcome);
        assert_eq!(wire["submission_id"], id, "{wire}");
        assert_eq!(wire["status"], "needs_connection", "{wire}");
        assert_eq!(wire["connect_url"], "https://composio.dev/connect/abc");
        assert!(
            wire["message"]
                .as_str()
                .unwrap()
                .contains("No active connection"),
            "{wire}"
        );
        // Present-and-null rather than absent, so the panel switches on `status`
        // without probing for keys.
        for key in ["snapshot", "previous", "payout"] {
            assert!(
                wire.get(key).is_some_and(Value::is_null),
                "'{key}' must be present and null: {wire}"
            );
        }
    }

    /// …and the same body DOES carry the reading and the re-priced payout on a
    /// real sample, so the guard above is a branch and not a broken path.
    #[tokio::test]
    async fn a_sample_refresh_body_carries_the_snapshot_previous_and_payout() {
        let ctx = ctx();
        let (id, _) = approved_with_metrics(&ctx).await;
        let submission = ctx.engine.store.get_submission(&id).await.unwrap().unwrap();
        let previous = ctx.engine.store.latest_snapshot(&id).await.unwrap();

        let outcome = ctx
            .engine
            .apply_metric_outcome(
                &submission,
                previous,
                MetricOutcome::Sample(MetricSample {
                    views: 82_400,
                    ..MetricSample::default()
                }),
            )
            .await
            .unwrap();

        let wire = refresh_outcome_body(&id, outcome);
        assert_eq!(wire["status"], "ok", "{wire}");
        assert_eq!(wire["snapshot"]["views"], 82_400);
        assert_eq!(wire["snapshot"]["source"], MetricSource::Composio.as_str());
        // `previous` is what the panel diffs the new reading against.
        assert_eq!(wire["previous"]["views"], 41_200, "{wire}");
        // 82 400 views at 250c/1k = 20 600c, re-priced in place.
        assert_eq!(wire["payout"]["amount_cents"], 20_600, "{wire}");
        assert!(
            wire["message"].is_null() && wire["connect_url"].is_null(),
            "{wire}"
        );
    }

    /// The campaign route answers 200 with one line per approved submission even
    /// when every one of them failed: a platform being down must never fail the
    /// batch or discard the snapshots that did land.
    ///
    /// Both rows are unrefreshable on purpose, which is also what keeps the test
    /// hermetic — a refreshable row would dispatch to Composio for real.
    #[tokio::test]
    async fn a_campaign_refresh_answers_a_line_per_submission_and_the_counts() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;

        let (_, body) = submit(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://youtu.be/dQw4w9WgXcQ",
        )
        .await;
        let uncurated = body["submission"]["id"].as_str().unwrap().to_string();
        let (code, _) = update_submission(
            State(ctx.clone()),
            Path(uncurated.clone()),
            Json(SubmissionEditBody {
                platform: Some("myspace".into()),
                post_url: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);

        // A real YouTube URL whose shape is not a post: recordable, reviewable, but
        // with no post id to look up.
        let (_, body) = submit(
            &ctx,
            &campaign_id,
            &creator_id,
            "https://www.youtube.com/feed/subscriptions",
        )
        .await;
        let unparsed = body["submission"]["id"].as_str().unwrap().to_string();
        assert_eq!(body["submission"]["external_post_id"], "", "{body}");

        for id in [&uncurated, &unparsed] {
            let _ = review_submission(
                State(ctx.clone()),
                Path(id.clone()),
                Json(ReviewBody {
                    decision: "approve".into(),
                    reason: None,
                }),
            )
            .await;
        }

        let (code, Json(out)) =
            refresh_campaign(State(ctx), Path(campaign_id), HeaderMap::new()).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            out["counts"],
            json!({ "ok": 0, "needs_connection": 0, "error": 2 }),
            "{out}"
        );
        let results = out["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r["status"] == "error"
            && r["snapshot"].is_null()
            && r["message"].is_string()));
        // Each line names its submission, so the panel can attach it to the row.
        let reported: Vec<&str> = results
            .iter()
            .map(|r| r["submission_id"].as_str().unwrap())
            .collect();
        assert!(reported.contains(&uncurated.as_str()) && reported.contains(&unparsed.as_str()));
    }

    /// A genuinely mixed batch — one real reading, one real not-connected outcome,
    /// one failure — split three ways on the wire. "Link this account" must never
    /// reach the panel as a failure, and it must never carry a snapshot.
    #[tokio::test]
    async fn a_mixed_batch_splits_ok_needs_connection_and_error() {
        let ctx = ctx();
        let (id, _) = approved_with_metrics(&ctx).await;
        let submission = ctx.engine.store.get_submission(&id).await.unwrap().unwrap();

        let refreshed = ctx
            .engine
            .apply_metric_outcome(
                &submission,
                None,
                MetricOutcome::Sample(MetricSample {
                    views: 82_400,
                    ..MetricSample::default()
                }),
            )
            .await
            .unwrap();
        let unlinked = ctx
            .engine
            .apply_metric_outcome(
                &submission,
                None,
                MetricOutcome::NeedsConnection {
                    message: "No active connection for TikTok".to_string(),
                    connect_url: None,
                },
            )
            .await
            .unwrap();

        // The ids are labels here; the outcomes are the real thing.
        let results = vec![
            SubmissionRefreshReport::from_outcome("sub_read", refreshed),
            SubmissionRefreshReport::from_outcome("sub_unlinked", unlinked),
            SubmissionRefreshReport::from_error(
                "sub_down",
                &RefreshError::Upstream("tiktok answered 503".to_string()),
            ),
        ];
        // Counted before the move: `CampaignRefreshReport`'s fields initialise in
        // written order, so `results` cannot be borrowed after it lands in one.
        let counts = RefreshCounts::of(&results);
        let wire = serde_json::to_value(CampaignRefreshReport { results, counts }).unwrap();

        assert_eq!(
            wire["counts"],
            json!({ "ok": 1, "needs_connection": 1, "error": 1 }),
            "{wire}"
        );
        assert_eq!(wire["results"][0]["status"], "ok");
        assert_eq!(wire["results"][0]["snapshot"]["views"], 82_400);
        assert_eq!(wire["results"][1]["status"], "needs_connection");
        assert!(
            wire["results"][1]["snapshot"].is_null(),
            "an unlinked account must not leave a reading behind: {wire}"
        );
        assert_eq!(wire["results"][2]["status"], "error");
        assert_eq!(wire["results"][2]["message"], "tiktok answered 503");
    }

    #[tokio::test]
    async fn manual_metrics_are_recorded_as_manual_and_reprice_in_place() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let (_, body) = submit(&ctx, &campaign_id, &creator_id, "https://youtu.be/abc123").await;
        let id = body["submission"]["id"].as_str().unwrap().to_string();
        let _ = review_submission(
            State(ctx.clone()),
            Path(id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;

        for views in [10_000i64, 41_200] {
            let _ = record_metrics(
                State(ctx.clone()),
                Path(id.clone()),
                Json(MetricsBody {
                    views,
                    ..MetricsBody::default()
                }),
            )
            .await;
        }
        let (_, Json(history)) = list_metrics(
            State(ctx.clone()),
            Path(id.clone()),
            Query(LimitQuery::default()),
        )
        .await;
        let snapshots: Vec<MetricSnapshot> =
            serde_json::from_value(history["snapshots"].clone()).unwrap();
        assert_eq!(snapshots.len(), 2, "history is append-only");
        assert_eq!(snapshots[0].source, MetricSource::Manual);

        let (_, Json(listed)) = list_payouts(State(ctx), Query(PayoutQuery::default())).await;
        let payouts = listed["payouts"].as_array().unwrap();
        assert_eq!(
            payouts.len(),
            1,
            "one row per submission, re-priced in place"
        );
        assert_eq!(payouts[0]["amount_cents"], 10_300);
        assert_eq!(payouts[0]["status"], PayoutStatus::Accrued.as_str());
    }

    #[tokio::test]
    async fn rejecting_records_the_reason_and_drops_the_accrual() {
        let ctx = ctx();
        let (campaign_id, creator_id) = seed(&ctx).await;
        let (_, body) = submit(&ctx, &campaign_id, &creator_id, "https://youtu.be/abc123").await;
        let id = body["submission"]["id"].as_str().unwrap().to_string();
        let _ = review_submission(
            State(ctx.clone()),
            Path(id.clone()),
            Json(ReviewBody {
                decision: "approve".into(),
                reason: None,
            }),
        )
        .await;
        let (code, Json(out)) = review_submission(
            State(ctx.clone()),
            Path(id.clone()),
            Json(ReviewBody {
                decision: "reject".into(),
                reason: Some("missing the required hashtag".into()),
            }),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            out["submission"]["rejection_reason"],
            "missing the required hashtag"
        );
        assert_eq!(out["payout"], Value::Null);

        let (code, _) = review_submission(
            State(ctx),
            Path(id),
            Json(ReviewBody {
                decision: "nonsense".into(),
                reason: None,
            }),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }
    // ── OpenAPI document ───────────────────────────────────────────────────────

    /// This app's own manifest, read at compile time. The route contract lives there,
    /// so the invariants below compare the document against the real declaration
    /// rather than against a second list that could drift from it.
    fn openapi_manifest() -> serde_json::Value {
        serde_json::from_str(include_str!("../../manifest.json")).expect("valid JSON")
    }

    /// The manifest sidecar whose HTTP surface this router serves: the one that
    /// declares an `http.mount`. Selected BY mount rather than by index because an app
    /// may declare a second, mountless sidecar (finetune already does), and
    /// `sidecars[0]` would then quietly start asserting against the wrong process.
    fn mounted_sidecar() -> serde_json::Value {
        openapi_manifest()["sidecars"]
            .as_array()
            .expect("sidecars must be an array")
            .iter()
            .find(|s| s["http"]["mount"].is_string())
            .expect("one sidecar must declare an http.mount")
            .clone()
    }

    /// A manifest route (relative to the mount, in axum's `:param` form) rewritten
    /// into the form the OpenAPI document uses (absolute, in `{param}` form).
    ///
    /// The two forms differ ON PURPOSE — the router registers paths relative to the
    /// mount because Core nests it there, while the `#[utoipa::path]` annotations carry
    /// the absolute EXTERNAL path a caller actually hits. Normalise here; do not
    /// "align" either side.
    fn doc_path_for(mount: &str, route: &str) -> String {
        let joined = if route == "/" {
            mount.to_owned()
        } else {
            format!("{mount}{route}")
        };
        joined
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn openapi_doc_is_served_and_non_empty() {
        // The doc is no longer dead code: Core fetches it to derive tools.
        assert!(!super::openapi().paths.paths.is_empty());
    }

    #[test]
    fn every_declared_route_appears_in_the_openapi_doc() {
        // The direction that decides tool yield. Core's `ext_api::lower` keeps only the
        // document operations the manifest ALSO declares, so a declared route with no
        // `#[utoipa::path]` annotation is a tool that silently never exists — nothing
        // errors, the agent simply cannot call it. (The other direction is harmless: an
        // annotated path the manifest does not declare is dropped by the same filter.)
        let sidecar = mounted_sidecar();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        let doc = super::openapi();
        for route in sidecar["http"]["routes"]
            .as_array()
            .expect("routes must be an array")
        {
            let path = route["path"].as_str().expect("a route path");
            if path == "/health" {
                // Declared so the ext-proxy forwards it, but it is a liveness probe,
                // not an API operation — annotating it would offer the model a tool
                // that answers nothing. Exempt by name rather than by weakening the
                // invariant for every other route.
                continue;
            }
            let expected = doc_path_for(mount, path);
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // OpenAPI → LLM tool derivation
    //
    // Core derives an LLM tool per route from this document (fetched over
    // loopback at `/openapi.json`), and the tool's ARGUMENTS come from the
    // operation's `requestBody` schema. Every annotation here used to say
    // `request_body = serde_json::Value`, which serialises to `{}` — so every
    // write route reached the model as a tool it could see and could not call.
    // These tests are the guard: they fail if a body type stops being described.
    // ─────────────────────────────────────────────────────────────────────────

    /// Escape a route for a JSON pointer segment (`/` is `~1`).
    fn body_schema(doc: &Value, route: &str, method: &str) -> Value {
        let pointer = format!(
            "/paths/{}/{method}/requestBody/content/application~1json/schema",
            route.replace('~', "~0").replace('/', "~1")
        );
        doc.pointer(&pointer)
            .unwrap_or_else(|| panic!("no request body documented at {route} {method}"))
            .clone()
    }

    #[test]
    fn post_routes_document_their_request_body() {
        let doc = serde_json::to_value(openapi()).unwrap();
        for (route, method) in [
            ("/api/ugc/campaigns", "post"),
            ("/api/ugc/creators", "post"),
            ("/api/ugc/submissions", "post"),
            ("/api/ugc/submissions/{id}/review", "post"),
            ("/api/ugc/submissions/{id}/metrics", "post"),
            ("/api/ugc/campaigns/{id}", "put"),
            ("/api/ugc/creators/{id}", "put"),
            ("/api/ugc/submissions/{id}", "put"),
        ] {
            let schema = body_schema(&doc, route, method);
            // A `$ref` is correct and expected — Core resolves it against
            // `components.schemas` on import.
            assert!(
                schema.get("$ref").is_some() || schema.get("properties").is_some(),
                "a derived write tool for {route} {method} would have no arguments: {schema}"
            );
        }
    }

    /// The assertion above is necessary but not sufficient: a `$ref` to a type
    /// that was never registered looks identical in the operation and still
    /// yields zero arguments once Core tries to resolve it.
    #[test]
    fn every_request_body_ref_resolves_against_components() {
        let doc = serde_json::to_value(openapi()).unwrap();
        let schemas = &doc["components"]["schemas"];
        for (route, methods) in doc["paths"].as_object().expect("paths") {
            for (method, op) in methods.as_object().expect("operations") {
                let Some(schema) = op.pointer("/requestBody/content/application~1json/schema")
                else {
                    continue;
                };
                let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
                    assert!(
                        schema.get("properties").is_some(),
                        "{route} {method} documents a body with neither a $ref nor \
                         properties — the model sees no arguments: {schema}"
                    );
                    continue;
                };
                let name = reference
                    .strip_prefix("#/components/schemas/")
                    .unwrap_or_else(|| panic!("{route} {method} points outside this document"));
                let target = &schemas[name];
                assert!(
                    target.get("properties").is_some(),
                    "{route} {method} refs '{name}', which is missing from \
                     components(schemas(...)) or carries no properties"
                );
            }
        }
    }

    /// A nested body type must describe itself in place. `payout` is a tagged
    /// enum one level down; left as a `$ref` the model would see an opaque
    /// pointer instead of the two rules it has to choose between.
    #[test]
    fn a_nested_struct_argument_is_self_describing() {
        let doc = serde_json::to_value(openapi()).unwrap();
        let payout = &doc["components"]["schemas"]["CampaignBody"]["properties"]["payout"];
        let rendered = serde_json::to_string(payout).unwrap();
        assert!(
            !rendered.contains("$ref"),
            "payout is still a pointer Core cannot follow: {rendered}"
        );
        for key in ["cpm_cents", "flat_cents"] {
            assert!(
                rendered.contains(key),
                "the model cannot see the '{key}' variant field: {rendered}"
            );
        }
        let tiers = &doc["components"]["schemas"]["CampaignBody"]["properties"]["bonus_tiers"];
        assert!(
            tiers["items"]["properties"]["bonus_cents"].is_object(),
            "bonus_tiers items are not self-describing: {tiers}"
        );
    }

    /// The payoff of typing the bodies: a `///` on a field becomes the argument
    /// description the model actually reads.
    #[test]
    fn body_field_docs_reach_the_schema_as_argument_descriptions() {
        let doc = serde_json::to_value(openapi()).unwrap();
        let budget = &doc["components"]["schemas"]["CampaignBody"]["properties"]["budget_cents"];
        assert_eq!(budget["description"], "0 = uncapped.");
        let handles = &doc["components"]["schemas"]["CreatorBody"]["properties"]["handles"];
        assert!(
            handles["description"]
                .as_str()
                .is_some_and(|d| d.contains("Platform key")),
            "CreatorBody.handles lost its description: {handles}"
        );
    }

    /// A handler with no `Json` extractor must declare no body at all — a
    /// `request_body` there is a lie that makes the model send one. Its `id`
    /// argument still has to survive, and that comes from `params(...)`.
    #[test]
    fn body_less_routes_declare_no_request_body() {
        let doc = serde_json::to_value(openapi()).unwrap();
        for route in [
            "/api/ugc/payouts/{id}/approve",
            "/api/ugc/payouts/{id}/paid",
            "/api/ugc/campaigns/{id}/refresh",
            "/api/ugc/submissions/{id}/refresh",
        ] {
            let op = &doc["paths"][route]["post"];
            assert!(
                op.is_object(),
                "{route} is missing from the document entirely"
            );
            assert!(
                op.get("requestBody").is_none(),
                "{route} documents a body its handler never reads"
            );
            assert!(
                op["parameters"][0]["name"] == "id",
                "{route} lost its id argument: {op}"
            );
        }
    }
}
