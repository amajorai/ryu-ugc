//! Inbox, toast and push copy as localizable messages.
//!
//! Core renders a notify hint's `i18n` text per recipient (the inbox row and
//! toast in the node's `ui-language`, each push in the locale its device
//! registered with); the English rendering still goes out as the plain
//! `title`/`body`, which an older Core delivers as is.
//!
//! Each `*_MESSAGE` is `format!` with one named argument per placeholder
//! (`name = arg("name")`) so the i18n extractor catalogs the ICU source;
//! `tools/generate-core-i18n.mjs` copies the translations into Core's catalog.
//! Every message is a whole sentence. Core's renderer has no plural selector,
//! so counts are written as `label: {count}`.
#![allow(clippy::useless_format)]

use ryu_app_events::{NotifyHint, NotifyMessage, NotifyTemplate as Template};

pub(crate) const NEW_SUBMISSION_MESSAGE: Template = |_| format!("New submission to review");
pub(crate) const SUBMITTED_ON_MESSAGE: Template = |arg| {
    format!(
        "Submitted by {creator} on {platform}",
        creator = arg("creator"),
        platform = arg("platform")
    )
};

/// A creator submitted a post for review.
pub(crate) fn submission_received(creator_id: &str, platform: &str) -> NotifyHint {
    NotifyHint::localized(
        NotifyMessage::new(NEW_SUBMISSION_MESSAGE),
        vec![NotifyMessage::new(SUBMITTED_ON_MESSAGE)
            .text("creator", creator_id)
            .text("platform", platform)],
    )
}
