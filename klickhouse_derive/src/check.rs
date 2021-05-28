use crate::ast::{Container};
use crate::{ctxt::Ctxt};

/// Cross-cutting checks that require looking at more than a single attrs
/// object. Simpler checks should happen when parsing and building the attrs.
pub fn check(cx: &Ctxt, cont: &mut Container) {
    check_from_and_try_from(cx, cont);
}

fn check_from_and_try_from(cx: &Ctxt, cont: &mut Container) {
    if cont.attrs.type_from().is_some() && cont.attrs.type_try_from().is_some() {
        cx.error_spanned_by(
            cont.original,
            "#[serde(from = \"...\")] and #[serde(try_from = \"...\")] conflict with each other",
        );
    }
}
