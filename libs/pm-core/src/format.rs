//! Pretty console formatting for human-facing log events.
//!
//! Keeps presentation concerns (money formatting, boxed banners) out of the task
//! files so they stay focused on logic.
//!
//! NOTE: we deliberately do *not* emit ANSI color escapes. `tracing_subscriber`'s
//! `fmt` layer escapes the ESC byte inside message values (rendering it as a
//! literal `\x1b`), so embedded color renders as garbage. Newlines, box-drawing
//! characters, and emoji pass through fine — so win/loss "color" is carried by the
//! 🟢 / 🔴 emoji, which terminals render in green/red from the font.

use rust_decimal::Decimal;
use unicode_width::UnicodeWidthStr;

/// `$4.50` — two-decimal money with a leading `$`.
pub fn usd(v: Decimal) -> String {
    format!("${:.2}", v.round_dp(2))
}

/// `+$5.50` / `-$3.20` — signed PnL for wins and losses.
pub fn signed_usd(v: Decimal) -> String {
    let v = v.round_dp(2);
    let sign = if v.is_sign_negative() { "-" } else { "+" };
    format!("{sign}${:.2}", v.abs())
}

/// Draw a Unicode box around `title` and `rows`.
///
/// Widths are measured with `unicode-width` so wide chars (emoji) keep the box
/// aligned. Returns a multi-line string; callers should emit it after a leading
/// `\n` so it lands below the tracing prefix and stays left-aligned at column 0.
pub fn banner(title: &str, rows: &[(&str, String)]) -> String {
    let label_w = rows.iter().map(|(l, _)| l.width()).max().unwrap_or(0);
    // Pre-render each row body and remember its display width.
    let bodies: Vec<(String, usize)> = rows
        .iter()
        .map(|(l, v)| {
            let pad = " ".repeat(label_w.saturating_sub(l.width()));
            let body = format!("{l}{pad}  {v}");
            let w = body.width();
            (body, w)
        })
        .collect();
    let inner = bodies
        .iter()
        .map(|(_, w)| *w)
        .chain(std::iter::once(title.width()))
        .max()
        .unwrap_or(0);

    let dash = "─".repeat(inner + 2);
    let mut out = String::new();
    out.push_str(&format!("┌{dash}┐\n"));
    out.push_str(&format!(
        "│ {title}{} │\n",
        " ".repeat(inner - title.width())
    ));
    out.push_str(&format!("├{dash}┤"));
    for (body, w) in &bodies {
        out.push_str(&format!("\n│ {body}{} │", " ".repeat(inner - w)));
    }
    out.push_str(&format!("\n└{dash}┘"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn usd_rounds_to_two_places() {
        assert_eq!(usd(dec!(4.5)), "$4.50");
        assert_eq!(usd(dec!(10)), "$10.00");
        assert_eq!(usd(dec!(3.146)), "$3.15");
    }

    #[test]
    fn signed_usd_carries_sign() {
        assert_eq!(signed_usd(dec!(5.5)), "+$5.50");
        assert_eq!(signed_usd(dec!(-3.2)), "-$3.20");
        assert_eq!(signed_usd(dec!(0)), "+$0.00");
    }

    #[test]
    fn banner_rows_are_display_aligned() {
        // Emoji title forces the width-aware path: the 🟢 is one char but two cells.
        let b = banner(
            "🟢 WON",
            &[
                ("outcome", "Up".to_string()),
                ("profit", "+$5.50".to_string()),
            ],
        );
        let widths: Vec<usize> = b.lines().map(|l| l.width()).collect();
        let w0 = widths[0];
        assert!(
            widths.iter().all(|w| *w == w0),
            "all box lines must share display width, got {widths:?}"
        );
        // top / title / separator + 2 rows + bottom
        assert_eq!(b.lines().count(), 6);
    }
}
