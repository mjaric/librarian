//! Display helpers: number formatting, reading-ease labels and the
//! deterministic "binder's cloth" palette that colors spines and covers.

/// `183505` → `183,505`; None → an em dash.
pub fn thousands(n: Option<i64>) -> String {
    match n {
        None => "—".into(),
        Some(n) => {
            let s = n.to_string();
            let mut out = String::with_capacity(s.len() + s.len() / 3);
            let digits = s.len();
            for (i, c) in s.chars().enumerate() {
                out.push(c);
                let left = digits - i - 1;
                if left > 0 && left % 3 == 0 {
                    out.push(',');
                }
            }
            out
        }
    }
}

/// `1_234_567` → `1.2 MB`; None → `—`.
pub fn human_bytes(n: Option<i64>) -> String {
    let Some(n) = n else { return "—".into() };
    let mb = n as f64 / 1_048_576.0;
    if mb >= 1.0 {
        format!("{mb:.1} MB")
    } else {
        format!("{} kB", n / 1024)
    }
}

/// Author list for a card line.
pub fn authors_line(authors: &[String]) -> String {
    if authors.is_empty() {
        "Unknown author".into()
    } else {
        authors.join(", ")
    }
}

/// Flesch score → a shelf-talk label (bands per Flesch's original scale).
pub fn ease_label(score: f32) -> &'static str {
    if score < 30.0 {
        "dense"
    } else if score < 50.0 {
        "demanding"
    } else if score < 60.0 {
        "fairly hard"
    } else if score < 70.0 {
        "plain prose"
    } else if score < 80.0 {
        "fairly easy"
    } else if score < 90.0 {
        "easy"
    } else {
        "very easy"
    }
}

/// Curated binder's-cloth hues. Names hash onto them deterministically so a
/// category or book keeps its color across sessions.
const CLOTH_HUES: [i16; 12] = [
    14,  // oxblood
    96,  // moss
    168, // verdigris
    38,  // ochre
    214, // slate
    300, // aubergine
    8,   // rust
    186, // teal
    26,  // walnut
    240, // ink blue
    60,  // olive
    330, // plum
];

fn name_hue(name: &str) -> i16 {
    let mut h: u32 = 5381;
    for b in name.bytes() {
        h = h.wrapping_mul(33) ^ u32::from(b);
    }
    CLOTH_HUES[(h as usize) % CLOTH_HUES.len()]
}

/// CSS custom props for a group-colored spine/cover.
pub fn cloth_style(name: &str) -> String {
    let hue = name_hue(name);
    format!(
        "--hue:{hue}; --cloth:hsl({hue} 30% 34%); --cloth-edge:hsl({hue} 32% 26%); \
         --cloth-ink:hsl({hue} 20% 92%)"
    )
}

/// Deterministic spine geometry for the home shelf, in px. Width follows the
/// plain-text size (page-count proxy, sqrt-scaled, capped); height varies
/// per book — real shelves are never level.
pub fn spine_geometry(id: i64, txt_bytes: Option<i64>) -> (u32, u32) {
    let w = match txt_bytes {
        None => 30,
        Some(bytes) => {
            (28.0 + 66.0 * ((bytes as f64) / 6_000_000.0).sqrt().min(1.0)).round() as u32
        }
    };
    let mut h = 5381u64;
    for b in id.to_le_bytes() {
        h = h.wrapping_mul(33) ^ u64::from(b);
    }
    let h = 152 + (h % 76) as u32;
    (w, h)
}
