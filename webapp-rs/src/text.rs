//! Text validation helpers (§ conventions) and user-search folding (§6.4).

use unicode_normalization::UnicodeNormalization;

/// text(a..b): length in Unicode code points within [min, max]. (Rust strings are always valid UTF-8.)
pub fn len_between(s: &str, min: usize, max: usize) -> bool {
    // fast path: byte length bounds the char count
    let bl = s.len();
    if bl < min {
        return false;
    }
    if bl <= max && s.is_ascii() {
        return bl >= min;
    }
    let n = s.chars().count();
    n >= min && n <= max
}

/// `^[a-z0-9_.-]{1,64}$`
pub fn valid_username(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && b.iter().all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'.' || c == b'-')
}

/// N(s) = NFC(fullCaseFold(NFC(s)))
pub fn fold(s: &str) -> String {
    let nfc: String = s.nfc().collect();
    let folded = caseless::default_case_fold_str(&nfc);
    folded.nfc().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        assert!(valid_username("yuma_kobayashi_186"));
        assert!(valid_username("soltor.5012"));
        assert!(!valid_username("Yuma"));
        assert!(!valid_username(""));
        assert!(!valid_username(&"a".repeat(65)));
        assert!(len_between("小林 悠真 186", 1, 10));
        assert!(!len_between("", 1, 100));
        assert!(len_between("", 0, 100));
        assert!(!len_between("あいう", 1, 2));
        assert_eq!(fold("Straße"), "strasse");
        assert!(fold("YUMA_Kobayashi").contains(&fold("kobaYASHI")));
    }
}
