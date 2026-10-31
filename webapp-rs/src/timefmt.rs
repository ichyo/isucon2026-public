//! RFC3339Nano formatting (Go-compatible), RFC3339 parsing, calendar dates.
//! Parsing logic adapted from the reference Rust implementation (`webapp/rust/src/timefmt.rs`).

use crate::model::{Date, Ts};

/// days since 1970-01-01 -> (year, month, day)   (H. Hinnant's civil_from_days)
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (year, month, day) -> days since 1970-01-01
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64;
    let doy = (153 * mp + 2) / 5 + d as u64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe as i64 - 719468
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn push2(out: &mut Vec<u8>, v: u32) {
    out.push(b'0' + (v / 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}
fn push4(out: &mut Vec<u8>, v: i64) {
    let v = v.clamp(0, 9999) as u32;
    out.push(b'0' + (v / 1000) as u8);
    out.push(b'0' + (v / 100 % 10) as u8);
    out.push(b'0' + (v / 10 % 10) as u8);
    out.push(b'0' + (v % 10) as u8);
}

/// Go `time.RFC3339Nano` in UTC: `2026-10-31T01:17:22.1367Z` (fraction trimmed, omitted when 0).
pub fn write_ts(out: &mut Vec<u8>, us: Ts) {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000) as u32; // microseconds
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400) as u32;
    let (y, m, d) = civil_from_days(days);
    push4(out, y);
    out.push(b'-');
    push2(out, m);
    out.push(b'-');
    push2(out, d);
    out.push(b'T');
    push2(out, sod / 3600);
    out.push(b':');
    push2(out, sod / 60 % 60);
    out.push(b':');
    push2(out, sod % 60);
    if frac != 0 {
        out.push(b'.');
        let mut digits = [0u8; 6];
        let mut f = frac;
        for i in (0..6).rev() {
            digits[i] = b'0' + (f % 10) as u8;
            f /= 10;
        }
        let mut n = 6;
        while n > 0 && digits[n - 1] == b'0' {
            n -= 1;
        }
        out.extend_from_slice(&digits[..n]);
    }
    out.push(b'Z');
}

pub fn format_ts(us: Ts) -> String {
    let mut v = Vec::with_capacity(32);
    write_ts(&mut v, us);
    String::from_utf8(v).unwrap()
}

pub fn write_date(out: &mut Vec<u8>, d: Date) {
    let (y, m, dd) = civil_from_days(d.0 as i64);
    push4(out, y);
    out.push(b'-');
    push2(out, m);
    out.push(b'-');
    push2(out, dd);
}

pub fn format_date(d: Date) -> String {
    let mut v = Vec::with_capacity(10);
    write_date(&mut v, d);
    String::from_utf8(v).unwrap()
}

fn parse_uint(s: &[u8], minimum: u32, maximum: u32) -> Option<u32> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = s.iter().fold(0u32, |value, c| value * 10 + (c - b'0') as u32);
    (minimum..=maximum).contains(&value).then_some(value)
}

/// `YYYY-MM-DD` (real calendar date, year 0000..9999).
pub fn parse_date(value: &str) -> Option<Date> {
    let s = value.as_bytes();
    if s.len() != 10 || s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let y = parse_uint(&s[0..4], 0, 9999)? as i64;
    let m = parse_uint(&s[5..7], 1, 12)?;
    let d = parse_uint(&s[8..10], 1, 31)?;
    if d > days_in_month(y, m) {
        return None;
    }
    Some(Date(days_from_civil(y, m, d) as i32))
}

/// `YYYY-MM` -> [first day of month, first day of next month)
pub fn parse_month(value: &str) -> Option<(Date, Date)> {
    let s = value.as_bytes();
    if s.len() != 7 || s[4] != b'-' {
        return None;
    }
    let y = parse_uint(&s[0..4], 0, 9999)? as i64;
    let m = parse_uint(&s[5..7], 1, 12)?;
    let start = days_from_civil(y, m, 1);
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    let end = days_from_civil(ny, nm, 1);
    Some((Date(start as i32), Date(end as i32)))
}

/// RFC 3339 (`Z` or `±hh:mm`, optional fraction) -> µs since epoch, sub-µs truncated (floor), like the
/// reference (Go time.Time bound into a PostgreSQL timestamptz).
pub fn parse_rfc3339(value: &str) -> Option<Ts> {
    let s = value.as_bytes();
    if s.len() < "2006-01-02T15:04:05".len() {
        return None;
    }
    if !(s[4] == b'-' && s[7] == b'-' && s[10] == b'T' && s[13] == b':' && s[16] == b':') {
        return None;
    }
    let year = parse_uint(&s[0..4], 0, 9999)? as i64;
    let month = parse_uint(&s[5..7], 1, 12)?;
    let day = parse_uint(&s[8..10], 1, 31)?;
    let hour = parse_uint(&s[11..13], 0, 23)?;
    let minute = parse_uint(&s[14..16], 0, 59)?;
    let second = parse_uint(&s[17..19], 0, 59)?;
    if day > days_in_month(year, month) {
        return None;
    }
    let mut rest = &s[19..];
    let mut nanos: i64 = 0;
    if rest.len() >= 2 && rest[0] == b'.' && rest[1].is_ascii_digit() {
        let digits = rest[1..].iter().take_while(|c| c.is_ascii_digit()).count();
        nanos = rest[1..1 + digits]
            .iter()
            .chain(std::iter::repeat(&b'0'))
            .take(9)
            .fold(0i64, |n, c| n * 10 + (c - b'0') as i64);
        rest = &rest[1 + digits..];
    }
    let days = days_from_civil(year, month, day);
    let secs = days * 86400 + (hour * 3600 + minute * 60 + second) as i64;
    let mut offset: i64 = 0;
    if rest == b"Z" {
    } else {
        if rest.len() != 6 || !((rest[0] == b'-' || rest[0] == b'+') && rest[3] == b':') {
            return None;
        }
        let zh = parse_uint(&rest[1..3], 0, 23)? as i64;
        let zm = parse_uint(&rest[4..6], 0, 59)? as i64;
        offset = (zh * 60 + zm) * 60;
        if rest[0] == b'-' {
            offset = -offset;
        }
    }
    let total_ns: i128 = (secs - offset) as i128 * 1_000_000_000 + nanos as i128;
    Some(total_ns.div_euclid(1000) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format() {
        let t = parse_rfc3339("2026-10-31T01:17:22.136796Z").unwrap();
        assert_eq!(format_ts(t), "2026-10-31T01:17:22.136796Z");
        let t = parse_rfc3339("2025-11-21T04:25:33Z").unwrap();
        assert_eq!(format_ts(t), "2025-11-21T04:25:33Z");
        let t = parse_rfc3339("2026-10-31T01:17:22.100Z").unwrap();
        assert_eq!(format_ts(t), "2026-10-31T01:17:22.1Z");
        let t = parse_rfc3339("2026-10-31T10:17:22.1234569+09:00").unwrap();
        assert_eq!(format_ts(t), "2026-10-31T01:17:22.123456Z");
        assert_eq!(format_ts(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_ts(-1), "1969-12-31T23:59:59.999999Z");
        assert!(parse_rfc3339("2026-02-30T00:00:00Z").is_none());
        assert!(parse_rfc3339("2026-10-31T01:17:22").is_none());
        assert!(parse_rfc3339("").is_none());
    }

    #[test]
    fn dates() {
        let d = parse_date("2026-10-31").unwrap();
        assert_eq!(format_date(d), "2026-10-31");
        assert_eq!(format_date(parse_date("0001-01-01").unwrap()), "0001-01-01");
        assert!(parse_date("2026-13-01").is_none());
        assert!(parse_date("2025-02-29").is_none());
        assert!(parse_date("2024-02-29").is_some());
        let (a, b) = parse_month("2026-12").unwrap();
        assert_eq!(format_date(a), "2026-12-01");
        assert_eq!(format_date(b), "2027-01-01");
        assert!(parse_month("2026-1").is_none());
    }
}
