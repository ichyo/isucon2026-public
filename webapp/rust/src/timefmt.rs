use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use serde::Serializer;

pub fn rfc3339_nano<S: Serializer>(t: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format_rfc3339_nano(t))
}

pub fn format_rfc3339_nano(t: &DateTime<Utc>) -> String {
    let mut value = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = t.timestamp_subsec_nanos();
    if nanos != 0 {
        value.push('.');
        value.push_str(format!("{nanos:09}").trim_end_matches('0'));
    }
    value.push('Z');
    value
}

pub fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    let s = value.as_bytes();
    if s.len() < "2006-01-02T15:04:05".len() {
        return None;
    }
    if !(s[4] == b'-' && s[7] == b'-' && s[10] == b'T' && s[13] == b':' && s[16] == b':') {
        return None;
    }
    let year = parse_uint(&s[0..4], 0, 9999)?;
    let month = parse_uint(&s[5..7], 1, 12)?;
    let day = parse_uint(&s[8..10], 1, 31)?;
    let hour = parse_uint(&s[11..13], 0, 23)?;
    let minute = parse_uint(&s[14..16], 0, 59)?;
    let second = parse_uint(&s[17..19], 0, 59)?;
    let mut rest = &s[19..];

    let mut nanos = 0;
    if rest.len() >= 2 && rest[0] == b'.' && rest[1].is_ascii_digit() {
        let digits = rest[1..].iter().take_while(|c| c.is_ascii_digit()).count();
        nanos = rest[1..1 + digits]
            .iter()
            .chain(std::iter::repeat(&b'0'))
            .take(9)
            .fold(0, |nanos, c| nanos * 10 + (c - b'0') as u32);
        rest = &rest[1 + digits..];
    }

    let date = NaiveDate::from_ymd_opt(year as i32, month, day)?;
    let t = date
        .and_hms_nano_opt(hour, minute, second, nanos)?
        .and_utc();
    if rest == b"Z" {
        return Some(t);
    }
    if rest.len() != "-07:00".len() || !((rest[0] == b'-' || rest[0] == b'+') && rest[3] == b':') {
        return None;
    }
    let zone_hour = parse_uint(&rest[1..3], 0, 23)?;
    let zone_minute = parse_uint(&rest[4..6], 0, 59)?;
    let mut zone_offset = TimeDelta::seconds(((zone_hour * 60 + zone_minute) * 60) as i64);
    if rest[0] == b'-' {
        zone_offset = -zone_offset;
    }
    Some(t - zone_offset)
}

pub fn parse_date_only(value: &str) -> Option<NaiveDate> {
    let s = value.as_bytes();
    if s.len() != "2006-01-02".len() || s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    NaiveDate::from_ymd_opt(
        parse_uint(&s[0..4], 0, 9999)? as i32,
        parse_uint(&s[5..7], 1, 12)?,
        parse_uint(&s[8..10], 1, 31)?,
    )
}

pub fn parse_year_month(value: &str) -> Option<NaiveDate> {
    let s = value.as_bytes();
    if s.len() != "2006-01".len() || s[4] != b'-' {
        return None;
    }
    NaiveDate::from_ymd_opt(
        parse_uint(&s[0..4], 0, 9999)? as i32,
        parse_uint(&s[5..7], 1, 12)?,
        1,
    )
}

fn parse_uint(s: &[u8], minimum: u32, maximum: u32) -> Option<u32> {
    if !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = s.iter().fold(0, |value, c| value * 10 + (c - b'0') as u32);
    (minimum..=maximum).contains(&value).then_some(value)
}
