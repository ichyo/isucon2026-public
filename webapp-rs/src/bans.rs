//! IP-ban detection (§8.1) and the ban table. Detection is local to the node that saw the abuse; bans are
//! replicated through the log (`Command::Ban` -> `LogEntry::IpBanned`).

use std::collections::VecDeque;

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;

use crate::http::Abuse;
use crate::model::Ts;

pub const BAN_DURATION_US: Ts = 120 * 1_000_000;

fn rule(a: Abuse) -> (usize, Ts) {
    match a {
        Abuse::FailedLogin => (10, 10_000_000),
        Abuse::InvalidImage => (2, 5_000_000),
        Abuse::InvalidSignature => (3, 5_000_000),
    }
}

#[derive(Default)]
pub struct Bans {
    table: RwLock<FxHashMap<String, Ts>>,
    counters: Mutex<FxHashMap<(String, Abuse), VecDeque<Ts>>>,
}

impl Bans {
    #[inline]
    pub fn is_banned(&self, ip: &str, now: Ts) -> bool {
        let t = self.table.read();
        if t.is_empty() {
            return false;
        }
        t.get(ip).is_some_and(|&until| now < until)
    }

    /// `bannedUntil = max(old, new)`
    pub fn set(&self, ip: &str, until: Ts) {
        let mut t = self.table.write();
        let e = t.entry(ip.to_string()).or_insert(until);
        if *e < until {
            *e = until;
        }
    }

    /// Records one abuse event; returns `Some(until)` when this event triggers a ban (counter reset).
    pub fn record(&self, ip: &str, abuse: Abuse, now: Ts) -> Option<Ts> {
        let (threshold, window) = rule(abuse);
        let mut c = self.counters.lock();
        let q = c.entry((ip.to_string(), abuse)).or_default();
        q.push_back(now);
        while q.front().is_some_and(|&t| t <= now - window) {
            q.pop_front();
        }
        if q.len() >= threshold {
            q.clear();
            Some(now + BAN_DURATION_US)
        } else {
            None
        }
    }

    /// Clears bans and counters (initialize).
    pub fn clear(&self) {
        self.table.write().clear();
        self.counters.lock().clear();
    }

    /// Drops expired bans and idle counters.
    pub fn purge(&self, now: Ts) {
        self.table.write().retain(|_, &mut until| until > now);
        self.counters.lock().retain(|(_, a), q| {
            let (_, window) = rule(*a);
            q.back().is_some_and(|&t| t > now - window)
        });
    }

    pub fn active(&self, now: Ts) -> usize {
        self.table.read().values().filter(|&&u| u > now).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds() {
        let b = Bans::default();
        let t = 1_000_000_000;
        for i in 0..9 {
            assert_eq!(b.record("1.2.3.4", Abuse::FailedLogin, t + i), None);
        }
        assert_eq!(b.record("1.2.3.4", Abuse::FailedLogin, t + 9), Some(t + 9 + BAN_DURATION_US));
        assert_eq!(b.record("1.2.3.4", Abuse::FailedLogin, t + 10), None); // reset
        assert_eq!(b.record("5.6.7.8", Abuse::InvalidImage, t), None);
        assert_eq!(b.record("5.6.7.8", Abuse::InvalidImage, t + 6_000_000), None); // first expired
        assert!(b.record("5.6.7.8", Abuse::InvalidImage, t + 7_000_000).is_some());
        b.set("5.6.7.8", t + 100);
        assert!(b.is_banned("5.6.7.8", t + 99));
        assert!(!b.is_banned("5.6.7.8", t + 100));
    }
}
