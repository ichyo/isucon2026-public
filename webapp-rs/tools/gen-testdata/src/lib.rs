//! Deterministic synthetic initial data in the format of docs/initial-data.md.
//!
//! * `Scale::Small`: ~30 named users, 10 ended windows per region, posts/realmojis/comments/relations, real
//!   distinct JPEGs, plus `secrets.csv` (username,secret_key,region,id) and sample upload JPEGs.
//! * `Scale::Full`: real row counts (5000 users, ~625k posts, ~1.2M realmojis, ~546k comments) with one shared
//!   JPEG blob, to measure load time / memory / initialize time.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scale {
    Small,
    Full,
}

pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
    pub fn uuid(&mut self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&self.next().to_be_bytes());
        b[8..].copy_from_slice(&self.next().to_be_bytes());
        b[6] = (b[6] & 0x0f) | 0x40;
        b[8] = (b[8] & 0x3f) | 0x80;
        b
    }
}

pub fn uuid_str(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn b64url(b: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for c in b.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        let chars = c.len() + 1;
        for i in 0..chars {
            out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

/// Deterministic 43-char secret key for a username (initial-data style).
pub fn secret_key_for(username: &str) -> String {
    b64url(&Sha256::digest(format!("isureal-test-secret:{username}").as_bytes()))
}

pub fn sha256_hex(s: &str) -> String {
    hex(&Sha256::digest(s.as_bytes()))
}

/// Encodes a w×h RGB JPEG with a pattern derived from `seed`.
pub fn make_jpeg(w: u16, h: u16, seed: u64) -> Vec<u8> {
    let mut px = Vec::with_capacity(w as usize * h as usize * 3);
    let (r0, g0, b0) = ((seed * 37 % 256) as u8, (seed * 91 % 256) as u8, (seed * 173 % 256) as u8);
    for y in 0..h as u32 {
        for x in 0..w as u32 {
            px.push(r0.wrapping_add((x * 3) as u8));
            px.push(g0.wrapping_add((y * 5) as u8));
            px.push(b0.wrapping_add(((x ^ y) * 2) as u8));
        }
    }
    let mut out = Vec::new();
    let enc = jpeg_encoder::Encoder::new(&mut out, 80);
    enc.encode(&px, w, h, jpeg_encoder::ColorType::Rgb).expect("jpeg encode");
    out
}

/// CSV field: NULL (None) = empty unquoted; strings are quoted when needed; empty string = `""`.
fn csv_str(s: &str) -> String {
    if s.is_empty() {
        return "\"\"".into();
    }
    if s.contains([',', '"', '\n', '\r']) || s.starts_with(' ') || s.ends_with(' ') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
fn csv_opt(o: Option<String>) -> String {
    o.unwrap_or_default()
}

struct Out {
    users: BufWriter<File>,
    admins: BufWriter<File>,
    windows: BufWriter<File>,
    posts: BufWriter<File>,
    rms: BufWriter<File>,
    comments: BufWriter<File>,
    frs: BufWriter<File>,
    fships: BufWriter<File>,
    follows: BufWriter<File>,
    blocks: BufWriter<File>,
    secrets: BufWriter<File>,
    pack: BufWriter<File>,
    pack_off: u64,
    idx: Vec<([u8; 16], u64, u32)>,
    shared_blob: Option<(u64, u32)>,
}

fn w(dir: &Path, name: &str, header: &str) -> io::Result<BufWriter<File>> {
    let mut f = BufWriter::with_capacity(1 << 20, File::create(dir.join(name))?);
    writeln!(f, "{header}")?;
    Ok(f)
}

impl Out {
    fn image(&mut self, id: [u8; 16], bytes: impl FnOnce() -> Vec<u8>) -> io::Result<()> {
        if let Some((off, len)) = self.shared_blob {
            self.idx.push((id, off, len));
            return Ok(());
        }
        let b = bytes();
        self.pack.write_all(&b)?;
        self.idx.push((id, self.pack_off, b.len() as u32));
        self.pack_off += b.len() as u64;
        Ok(())
    }
}

#[derive(Clone)]
struct U {
    id: [u8; 16],
    name: String,
    region: usize,
    certified: bool,
    deleted: bool,
}

const REGIONS: [&str; 3] = ["asia", "europe", "america"];
const VIS: [&str; 3] = ["friends", "friends_of_friends", "public"];
const REACT: [&str; 6] = ["like", "smile", "surprise", "heart", "laugh", "instant"];
const LOCALITIES: [&str; 5] = ["渋谷", "Shinjuku", "London, UK", "New \"York\"", "大阪"];

/// days since epoch for 2026-10-18 (first window date)
const DAY0: i64 = 20744;

pub struct Summary {
    pub users: usize,
    pub posts: usize,
    pub realmojis: usize,
    pub comments: usize,
    pub images: usize,
}

pub fn generate(dir: &Path, scale: Scale) -> io::Result<Summary> {
    fs::create_dir_all(dir)?;
    let mut rng = Rng::new(if scale == Scale::Small { 42 } else { 4242 });
    let mut o = Out {
        users: w(dir, "users.csv", "id,username,secret_key_hash,display_name,bio,profile_image_id,region,account_type,status,deleted_at_us,created_at_us,updated_at_us")?,
        admins: w(dir, "administrators.csv", "id,username,password_hash")?,
        windows: w(dir, "posting_windows.csv", "id,region,local_date,starts_at_us,on_time_deadline_us,ended_at_us,created_at_us")?,
        posts: w(dir, "posts.csv", "id,author_id,posting_window_id,image_id,caption,visibility,location_kind,latitude,longitude,locality,posted_at_us,created_at_us,deleted_at_us")?,
        rms: w(dir, "realmojis.csv", "id,post_id,reactor_id,image_id,reaction_type,created_at_us,replaced_at_us,deleted_at_us")?,
        comments: w(dir, "comments.csv", "id,post_id,author_id,body,created_at_us,deleted_at_us")?,
        frs: w(dir, "friend_requests.csv", "id,requester_id,recipient_id,status,created_at_us,updated_at_us")?,
        fships: w(dir, "friendships.csv", "id,user_low_id,user_high_id,source_request_id,created_at_us,updated_at_us")?,
        follows: w(dir, "follows.csv", "id,follower_id,followee_id,created_at_us,updated_at_us")?,
        blocks: w(dir, "block_relations.csv", "id,blocker_id,blocked_id,created_at_us")?,
        secrets: w(dir, "secrets.csv", "username,secret_key,region,id,status,account_type")?,
        pack: BufWriter::with_capacity(1 << 20, File::create(dir.join("images.pack"))?),
        pack_off: 0,
        idx: Vec::new(),
        shared_blob: None,
    };
    if scale == Scale::Full {
        let b = make_jpeg(320, 320, 7);
        o.pack.write_all(&b)?;
        o.shared_blob = Some((0, b.len() as u32));
        o.pack_off = b.len() as u64;
    }

    // admin
    writeln!(o.admins, "00000000-0000-0000-0000-000000000001,admin,{}", sha256_hex("password1234"))?;

    // users
    let mut users: Vec<U> = Vec::new();
    let t_user0: i64 = 1_730_073_600_000_000; // 2024-10-28
    match scale {
        Scale::Small => {
            let named: [(&str, usize, bool, bool); 16] = [
                ("alice", 0, false, false),
                ("bob", 0, false, false),
                ("carol", 0, true, false),
                ("dave", 0, false, false),
                ("erin", 0, false, true),
                ("frank", 0, false, false),
                ("grace", 0, false, false),
                ("heidi", 0, false, false),
                ("ivan", 0, false, false),
                ("judy", 0, false, false),
                ("emma", 1, false, false),
                ("felix", 1, true, false),
                ("gina", 1, false, false),
                ("aaron", 2, false, false),
                ("amy", 2, true, false),
                ("andy", 2, false, false),
            ];
            for (n, r, c, d) in named {
                users.push(U { id: rng.uuid(), name: n.into(), region: r, certified: c, deleted: d });
            }
            for i in 0..10 {
                users.push(U { id: rng.uuid(), name: format!("user_{i:02}"), region: 0, certified: false, deleted: i == 9 });
            }
        }
        Scale::Full => {
            for i in 0..5000usize {
                let region = if i < 4948 { 0 } else if i < 4971 { 1 } else { 2 };
                let certified = matches!(i, 10..=17 | 4950 | 4980);
                let deleted = !certified && i % 44 == 3 && region == 0;
                users.push(U { id: rng.uuid(), name: format!("user_{i}_{}", rng.below(1000)), region, certified, deleted });
            }
        }
    }
    let n = users.len();
    let mut profile_images: Vec<Option<[u8; 16]>> = vec![None; n];
    for (i, u) in users.iter().enumerate() {
        let has_pi = match scale {
            Scale::Small => matches!(u.name.as_str(), "alice" | "carol" | "emma" | "aaron" | "erin" | "felix"),
            Scale::Full => i % 2 == 0,
        };
        if has_pi {
            let id = rng.uuid();
            profile_images[i] = Some(id);
            o.image(id, || make_jpeg(24, 24, i as u64 + 1000))?;
        }
        let sk = secret_key_for(&u.name);
        let display = match scale {
            Scale::Small => {
                let mut c = u.name.chars();
                let cap: String = c.next().map(|f| f.to_uppercase().collect::<String>() + c.as_str()).unwrap_or_default();
                format!("{cap} 小林 {i}")
            }
            Scale::Full => format!("小林 悠真 {i}"),
        };
        let bio = if u.certified { format!("Official account of {}", u.name) } else if i % 3 == 0 { format!("hello, I am {}!", u.name) } else { String::new() };
        let created = t_user0 + i as i64 * 3_600_000_000;
        let deleted_at = u.deleted.then(|| (created + 86_400_000_000).to_string());
        writeln!(
            o.users,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            uuid_str(&u.id),
            u.name,
            sha256_hex(&sk),
            csv_str(&display),
            csv_str(&bio),
            csv_opt(profile_images[i].map(|x| uuid_str(&x))),
            REGIONS[u.region],
            if u.certified { "certified_public" } else { "regular" },
            if u.deleted { "deleted" } else { "active" },
            csv_opt(deleted_at),
            created,
            created
        )?;
        writeln!(o.secrets, "{},{},{},{},{},{}", u.name, sk, REGIONS[u.region], uuid_str(&u.id), if u.deleted { "deleted" } else { "active" }, if u.certified { "certified_public" } else { "regular" })?;
    }

    // friendships (+ accepted requests), pending/rejected/cancelled requests
    let mut friends: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut pair_used = std::collections::HashSet::new();
    let t_rel0: i64 = 1_750_000_000_000_000;
    let n_friend = if scale == Scale::Small { 40 } else { 24_985 };
    let mut k = 0;
    let mut tries = 0;
    while k < n_friend && tries < n_friend * 20 {
        tries += 1;
        let (a, b) = if scale == Scale::Small && k < n {
            // ring among same-region users first
            let a = k % n;
            let b = (a + 1) % n;
            (a, b)
        } else {
            (rng.below(n as u64) as usize, rng.below(n as u64) as usize)
        };
        if a == b || users[a].region != users[b].region && scale == Scale::Small {
            if scale == Scale::Small && k < n {
                k += 1;
            }
            continue;
        }
        let key = (a.min(b), a.max(b));
        if !pair_used.insert(key) {
            if scale == Scale::Small && k < n {
                k += 1;
            }
            continue;
        }
        let req = rng.uuid();
        let at = t_rel0 + k as i64 * 60_000_000;
        writeln!(o.frs, "{},{},{},accepted,{},{}", uuid_str(&req), uuid_str(&users[a].id), uuid_str(&users[b].id), at - 1_000_000, at)?;
        let (lo, hi) = if users[a].id < users[b].id { (a, b) } else { (b, a) };
        writeln!(o.fships, "{},{},{},{},{},{}", uuid_str(&rng.uuid()), uuid_str(&users[lo].id), uuid_str(&users[hi].id), uuid_str(&req), at, at)?;
        friends[a].push(b);
        friends[b].push(a);
        k += 1;
    }
    let n_other = if scale == Scale::Small { 8 } else { 3_985 };
    for i in 0..n_other {
        let a = rng.below(n as u64) as usize;
        let b = rng.below(n as u64) as usize;
        let key = (a.min(b), a.max(b));
        if a == b || !pair_used.insert(key) {
            continue;
        }
        let status = match i % 8 {
            0..=4 => "pending",
            5 | 6 => "rejected",
            _ => "cancelled",
        };
        let at = t_rel0 + (100_000 + i as i64) * 60_000_000;
        writeln!(o.frs, "{},{},{},{status},{},{}", uuid_str(&rng.uuid()), uuid_str(&users[a].id), uuid_str(&users[b].id), at - 5_000_000, at)?;
    }
    // follows to certified users
    let certified: Vec<usize> = (0..n).filter(|&i| users[i].certified).collect();
    let n_follow = if scale == Scale::Small { 12 } else { 10_020 };
    let mut fol_used = std::collections::HashSet::new();
    for i in 0..n_follow {
        let a = rng.below(n as u64) as usize;
        let c = certified[rng.below(certified.len() as u64) as usize];
        if a == c || !fol_used.insert((a, c)) {
            continue;
        }
        let at = t_rel0 + (200_000 + i as i64) * 1_000_000;
        writeln!(o.follows, "{},{},{},{},{}", uuid_str(&rng.uuid()), uuid_str(&users[a].id), uuid_str(&users[c].id), at, at)?;
    }
    // blocks
    let n_block = if scale == Scale::Small { 2 } else { 250 };
    let mut blk_used = std::collections::HashSet::new();
    for i in 0..n_block {
        let (a, b) = if scale == Scale::Small {
            if i == 0 {
                (3, 5) // dave blocks frank
            } else {
                (6, 19) // grace blocks user_03
            }
        } else {
            (rng.below(n as u64) as usize, rng.below(n as u64) as usize)
        };
        if a == b || !blk_used.insert((a, b)) || friends[a].contains(&b) {
            continue;
        }
        let at = t_rel0 + (300_000 + i as i64) * 1_000_000;
        writeln!(o.blocks, "{},{},{},{}", uuid_str(&rng.uuid()), uuid_str(&users[a].id), uuid_str(&users[b].id), at)?;
    }

    // windows: per region consecutive dates, all ended
    let ndays: i64 = if scale == Scale::Small { 10 } else { 365 };
    let day_start = DAY0 + 10 - ndays; // last date = 2026-10-27
    let mut windows: Vec<Vec<([u8; 16], i64, i64)>> = vec![Vec::new(); 3]; // (id, starts, deadline)
    for r in 0..3 {
        let mut starts: Vec<i64> = (0..ndays).map(|d| ((day_start + d) * 86400 + r as i64 * 8 * 3600 + 3600) * 1_000_000).collect();
        starts.push(starts[starts.len() - 1] + 6 * 3_600_000_000);
        for d in 0..ndays as usize {
            let id = rng.uuid();
            let s = starts[d];
            let dl = s + 120_000_000;
            let date = day_start + d as i64;
            writeln!(o.windows, "{},{},{},{},{},{},{}", uuid_str(&id), REGIONS[r], date_str(date), s, dl, starts[d + 1], s)?;
            windows[r].push((id, s, dl));
        }
    }

    // posts, realmojis, comments
    let (mut np, mut nrm, mut ncm) = (0usize, 0usize, 0usize);
    let post_prob = if scale == Scale::Small { (8, 10) } else { (34, 100) };
    for (ui, u) in users.iter().enumerate() {
        for (wi, (wid, ws, wdl)) in windows[u.region].iter().enumerate() {
            if !rng.chance(post_prob.0, post_prob.1) {
                continue;
            }
            let emit_post = |o: &mut Out, rng: &mut Rng, deleted: bool| -> io::Result<([u8; 16], i64)> {
                let pid = rng.uuid();
                let img = rng.uuid();
                let late = rng.chance(1, 5);
                let posted = if late { wdl + 1 + rng.below(3_600_000_000) as i64 } else { ws + rng.below(120_000_000) as i64 };
                let vis = VIS[rng.below(3) as usize];
                let (kind, lat, lng, loc) = match rng.below(4) {
                    0 => ("exact", format!("{}", (rng.below(180_000_000) as f64 / 1e6) - 90.0), format!("{}", (rng.below(360_000_000) as f64 / 1e6) - 180.0), String::new()),
                    1 => ("approximate", String::new(), String::new(), csv_str(LOCALITIES[rng.below(5) as usize])),
                    _ => ("none", String::new(), String::new(), String::new()),
                };
                let caption = if rng.chance(1, 10) { String::new() } else { format!("day {wi} by {} <&> #{}", u.name, rng.below(1000)) };
                let del = deleted.then(|| (posted + 3_600_000_000).to_string());
                writeln!(
                    o.posts,
                    "{},{},{},{},{},{vis},{kind},{lat},{lng},{loc},{posted},{posted},{}",
                    uuid_str(&pid),
                    uuid_str(&u.id),
                    uuid_str(wid),
                    uuid_str(&img),
                    csv_str(&caption),
                    csv_opt(del)
                )?;
                let seed = (ui * 1000 + wi) as u64;
                o.image(img, || make_jpeg(64, 48, seed))?;
                Ok((pid, posted))
            };
            if rng.chance(1, 20) {
                emit_post(&mut o, &mut rng, true)?;
                np += 1;
            }
            let (pid, posted) = emit_post(&mut o, &mut rng, false)?;
            np += 1;
            // realmojis by friends (or random users)
            let nr = rng.below(4) as usize;
            for j in 0..nr {
                let reactor = if !friends[ui].is_empty() { friends[ui][rng.below(friends[ui].len() as u64) as usize] } else { rng.below(n as u64) as usize };
                if reactor == ui {
                    continue;
                }
                let replaced_first = rng.chance(1, 10);
                let base_t = posted + (j as i64 + 1) * 60_000_000;
                if replaced_first {
                    let (rid, img) = (rng.uuid(), rng.uuid());
                    writeln!(o.rms, "{},{},{},{},{},{},{},", uuid_str(&rid), uuid_str(&pid), uuid_str(&users[reactor].id), uuid_str(&img), REACT[rng.below(6) as usize], base_t, base_t + 1_000_000)?;
                    let seed = nrm as u64 + 5000;
                    o.image(img, || make_jpeg(32, 32, seed))?;
                    nrm += 1;
                }
                let (rid, img) = (rng.uuid(), rng.uuid());
                writeln!(o.rms, "{},{},{},{},{},{},,", uuid_str(&rid), uuid_str(&pid), uuid_str(&users[reactor].id), uuid_str(&img), REACT[rng.below(6) as usize], base_t + 1_000_000)?;
                let seed = nrm as u64 + 5000;
                o.image(img, || make_jpeg(32, 32, seed))?;
                nrm += 1;
            }
            let nc = rng.below(if scale == Scale::Small { 4 } else { 3 }) as usize;
            for j in 0..nc {
                let author = if !friends[ui].is_empty() && rng.chance(3, 4) { friends[ui][rng.below(friends[ui].len() as u64) as usize] } else { ui };
                let t = posted + (j as i64 + 1) * 90_000_000;
                let del = rng.chance(1, 30).then(|| (t + 1_000_000).to_string());
                let body = if j % 2 == 0 { format!("nice, {}!", users[ui].name) } else { "line1\nline2 \"quoted\"".to_string() };
                writeln!(o.comments, "{},{},{},{},{},{}", uuid_str(&rng.uuid()), uuid_str(&pid), uuid_str(&users[author].id), csv_str(&body), t, csv_opt(del))?;
                ncm += 1;
            }
        }
    }

    // images.idx (sorted)
    o.idx.sort_unstable_by_key(|x| x.0);
    let mut idxw = BufWriter::with_capacity(1 << 20, File::create(dir.join("images.idx"))?);
    for (id, off, len) in &o.idx {
        idxw.write_all(id)?;
        idxw.write_all(&off.to_le_bytes())?;
        idxw.write_all(&len.to_le_bytes())?;
    }
    idxw.flush()?;
    for f in [&mut o.users, &mut o.admins, &mut o.windows, &mut o.posts, &mut o.rms, &mut o.comments, &mut o.frs, &mut o.fships, &mut o.follows, &mut o.blocks, &mut o.secrets, &mut o.pack] {
        f.flush()?;
    }

    // sample upload JPEGs
    if scale == Scale::Small {
        let jd = dir.parent().unwrap_or(dir).join("jpeg");
        fs::create_dir_all(&jd)?;
        fs::write(jd.join("post_640x480.jpg"), make_jpeg(640, 480, 1))?;
        fs::write(jd.join("realmoji_128.jpg"), make_jpeg(128, 128, 2))?;
        fs::write(jd.join("profile_96.jpg"), make_jpeg(96, 96, 3))?;
        fs::write(jd.join("big_600x600.jpg"), make_jpeg(600, 600, 4))?;
        fs::write(jd.join("huge_4100x8.jpg"), make_jpeg(4100, 8, 5))?;
        let mut trunc = make_jpeg(64, 64, 6);
        trunc.truncate(trunc.len() / 2);
        fs::write(jd.join("truncated.jpg"), trunc)?;
        fs::write(jd.join("invalid.jpg"), b"this is not a jpeg")?;
    }

    Ok(Summary { users: n, posts: np, realmojis: nrm, comments: ncm, images: o.idx.len() })
}

fn date_str(days: i64) -> String {
    // civil_from_days
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dates_and_keys() {
        assert_eq!(date_str(DAY0), "2026-10-18");
        assert_eq!(secret_key_for("alice").len(), 43);
    }
}
