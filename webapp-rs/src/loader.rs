//! Initial-data loader (docs/initial-data.md): CSV files + images.idx -> pristine `State`.
//! Tables are sorted by id so dense indices are identical on every node.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use csv::ByteRecord;
use rustc_hash::FxHashMap;

use crate::model::*;
use crate::state::*;
use crate::text;
use crate::timefmt;

struct Csv {
    rdr: csv::Reader<std::fs::File>,
    cols: FxHashMap<String, usize>,
    name: String,
}

impl Csv {
    fn open(dir: &Path, name: &str) -> Result<Option<Csv>> {
        let path = dir.join(name);
        if !path.exists() {
            eprintln!("[loader] WARN {} missing, treating as empty", path.display());
            return Ok(None);
        }
        let mut rdr = csv::ReaderBuilder::new().has_headers(true).buffer_capacity(1 << 20).from_path(&path).with_context(|| format!("open {}", path.display()))?;
        let hdr = rdr.byte_headers()?.clone();
        let cols = hdr.iter().enumerate().map(|(i, h)| (String::from_utf8_lossy(h).trim().to_string(), i)).collect();
        Ok(Some(Csv { rdr, cols, name: name.to_string() }))
    }
    fn col(&self, n: &str) -> Result<usize> {
        self.cols.get(n).copied().ok_or_else(|| anyhow!("{}: missing column {n}", self.name))
    }
    fn each(&mut self, mut f: impl FnMut(&ByteRecord) -> Result<()>) -> Result<()> {
        let mut rec = ByteRecord::new();
        let mut line = 1u64;
        while self.rdr.read_byte_record(&mut rec)? {
            line += 1;
            f(&rec).with_context(|| format!("{} record {line}", self.name))?;
        }
        Ok(())
    }
}

fn field<'a>(r: &'a ByteRecord, c: usize) -> Result<&'a [u8]> {
    r.get(c).ok_or_else(|| anyhow!("missing field {c}"))
}
fn f_id(r: &ByteRecord, c: usize) -> Result<Id> {
    let f = field(r, c)?;
    Id::parse_bytes(f).ok_or_else(|| anyhow!("bad uuid {:?}", String::from_utf8_lossy(f)))
}
fn f_opt_id(r: &ByteRecord, c: usize) -> Result<Option<Id>> {
    if field(r, c)?.is_empty() {
        Ok(None)
    } else {
        f_id(r, c).map(Some)
    }
}
fn f_str<'a>(r: &'a ByteRecord, c: usize) -> Result<&'a str> {
    std::str::from_utf8(field(r, c)?).map_err(|e| anyhow!("utf8: {e}"))
}
fn f_arc(r: &ByteRecord, c: usize) -> Result<Arc<str>> {
    Ok(Arc::from(f_str(r, c)?))
}
fn f_i64(r: &ByteRecord, c: usize) -> Result<i64> {
    let s = f_str(r, c)?;
    s.parse().map_err(|_| anyhow!("bad int {s:?}"))
}
fn f_opt_i64(r: &ByteRecord, c: usize) -> Result<Option<i64>> {
    if field(r, c)?.is_empty() {
        Ok(None)
    } else {
        f_i64(r, c).map(Some)
    }
}
fn f_f64(r: &ByteRecord, c: usize) -> Result<f64> {
    let s = f_str(r, c)?;
    s.parse().map_err(|_| anyhow!("bad float {s:?}"))
}
fn f_hash(r: &ByteRecord, c: usize) -> Result<[u8; 32]> {
    let s = field(r, c)?;
    hex32(s).ok_or_else(|| anyhow!("bad hash"))
}
pub fn hex32(s: &[u8]) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let h = (s[i * 2] as char).to_digit(16)?;
        let l = (s[i * 2 + 1] as char).to_digit(16)?;
        out[i] = (h * 16 + l) as u8;
    }
    Some(out)
}

struct RawUser {
    id: Id,
    username: Arc<str>,
    secret_hash: [u8; 32],
    display_name: Arc<str>,
    bio: Arc<str>,
    profile_image: Option<Id>,
    region: Region,
    account_type: AccountType,
    deleted: bool,
    deleted_at: Option<Ts>,
    created_at: Ts,
    updated_at: Ts,
}

fn load_users(dir: &Path) -> Result<Vec<RawUser>> {
    let Some(mut c) = Csv::open(dir, "users.csv")? else { return Ok(vec![]) };
    let (ci, cu, ch, cd, cb, cp, cr, ca, cs, cdel, ccr, cup) = (
        c.col("id")?, c.col("username")?, c.col("secret_key_hash")?, c.col("display_name")?, c.col("bio")?, c.col("profile_image_id")?,
        c.col("region")?, c.col("account_type")?, c.col("status")?, c.col("deleted_at_us")?, c.col("created_at_us")?, c.col("updated_at_us")?,
    );
    let mut v = Vec::with_capacity(8192);
    c.each(|r| {
        let region = f_str(r, cr)?;
        let at = f_str(r, ca)?;
        v.push(RawUser {
            id: f_id(r, ci)?,
            username: f_arc(r, cu)?,
            secret_hash: f_hash(r, ch)?,
            display_name: f_arc(r, cd)?,
            bio: f_arc(r, cb)?,
            profile_image: f_opt_id(r, cp)?,
            region: Region::parse(region).ok_or_else(|| anyhow!("bad region {region}"))?,
            account_type: AccountType::parse(at).ok_or_else(|| anyhow!("bad account_type {at}"))?,
            deleted: f_str(r, cs)? == "deleted",
            deleted_at: f_opt_i64(r, cdel)?,
            created_at: f_i64(r, ccr)?,
            updated_at: f_i64(r, cup)?,
        });
        Ok(())
    })?;
    Ok(v)
}

fn load_admins(dir: &Path) -> Result<Vec<Admin>> {
    let Some(mut c) = Csv::open(dir, "administrators.csv")? else { return Ok(vec![]) };
    let (ci, cu, cp) = (c.col("id")?, c.col("username")?, c.col("password_hash")?);
    let mut v = Vec::new();
    c.each(|r| {
        v.push(Admin { id: f_id(r, ci)?, username: f_arc(r, cu)?, password_hash: f_hash(r, cp)? });
        Ok(())
    })?;
    Ok(v)
}

fn load_windows(dir: &Path) -> Result<Vec<Window>> {
    let Some(mut c) = Csv::open(dir, "posting_windows.csv")? else { return Ok(vec![]) };
    let (ci, cr, cd, cs, co, ce, cc) =
        (c.col("id")?, c.col("region")?, c.col("local_date")?, c.col("starts_at_us")?, c.col("on_time_deadline_us")?, c.col("ended_at_us")?, c.col("created_at_us")?);
    let mut v = Vec::new();
    c.each(|r| {
        let region = f_str(r, cr)?;
        let d = f_str(r, cd)?;
        v.push(Window {
            id: f_id(r, ci)?,
            region: Region::parse(region).ok_or_else(|| anyhow!("bad region {region}"))?,
            local_date: timefmt::parse_date(d).ok_or_else(|| anyhow!("bad date {d}"))?,
            starts_at: f_i64(r, cs)?,
            on_time_deadline: f_i64(r, co)?,
            ended_at: OptTs::from(f_opt_i64(r, ce)?),
            created_at: f_i64(r, cc)?,
            posts: Vec::new(),
        });
        Ok(())
    })?;
    Ok(v)
}

struct RawPost {
    id: Id,
    author: Id,
    window: Id,
    image: Id,
    caption: Arc<str>,
    visibility: Visibility,
    location: Location,
    posted_at: Ts,
    deleted_at: Option<Ts>,
}

fn load_posts(dir: &Path) -> Result<Vec<RawPost>> {
    let Some(mut c) = Csv::open(dir, "posts.csv")? else { return Ok(vec![]) };
    let (ci, ca, cw, cim, cc, cv, ck, clat, clng, cloc, cp, cd) = (
        c.col("id")?, c.col("author_id")?, c.col("posting_window_id")?, c.col("image_id")?, c.col("caption")?, c.col("visibility")?,
        c.col("location_kind")?, c.col("latitude")?, c.col("longitude")?, c.col("locality")?, c.col("posted_at_us")?, c.col("deleted_at_us")?,
    );
    let mut v = Vec::with_capacity(1 << 20);
    c.each(|r| {
        let vis = f_str(r, cv)?;
        let kind = f_str(r, ck)?;
        let location = match kind {
            "none" => Location::None,
            "exact" => Location::Exact { lat: f_f64(r, clat)?, lng: f_f64(r, clng)? },
            "approximate" => Location::Approximate { locality: f_arc(r, cloc)? },
            _ => bail!("bad location_kind {kind}"),
        };
        v.push(RawPost {
            id: f_id(r, ci)?,
            author: f_id(r, ca)?,
            window: f_id(r, cw)?,
            image: f_id(r, cim)?,
            caption: f_arc(r, cc)?,
            visibility: Visibility::parse(vis).ok_or_else(|| anyhow!("bad visibility {vis}"))?,
            location,
            posted_at: f_i64(r, cp)?,
            deleted_at: f_opt_i64(r, cd)?,
        });
        Ok(())
    })?;
    Ok(v)
}

struct RawRm {
    id: Id,
    post: Id,
    reactor: Id,
    image: Id,
    reaction: Reaction,
    created_at: Ts,
    replaced_at: Option<Ts>,
    deleted_at: Option<Ts>,
}

fn load_realmojis(dir: &Path) -> Result<Vec<RawRm>> {
    let Some(mut c) = Csv::open(dir, "realmojis.csv")? else { return Ok(vec![]) };
    let (ci, cp, cr, cim, ct, cc, crp, cd) = (
        c.col("id")?, c.col("post_id")?, c.col("reactor_id")?, c.col("image_id")?, c.col("reaction_type")?, c.col("created_at_us")?,
        c.col("replaced_at_us")?, c.col("deleted_at_us")?,
    );
    let mut v = Vec::with_capacity(1 << 21);
    c.each(|r| {
        let t = f_str(r, ct)?;
        v.push(RawRm {
            id: f_id(r, ci)?,
            post: f_id(r, cp)?,
            reactor: f_id(r, cr)?,
            image: f_id(r, cim)?,
            reaction: Reaction::parse(t).ok_or_else(|| anyhow!("bad reaction {t}"))?,
            created_at: f_i64(r, cc)?,
            replaced_at: f_opt_i64(r, crp)?,
            deleted_at: f_opt_i64(r, cd)?,
        });
        Ok(())
    })?;
    Ok(v)
}

struct RawComment {
    id: Id,
    post: Id,
    author: Id,
    body: Arc<str>,
    created_at: Ts,
    deleted_at: Option<Ts>,
}

fn load_comments(dir: &Path) -> Result<Vec<RawComment>> {
    let Some(mut c) = Csv::open(dir, "comments.csv")? else { return Ok(vec![]) };
    let (ci, cp, ca, cb, cc, cd) = (c.col("id")?, c.col("post_id")?, c.col("author_id")?, c.col("body")?, c.col("created_at_us")?, c.col("deleted_at_us")?);
    let mut v = Vec::with_capacity(1 << 20);
    c.each(|r| {
        v.push(RawComment {
            id: f_id(r, ci)?,
            post: f_id(r, cp)?,
            author: f_id(r, ca)?,
            body: f_arc(r, cb)?,
            created_at: f_i64(r, cc)?,
            deleted_at: f_opt_i64(r, cd)?,
        });
        Ok(())
    })?;
    Ok(v)
}

struct RawFr {
    id: Id,
    requester: Id,
    recipient: Id,
    status: FrStatus,
    created_at: Ts,
    updated_at: Ts,
}

fn load_frs(dir: &Path) -> Result<Vec<RawFr>> {
    let Some(mut c) = Csv::open(dir, "friend_requests.csv")? else { return Ok(vec![]) };
    let (ci, cq, cr, cs, cc, cu) = (c.col("id")?, c.col("requester_id")?, c.col("recipient_id")?, c.col("status")?, c.col("created_at_us")?, c.col("updated_at_us")?);
    let mut v = Vec::new();
    c.each(|r| {
        let s = f_str(r, cs)?;
        v.push(RawFr {
            id: f_id(r, ci)?,
            requester: f_id(r, cq)?,
            recipient: f_id(r, cr)?,
            status: FrStatus::parse(s).ok_or_else(|| anyhow!("bad status {s}"))?,
            created_at: f_i64(r, cc)?,
            updated_at: f_i64(r, cu)?,
        });
        Ok(())
    })?;
    Ok(v)
}

/// (id, a, b, at) rows for friendships (low, high, updated_at), follows (follower, followee, updated_at),
/// blocks (blocker, blocked, created_at).
fn load_pairs(dir: &Path, file: &str, a: &str, b: &str, at: &str) -> Result<Vec<(Id, Id, Id, Ts)>> {
    let Some(mut c) = Csv::open(dir, file)? else { return Ok(vec![]) };
    let (ci, ca, cb, ct) = (c.col("id")?, c.col(a)?, c.col(b)?, c.col(at)?);
    let mut v = Vec::new();
    c.each(|r| {
        v.push((f_id(r, ci)?, f_id(r, ca)?, f_id(r, cb)?, f_i64(r, ct)?));
        Ok(())
    })?;
    Ok(v)
}

/// images.idx: 28-byte records (uuid[16] BE, offset u64 LE, len u32 LE), sorted by uuid.
fn load_idx(dir: &Path) -> Result<Vec<(Id, u64, u32)>> {
    let path = dir.join("images.idx");
    if !path.exists() {
        eprintln!("[loader] WARN {} missing", path.display());
        return Ok(vec![]);
    }
    let data = std::fs::read(&path)?;
    if data.len() % 28 != 0 {
        bail!("images.idx size {} is not a multiple of 28", data.len());
    }
    let mut v: Vec<(Id, u64, u32)> = data
        .chunks_exact(28)
        .map(|c| {
            (
                Id::from_bytes(c[0..16].try_into().unwrap()),
                u64::from_le_bytes(c[16..24].try_into().unwrap()),
                u32::from_le_bytes(c[24..28].try_into().unwrap()),
            )
        })
        .collect();
    if !v.windows(2).all(|w| w[0].0 < w[1].0) {
        eprintln!("[loader] WARN images.idx not strictly sorted; sorting");
        v.sort_unstable_by_key(|x| x.0);
        v.dedup_by_key(|x| x.0);
    }
    Ok(v)
}

fn cmp_ts_id<'a, T>(v: &'a [T], key: impl Fn(&T) -> (Ts, Id) + 'a) -> impl Fn(&u32, &u32) -> std::cmp::Ordering + 'a {
    move |a, b| key(&v[*a as usize]).cmp(&key(&v[*b as usize]))
}

/// Loads the initial dataset from `dir`.
pub fn load(dir: &Path) -> Result<State> {
    let t0 = Instant::now();
    let res = std::thread::scope(|s| -> Result<_> {
        let h_rm = s.spawn(|| load_realmojis(dir));
        let h_cm = s.spawn(|| load_comments(dir));
        let h_idx = s.spawn(|| load_idx(dir));
        let h_small = s.spawn(|| -> Result<_> {
            Ok((
                load_users(dir)?,
                load_admins(dir)?,
                load_windows(dir)?,
                load_frs(dir)?,
                load_pairs(dir, "friendships.csv", "user_low_id", "user_high_id", "updated_at_us")?,
                load_pairs(dir, "follows.csv", "follower_id", "followee_id", "updated_at_us")?,
                load_pairs(dir, "block_relations.csv", "blocker_id", "blocked_id", "created_at_us")?,
            ))
        });
        let posts = load_posts(dir)?;
        let small = h_small.join().map_err(|_| anyhow!("loader thread panicked"))??;
        let rms = h_rm.join().map_err(|_| anyhow!("loader thread panicked"))??;
        let cms = h_cm.join().map_err(|_| anyhow!("loader thread panicked"))??;
        let idx = h_idx.join().map_err(|_| anyhow!("loader thread panicked"))??;
        Ok((small, posts, rms, cms, idx))
    })?;
    let ((mut users_raw, admins, mut windows, mut frs, fships, follows, blocks), mut posts_raw, mut rms_raw, mut cms_raw, idx) = res;
    let t_parse = t0.elapsed();

    let mut st = State::default();

    // users
    users_raw.sort_unstable_by_key(|u| u.id);
    for (i, u) in users_raw.into_iter().enumerate() {
        let idx = i as UIdx;
        st.user_by_id.insert(u.id, idx);
        st.user_by_name.insert(u.username.clone(), idx);
        st.users.push(User {
            id: u.id,
            fold_username: text::fold(&u.username).into(),
            fold_display: text::fold(&u.display_name).into(),
            username: u.username,
            secret_hash: u.secret_hash,
            display_name: u.display_name,
            bio: u.bio,
            profile_image: u.profile_image,
            region: u.region,
            account_type: u.account_type,
            deleted: u.deleted,
            created_at: u.created_at,
            updated_at: u.updated_at,
            deleted_at: OptTs::from(u.deleted_at),
            posts: Vec::new(),
            friends: Default::default(),
            following: Default::default(),
            followers: Default::default(),
            blocking: Default::default(),
            blocked_by: Default::default(),
            incoming: Default::default(),
            outgoing: Default::default(),
            session: None,
        });
    }
    let uid = |st: &State, id: Id| st.user_by_id.get(&id).copied().ok_or_else(|| anyhow!("unknown user {id}"));
    st.admins = admins;

    // windows
    windows.sort_unstable_by_key(|w| w.id);
    for (i, w) in windows.into_iter().enumerate() {
        let r = w.region.idx();
        st.window_by_id.insert(w.id, i as WIdx);
        st.last_date[r] = Some(st.last_date[r].map_or(w.local_date, |d| d.max(w.local_date)));
        if w.ended_at.is_none() {
            let better = match st.open_window[r] {
                None => true,
                Some(o) => st.windows[o as usize].starts_at < w.starts_at,
            };
            if better {
                st.open_window[r] = Some(i as WIdx);
            }
        }
        st.windows.push(w);
    }

    // posts
    posts_raw.sort_unstable_by_key(|p| p.id);
    let mut post_map = FxHashMap::with_capacity_and_hasher(posts_raw.len(), Default::default());
    let mut image_refs: Vec<(Id, ImageKind, UIdx, u32)> = Vec::with_capacity(posts_raw.len() + rms_raw.len() + st.users.len());
    st.posts.reserve_exact(posts_raw.len());
    for (i, p) in posts_raw.into_iter().enumerate() {
        let idx = i as PIdx;
        let a = uid(&st, p.author)?;
        let w = st.window_by_id.get(&p.window).copied().ok_or_else(|| anyhow!("post {} in unknown window {}", p.id, p.window))?;
        post_map.insert(p.id, idx);
        st.users[a as usize].posts.push(idx);
        st.windows[w as usize].posts.push(idx);
        image_refs.push((p.image, ImageKind::Post, a, idx));
        st.posts.push(Post {
            id: p.id,
            author: a,
            window: w,
            image: p.image,
            caption: p.caption,
            visibility: p.visibility,
            location: p.location,
            posted_at: p.posted_at,
            deleted_at: OptTs::from(p.deleted_at),
        });
    }
    {
        let posts = &st.posts;
        for u in st.users.iter_mut() {
            u.posts.sort_unstable_by(cmp_ts_id(posts, |p| (p.posted_at, p.id)));
        }
    }
    st.post_by_id = IdMap::from_base(post_map);

    // realmojis
    rms_raw.sort_unstable_by_key(|r| r.id);
    let np = st.posts.len();
    let mut rm_map = FxHashMap::with_capacity_and_hasher(rms_raw.len(), Default::default());
    let mut counts = vec![0u32; np + 1];
    st.realmojis.reserve_exact(rms_raw.len());
    for (i, r) in rms_raw.into_iter().enumerate() {
        let idx = i as RmIdx;
        let p = st.post_by_id.get(&r.post).ok_or_else(|| anyhow!("realmoji {} on unknown post {}", r.id, r.post))?;
        let a = uid(&st, r.reactor)?;
        rm_map.insert(r.id, idx);
        counts[p as usize] += 1;
        image_refs.push((r.image, ImageKind::RealMoji, a, idx));
        st.realmojis.push(RealMoji {
            id: r.id,
            post: p,
            reactor: a,
            image: r.image,
            reaction: r.reaction,
            created_at: r.created_at,
            replaced_at: OptTs::from(r.replaced_at),
            deleted_at: OptTs::from(r.deleted_at),
        });
    }
    st.realmoji_by_id = IdMap::from_base(rm_map);
    st.post_realmojis = build_csr(np, &counts, st.realmojis.iter().map(|r| r.post), cmp_ts_id(&st.realmojis, |r| (r.created_at, r.id)));

    // comments
    cms_raw.sort_unstable_by_key(|c| c.id);
    let mut cm_map = FxHashMap::with_capacity_and_hasher(cms_raw.len(), Default::default());
    let mut counts = vec![0u32; np + 1];
    st.comments.reserve_exact(cms_raw.len());
    for (i, c) in cms_raw.into_iter().enumerate() {
        let idx = i as CIdx;
        let p = st.post_by_id.get(&c.post).ok_or_else(|| anyhow!("comment {} on unknown post {}", c.id, c.post))?;
        let a = uid(&st, c.author)?;
        cm_map.insert(c.id, idx);
        counts[p as usize] += 1;
        st.comments.push(Comment { id: c.id, post: p, author: a, body: c.body, created_at: c.created_at, deleted_at: OptTs::from(c.deleted_at) });
    }
    st.comment_by_id = IdMap::from_base(cm_map);
    st.post_comments = build_csr(np, &counts, st.comments.iter().map(|c| c.post), cmp_ts_id(&st.comments, |c| (c.created_at, c.id)));

    // friend requests
    frs.sort_unstable_by_key(|f| f.id);
    for (i, f) in frs.into_iter().enumerate() {
        let idx = i as FrIdx;
        let a = uid(&st, f.requester)?;
        let b = uid(&st, f.recipient)?;
        st.fr_by_id.insert(f.id, idx);
        if f.status == FrStatus::Pending {
            st.users[a as usize].outgoing.insert(b, idx);
            st.users[b as usize].incoming.insert(a, idx);
        }
        st.friend_requests.push(FriendRequest { id: f.id, requester: a, recipient: b, status: f.status, created_at: f.created_at, updated_at: f.updated_at });
    }
    for (id, lo, hi, at) in fships {
        let (a, b) = (uid(&st, lo)?, uid(&st, hi)?);
        let rel = Rel { id, at };
        st.users[a as usize].friends.insert(b, rel);
        st.users[b as usize].friends.insert(a, rel);
    }
    for (id, fr, fe, at) in follows {
        let (a, b) = (uid(&st, fr)?, uid(&st, fe)?);
        let rel = Rel { id, at };
        st.users[a as usize].following.insert(b, rel);
        st.users[b as usize].followers.insert(a, rel);
    }
    for (id, br, bd, at) in blocks {
        let (a, b) = (uid(&st, br)?, uid(&st, bd)?);
        let rel = Rel { id, at };
        st.users[a as usize].blocking.insert(b, rel);
        st.users[b as usize].blocked_by.insert(a, rel);
    }

    // images: merge-join the referencing rows with images.idx
    for (i, u) in st.users.iter().enumerate() {
        if let Some(img) = u.profile_image {
            image_refs.push((img, ImageKind::Profile, i as UIdx, i as u32));
        }
    }
    image_refs.sort_unstable_by_key(|x| x.0);
    let mut base: Vec<(Id, ImageMeta)> = Vec::with_capacity(image_refs.len());
    let (mut i, mut j) = (0usize, 0usize);
    let mut missing = 0usize;
    while i < image_refs.len() {
        let (img, kind, owner, ref_idx) = image_refs[i];
        while j < idx.len() && idx[j].0 < img {
            j += 1;
        }
        if j < idx.len() && idx[j].0 == img {
            base.push((img, ImageMeta { kind, owner, ref_idx, loc: ImageLoc::Pack { off: idx[j].1, len: idx[j].2 } }));
        } else {
            missing += 1;
        }
        i += 1;
    }
    base.dedup_by_key(|x| x.0);
    if missing > 0 {
        eprintln!("[loader] WARN {missing} referenced images missing from images.idx");
    }
    st.images = ImageIndex::from_base(base);

    // search order
    let mut us: Vec<UIdx> = (0..st.users.len() as UIdx).collect();
    us.sort_unstable_by(|&a, &b| {
        let (ua, ub) = (&st.users[a as usize], &st.users[b as usize]);
        (&*ua.username, ua.id).cmp(&(&*ub.username, ub.id))
    });
    st.users_sorted = us;

    eprintln!("[loader] loaded {} (parse {:.2?}, total {:.2?})", st.summary(), t_parse, t0.elapsed());
    Ok(st)
}

fn build_csr(np: usize, counts: &[u32], posts_of: impl Iterator<Item = PIdx>, cmp: impl Fn(&u32, &u32) -> std::cmp::Ordering) -> PostLists {
    let mut off = vec![0u32; np + 1];
    for p in 0..np {
        off[p + 1] = off[p] + counts[p];
    }
    let total = off[np] as usize;
    let mut fill = off.clone();
    let mut items = vec![0u32; total];
    for (i, p) in posts_of.enumerate() {
        let f = &mut fill[p as usize];
        items[*f as usize] = i as u32;
        *f += 1;
    }
    for p in 0..np {
        let s = &mut items[off[p] as usize..off[p + 1] as usize];
        if s.len() > 1 {
            s.sort_unstable_by(&cmp);
        }
    }
    PostLists::from_base(off, items)
}
