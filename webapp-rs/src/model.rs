//! Ids, enums and entity structs (the in-memory data model). See docs/design.md §2.

use std::fmt;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

pub type Ts = i64; // microseconds since the Unix epoch
pub type UIdx = u32;
pub type PIdx = u32;
pub type WIdx = u32;
pub type RmIdx = u32;
pub type CIdx = u32;
pub type FrIdx = u32;

/// 128-bit UUID. `Ord` equals the byte order of the lowercase 36-char string.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct Id {
    pub hi: u64,
    pub lo: u64,
}

const HEX: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn hexval(c: u8) -> Option<u64> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as u64),
        b'a'..=b'f' => Some((c - b'a' + 10) as u64),
        b'A'..=b'F' => Some((c - b'A' + 10) as u64),
        _ => None,
    }
}

impl Id {
    pub const NIL: Id = Id { hi: 0, lo: 0 };

    pub fn from_u128(v: u128) -> Id {
        Id { hi: (v >> 64) as u64, lo: v as u64 }
    }
    pub fn as_u128(self) -> u128 {
        ((self.hi as u128) << 64) | self.lo as u128
    }
    pub fn from_bytes(b: [u8; 16]) -> Id {
        Id::from_u128(u128::from_be_bytes(b))
    }
    pub fn to_bytes(self) -> [u8; 16] {
        self.as_u128().to_be_bytes()
    }
    /// Random UUID v4.
    pub fn new_v4() -> Id {
        Id::from_u128(uuid::Uuid::new_v4().as_u128())
    }
    /// Parses the canonical 36-char hyphenated form (hex digits in either case).
    pub fn parse(s: &str) -> Option<Id> {
        Id::parse_bytes(s.as_bytes())
    }
    pub fn parse_bytes(b: &[u8]) -> Option<Id> {
        if b.len() != 36 || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
            return None;
        }
        let mut v: u128 = 0;
        for (i, &c) in b.iter().enumerate() {
            if i == 8 || i == 13 || i == 18 || i == 23 {
                continue;
            }
            v = (v << 4) | hexval(c)? as u128;
        }
        Some(Id::from_u128(v))
    }
    /// Writes the 36-char lowercase form.
    pub fn write_to(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.to_buf());
    }
    pub fn to_buf(self) -> [u8; 36] {
        let mut buf = [0u8; 36];
        let v = self.as_u128();
        let mut j = 0;
        for i in 0..32 {
            if i == 8 || i == 12 || i == 16 || i == 20 {
                buf[j] = b'-';
                j += 1;
            }
            let nib = ((v >> (124 - i * 4)) & 0xf) as usize;
            buf[j] = HEX[nib];
            j += 1;
        }
        buf
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.to_buf();
        f.write_str(std::str::from_utf8(&b).unwrap())
    }
}
impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Compact optional timestamp (`i64::MIN` = none).
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptTs(i64);
impl OptTs {
    pub const NONE: OptTs = OptTs(i64::MIN);
    pub fn some(t: Ts) -> OptTs {
        OptTs(t)
    }
    pub fn from(o: Option<Ts>) -> OptTs {
        o.map(OptTs).unwrap_or(OptTs::NONE)
    }
    pub fn get(self) -> Option<Ts> {
        if self.0 == i64::MIN {
            None
        } else {
            Some(self.0)
        }
    }
    pub fn is_some(self) -> bool {
        self.0 != i64::MIN
    }
    pub fn is_none(self) -> bool {
        self.0 == i64::MIN
    }
}
impl Default for OptTs {
    fn default() -> Self {
        OptTs::NONE
    }
}
impl fmt::Debug for OptTs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

/// Calendar date as days since 1970-01-01.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Date(pub i32);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Region {
    Asia = 0,
    Europe = 1,
    America = 2,
}
impl Region {
    pub const ALL: [Region; 3] = [Region::Asia, Region::Europe, Region::America];
    pub fn parse(s: &str) -> Option<Region> {
        match s {
            "asia" => Some(Region::Asia),
            "europe" => Some(Region::Europe),
            "america" => Some(Region::America),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Region::Asia => "asia",
            Region::Europe => "europe",
            Region::America => "america",
        }
    }
    pub fn idx(self) -> usize {
        self as usize
    }
}

/// A server (node) of the cluster. Several nodes may share a region (asia1 + asia2).
/// Encoded as one byte (0..=3); values 0..=2 coincide with the old `Region` encoding of blob origins.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(pub u8);

impl NodeId {
    pub const ASIA1: NodeId = NodeId(0);
    pub const EUROPE1: NodeId = NodeId(1);
    pub const AMERICA1: NodeId = NodeId(2);
    pub const ASIA2: NodeId = NodeId(3);
    pub const COUNT: usize = 4;
    pub const ALL: [NodeId; 4] = [NodeId::ASIA1, NodeId::EUROPE1, NodeId::AMERICA1, NodeId::ASIA2];

    /// `asia1|asia2|europe1|america1`; a bare region name means that region's primary node.
    pub fn parse(s: &str) -> Option<NodeId> {
        match s {
            "asia1" | "asia" => Some(NodeId::ASIA1),
            "europe1" | "europe" => Some(NodeId::EUROPE1),
            "america1" | "america" => Some(NodeId::AMERICA1),
            "asia2" => Some(NodeId::ASIA2),
            _ => None,
        }
    }
    pub fn from_u8(v: u8) -> Option<NodeId> {
        ((v as usize) < NodeId::COUNT).then_some(NodeId(v))
    }
    pub fn name(self) -> &'static str {
        match self.0 {
            0 => "asia1",
            1 => "europe1",
            2 => "america1",
            3 => "asia2",
            _ => "unknown",
        }
    }
    pub fn region(self) -> Region {
        match self.0 {
            1 => Region::Europe,
            2 => Region::America,
            _ => Region::Asia,
        }
    }
    /// The node that historically serves a region (asia1/europe1/america1).
    pub fn primary(r: Region) -> NodeId {
        NodeId(r as u8)
    }
    pub fn idx(self) -> usize {
        self.0 as usize
    }
    /// Private IP of the contest server hosting this node.
    pub fn private_ip(self) -> &'static str {
        match self.0 {
            1 => "192.168.2.10",
            2 => "192.168.3.10",
            3 => "192.168.4.10",
            _ => "192.168.1.10",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum AccountType {
    Regular,
    CertifiedPublic,
}
impl AccountType {
    pub fn parse(s: &str) -> Option<AccountType> {
        match s {
            "regular" => Some(AccountType::Regular),
            "certified_public" => Some(AccountType::CertifiedPublic),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            AccountType::Regular => "regular",
            AccountType::CertifiedPublic => "certified_public",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Visibility {
    Friends,
    FriendsOfFriends,
    Public,
}
impl Visibility {
    pub fn parse(s: &str) -> Option<Visibility> {
        match s {
            "friends" => Some(Visibility::Friends),
            "friends_of_friends" => Some(Visibility::FriendsOfFriends),
            "public" => Some(Visibility::Public),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Visibility::Friends => "friends",
            Visibility::FriendsOfFriends => "friends_of_friends",
            Visibility::Public => "public",
        }
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum Location {
    None,
    Exact { lat: f64, lng: f64 },
    Approximate { locality: Arc<str> },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Reaction {
    Like,
    Smile,
    Surprise,
    Heart,
    Laugh,
    Instant,
}
impl Reaction {
    pub fn parse(s: &str) -> Option<Reaction> {
        match s {
            "like" => Some(Reaction::Like),
            "smile" => Some(Reaction::Smile),
            "surprise" => Some(Reaction::Surprise),
            "heart" => Some(Reaction::Heart),
            "laugh" => Some(Reaction::Laugh),
            "instant" => Some(Reaction::Instant),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Reaction::Like => "like",
            Reaction::Smile => "smile",
            Reaction::Surprise => "surprise",
            Reaction::Heart => "heart",
            Reaction::Laugh => "laugh",
            Reaction::Instant => "instant",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum FrStatus {
    Pending,
    Accepted,
    Rejected,
    Cancelled,
}
impl FrStatus {
    pub fn parse(s: &str) -> Option<FrStatus> {
        match s {
            "pending" => Some(FrStatus::Pending),
            "accepted" => Some(FrStatus::Accepted),
            "rejected" => Some(FrStatus::Rejected),
            "cancelled" => Some(FrStatus::Cancelled),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            FrStatus::Pending => "pending",
            FrStatus::Accepted => "accepted",
            FrStatus::Rejected => "rejected",
            FrStatus::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ImageKind {
    Post,
    RealMoji,
    Profile,
}

/// A relation row as seen from one side (friendship / follow / block): its id and its
/// `updatedAt` (friendship/follow: creation time; block: creation time).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Rel {
    pub id: Id,
    pub at: Ts,
}

#[derive(Clone, Debug)]
pub struct User {
    pub id: Id,
    pub username: Arc<str>,
    /// raw 32 bytes of hex_decode(secretKeyHash); also the image signing key
    pub secret_hash: [u8; 32],
    pub display_name: Arc<str>,
    pub bio: Arc<str>,
    pub profile_image: Option<Id>,
    pub region: Region,
    pub account_type: AccountType,
    pub deleted: bool,
    pub created_at: Ts,
    pub updated_at: Ts,
    pub deleted_at: OptTs,
    /// search keys: text::fold(username), text::fold(display_name)
    pub fold_username: Arc<str>,
    pub fold_display: Arc<str>,
    /// own posts (incl. deleted), sorted by (posted_at, id) ascending
    pub posts: Vec<PIdx>,
    /// friendships (both sides hold the same Rel); deleted users are kept (§4.1)
    pub friends: FxHashMap<UIdx, Rel>,
    pub following: FxHashMap<UIdx, Rel>,
    pub followers: FxHashMap<UIdx, Rel>,
    pub blocking: FxHashMap<UIdx, Rel>,
    pub blocked_by: FxHashMap<UIdx, Rel>,
    /// pending friend requests addressed to me, keyed by requester
    pub incoming: FxHashMap<UIdx, FrIdx>,
    /// pending friend requests sent by me, keyed by recipient
    pub outgoing: FxHashMap<UIdx, FrIdx>,
    /// Baseline from the legacy WAL or latest ownership transfer; the owner overlays its journal.
    pub session: Option<Id>,
    pub session_generation: u64,
}

impl User {
    pub fn is_active(&self) -> bool {
        !self.deleted
    }
    pub fn status_str(&self) -> &'static str {
        if self.deleted {
            "deleted"
        } else {
            "active"
        }
    }
}

#[derive(Clone, Debug)]
pub struct Admin {
    pub id: Id,
    pub username: Arc<str>,
    pub password_hash: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct Window {
    pub id: Id,
    pub region: Region,
    pub local_date: Date,
    pub starts_at: Ts,
    pub on_time_deadline: Ts,
    pub ended_at: OptTs,
    pub created_at: Ts,
    /// posts of this window (incl. deleted), in apply order
    pub posts: Vec<PIdx>,
}
impl Window {
    pub fn is_open(&self) -> bool {
        self.ended_at.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct Post {
    pub id: Id,
    pub author: UIdx,
    pub window: WIdx,
    pub image: Id,
    pub caption: Arc<str>,
    pub visibility: Visibility,
    pub location: Location,
    pub posted_at: Ts,
    pub deleted_at: OptTs,
}

#[derive(Clone, Copy, Debug)]
pub struct RealMoji {
    pub id: Id,
    pub post: PIdx,
    pub reactor: UIdx,
    pub image: Id,
    pub reaction: Reaction,
    pub created_at: Ts,
    pub replaced_at: OptTs,
    pub deleted_at: OptTs,
}
impl RealMoji {
    /// not replaced and not deleted
    pub fn is_current(&self) -> bool {
        self.replaced_at.is_none() && self.deleted_at.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct Comment {
    pub id: Id,
    pub post: PIdx,
    pub author: UIdx,
    pub body: Arc<str>,
    pub created_at: Ts,
    pub deleted_at: OptTs,
}

#[derive(Clone, Copy, Debug)]
pub struct FriendRequest {
    pub id: Id,
    pub requester: UIdx,
    pub recipient: UIdx,
    pub status: FrStatus,
    pub created_at: Ts,
    pub updated_at: Ts,
}

/// Where an image's bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageLoc {
    /// initial image inside images.pack
    Pack { off: u64, len: u32 },
    /// small uploaded image stored inline in the local WAL file
    Wal { off: u64, len: u32 },
    /// large uploaded image in this node's durable blob store (this node is its origin)
    Blob { key: u64, off: u64, len: u32 },
    /// large uploaded image living in another node's blob store (fetched on demand + cached locally)
    Remote { origin: NodeId, key: u64, off: u64, len: u32 },
}

/// Reference to a large blob in its origin node's durable blob store (carried in the log instead of the bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    /// node whose blobs.dat holds the bytes
    pub origin: NodeId,
    /// random key also written in the blob's header (guards against reading a different blob)
    pub key: u64,
    /// offset of the data in the origin's blobs.dat
    pub off: u64,
    pub len: u32,
}

impl BlobRef {
    /// Where this blob is read from on node `me`.
    pub fn loc_on(&self, me: NodeId) -> ImageLoc {
        if self.origin == me {
            ImageLoc::Blob { key: self.key, off: self.off, len: self.len }
        } else {
            ImageLoc::Remote { origin: self.origin, key: self.key, off: self.off, len: self.len }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ImageMeta {
    pub kind: ImageKind,
    pub owner: UIdx,
    /// PIdx (post image) / RmIdx (RealMoji image) / UIdx (profile image)
    pub ref_idx: u32,
    pub loc: ImageLoc,
}

pub fn now_us() -> Ts {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970");
    d.as_micros() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_roundtrip_and_order() {
        let s = "95a1b321-f2a8-4d4a-b068-4634710884b5";
        let id = Id::parse(s).unwrap();
        assert_eq!(id.to_string(), s);
        assert_eq!(Id::parse("95A1B321-F2A8-4D4A-B068-4634710884B5"), Some(id));
        assert!(Id::parse("95a1b321f2a84d4ab0684634710884b5").is_none());
        assert!(Id::parse("").is_none());
        let a = Id::parse("0a000000-0000-4000-8000-000000000000").unwrap();
        let b = Id::parse("a0000000-0000-4000-8000-000000000000").unwrap();
        assert!(a < b);
        assert!(a.to_string() < b.to_string());
        let v = Id::new_v4();
        assert_eq!(Id::parse(&v.to_string()), Some(v));
        assert_eq!(Id::from_bytes(v.to_bytes()), v);
    }
}
