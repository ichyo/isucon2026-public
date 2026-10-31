//! Log entries (deterministic facts) and the record format shared by the WAL and the wire.
//!
//! Record = `[u32 len][u32 crc32(body)][body]`,
//! body   = `[u64 seq][u32 meta_len][postcard(LogEntry)][u8 blob_kind]` +
//!          kind 0: nothing | kind 1: `[u32 blob_len][blob]` | kind 2: `[u64 req_id]` (FromForward, wire only)
//!          | kind 3: `[u8 origin node][u64 key][u64 off][u32 len]` (large blob stored on its origin node).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::model::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogEntry {
    UserCreated { id: Id, username: Arc<str>, secret_hash: [u8; 32], display_name: Arc<str>, region: Region, at: Ts },
    SessionCreated { token: Id, user: Id, at: Ts },
    SessionEnded { token: Id },
    ProfileUpdated { user: Id, display_name: Option<Arc<str>>, bio: Option<Arc<str>>, region: Option<Region>, at: Ts },
    /// `image: Some` ⇒ the record carries the JPEG blob
    ProfileImageSet { user: Id, image: Option<Id>, at: Ts },
    AccountDeleted { user: Id, at: Ts },
    FriendRequestCreated { id: Id, requester: Id, recipient: Id, at: Ts },
    FriendRequestUpdated { id: Id, status: FrStatus, at: Ts },
    FriendshipCreated { id: Id, a: Id, b: Id, source_request: Option<Id>, at: Ts },
    FriendshipRemoved { a: Id, b: Id },
    FollowCreated { id: Id, follower: Id, followee: Id, at: Ts },
    FollowRemoved { follower: Id, followee: Id },
    /// cascade (in apply): removes friendship + follows both ways, cancels pending requests either way
    BlockCreated { id: Id, blocker: Id, blocked: Id, at: Ts },
    BlockRemoved { blocker: Id, blocked: Id },
    AdminSessionCreated { token: Id, admin: Id, at: Ts },
    AdminSessionEnded { token: Id },
    UserCertified { user: Id, at: Ts },
    /// ends the region's open window (ended_at = starts_at); notifications are implicit
    WindowOpened { id: Id, region: Region, local_date: Date, starts_at: Ts, on_time_deadline: Ts },
    /// record carries the JPEG blob
    PostCreated { id: Id, author: Id, window: Id, image: Id, caption: Arc<str>, visibility: Visibility, location: Location, at: Ts },
    PostUpdated { id: Id, caption: Option<Arc<str>>, visibility: Option<Visibility>, at: Ts },
    PostDeleted { id: Id, at: Ts },
    CommentCreated { id: Id, post: Id, author: Id, body: Arc<str>, at: Ts },
    CommentDeleted { id: Id, at: Ts },
    /// record carries the JPEG blob; previous current RealMoji of (post, reactor) gets replaced_at = at
    RealMojiCreated { id: Id, post: Id, reactor: Id, image: Id, reaction: Reaction, at: Ts },
    RealMojiDeleted { id: Id, at: Ts },
    IpBanned { ip: String, until: Ts },
}

#[derive(Clone, Copy, Debug)]
pub enum BlobOut<'a> {
    None,
    Inline(&'a [u8]),
    FromForward(u64),
    Ref(BlobRef),
}

pub const REC_HDR: usize = 8; // len + crc

/// Encodes one record. Returns (bytes, offset of the blob bytes within the record if inline).
pub fn encode_record(seq: u64, entry: &LogEntry, blob: BlobOut<'_>) -> (Vec<u8>, Option<usize>) {
    let meta = postcard::to_stdvec(entry).expect("postcard encode");
    let blob_extra = match blob {
        BlobOut::None => 0,
        BlobOut::Inline(b) => 4 + b.len(),
        BlobOut::FromForward(_) => 8,
        BlobOut::Ref(_) => 21,
    };
    let body_len = 8 + 4 + meta.len() + 1 + blob_extra;
    let mut out = Vec::with_capacity(REC_HDR + body_len);
    out.extend_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // crc placeholder
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    out.extend_from_slice(&meta);
    let mut blob_off = None;
    match blob {
        BlobOut::None => out.push(0),
        BlobOut::Inline(b) => {
            out.push(1);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            blob_off = Some(out.len());
            out.extend_from_slice(b);
        }
        BlobOut::FromForward(req) => {
            out.push(2);
            out.extend_from_slice(&req.to_le_bytes());
        }
        BlobOut::Ref(r) => {
            out.push(3);
            out.push(r.origin.0);
            out.extend_from_slice(&r.key.to_le_bytes());
            out.extend_from_slice(&r.off.to_le_bytes());
            out.extend_from_slice(&r.len.to_le_bytes());
        }
    }
    let crc = crc32fast::hash(&out[REC_HDR..]);
    out[4..8].copy_from_slice(&crc.to_le_bytes());
    (out, blob_off)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobIn {
    None,
    /// (offset within the record, len)
    Inline { off: usize, len: usize },
    FromForward(u64),
    Ref(BlobRef),
}

#[derive(Debug)]
pub struct Decoded {
    pub seq: u64,
    pub crc: u32,
    pub entry: LogEntry,
    pub blob: BlobIn,
    /// total record length (header included)
    pub len: usize,
}

#[derive(Debug)]
pub enum DecodeErr {
    /// need more bytes (torn tail)
    Short,
    Corrupt(&'static str),
}

/// Peeks the total length of the record at the start of `buf` (header included).
pub fn record_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < REC_HDR {
        return None;
    }
    Some(REC_HDR + u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize)
}

pub fn decode_record(buf: &[u8]) -> Result<Decoded, DecodeErr> {
    let total = record_len(buf).ok_or(DecodeErr::Short)?;
    if buf.len() < total {
        return Err(DecodeErr::Short);
    }
    let crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let body = &buf[REC_HDR..total];
    if crc32fast::hash(body) != crc {
        return Err(DecodeErr::Corrupt("crc"));
    }
    if body.len() < 13 {
        return Err(DecodeErr::Corrupt("short body"));
    }
    let seq = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let meta_len = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    if body.len() < 12 + meta_len + 1 {
        return Err(DecodeErr::Corrupt("meta len"));
    }
    let entry: LogEntry = postcard::from_bytes(&body[12..12 + meta_len]).map_err(|_| DecodeErr::Corrupt("postcard"))?;
    let kpos = 12 + meta_len;
    let blob = match body[kpos] {
        0 => BlobIn::None,
        1 => {
            if body.len() < kpos + 5 {
                return Err(DecodeErr::Corrupt("blob len"));
            }
            let len = u32::from_le_bytes(body[kpos + 1..kpos + 5].try_into().unwrap()) as usize;
            if body.len() != kpos + 5 + len {
                return Err(DecodeErr::Corrupt("blob size"));
            }
            BlobIn::Inline { off: REC_HDR + kpos + 5, len }
        }
        2 => {
            if body.len() != kpos + 9 {
                return Err(DecodeErr::Corrupt("fwd"));
            }
            BlobIn::FromForward(u64::from_le_bytes(body[kpos + 1..kpos + 9].try_into().unwrap()))
        }
        3 => {
            if body.len() != kpos + 22 {
                return Err(DecodeErr::Corrupt("ref"));
            }
            let Some(origin) = NodeId::from_u8(body[kpos + 1]) else {
                return Err(DecodeErr::Corrupt("ref origin"));
            };
            BlobIn::Ref(BlobRef {
                origin,
                key: u64::from_le_bytes(body[kpos + 2..kpos + 10].try_into().unwrap()),
                off: u64::from_le_bytes(body[kpos + 10..kpos + 18].try_into().unwrap()),
                len: u32::from_le_bytes(body[kpos + 18..kpos + 22].try_into().unwrap()),
            })
        }
        _ => return Err(DecodeErr::Corrupt("blob kind")),
    };
    Ok(Decoded { seq, crc, entry, blob, len: total })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let e = LogEntry::CommentCreated { id: Id::new_v4(), post: Id::new_v4(), author: Id::new_v4(), body: "hi".into(), at: 5 };
        let (r, off) = encode_record(7, &e, BlobOut::Inline(b"JPEGDATA"));
        let d = decode_record(&r).unwrap();
        assert_eq!(d.seq, 7);
        assert_eq!(d.len, r.len());
        let BlobIn::Inline { off: o, len } = d.blob else { panic!() };
        assert_eq!(Some(o), off);
        assert_eq!(&r[o..o + len], b"JPEGDATA");
        let (r2, _) = encode_record(8, &e, BlobOut::FromForward(99));
        assert_eq!(decode_record(&r2).unwrap().blob, BlobIn::FromForward(99));
        let br = BlobRef { origin: NodeId::ASIA2, key: 0xdead_beef, off: 1 << 33, len: 500_000 };
        let (r3, off3) = encode_record(9, &e, BlobOut::Ref(br));
        assert_eq!(off3, None);
        assert_eq!(decode_record(&r3).unwrap().blob, BlobIn::Ref(br));
        let mut bad = r.clone();
        let n = bad.len();
        bad[n - 1] ^= 1;
        assert!(matches!(decode_record(&bad), Err(DecodeErr::Corrupt(_))));
        assert!(matches!(decode_record(&r[..n - 1]), Err(DecodeErr::Short)));
    }
}
