use anyhow::anyhow;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{TimeDelta, Utc};
use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::Sha256;

use crate::api_error::Error;
use crate::user::User;

pub const IMAGE_URL_LIFETIME: TimeDelta = TimeDelta::hours(24);
pub const IMAGE_SIGNATURE_ITERATIONS: usize = 1000;

const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b':')
    .remove(b'=')
    .remove(b'@');

pub fn image_signing_key(secret_key_hash: &str) -> Result<Vec<u8>, Error> {
    match hex::decode(secret_key_hash) {
        Ok(key) if key.len() == 32 => Ok(key),
        _ => Err(anyhow!("invalid image signing key").into()),
    }
}

pub fn image_signature(key: &[u8], image_id: &str, exp: i64) -> Vec<u8> {
    let mut value = format!("{exp}:{image_id}").into_bytes();
    for _ in 0..IMAGE_SIGNATURE_ITERATIONS {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(&value);
        value = mac.finalize().into_bytes().to_vec();
    }
    value
}

pub fn signed_image_url(user: &User, image_id: &str) -> String {
    let exp = (Utc::now() + IMAGE_URL_LIFETIME).timestamp();
    format!(
        "/api/images/{}?exp={}&sig={}",
        utf8_percent_encode(image_id, PATH_SEGMENT),
        exp,
        URL_SAFE_NO_PAD.encode(image_signature(&user.image_key, image_id, exp))
    )
}
