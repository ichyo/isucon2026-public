use std::env;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

#[derive(Debug, Clone)]
pub struct Config {
    pub region: String,
    pub db_host: String,
    pub db_port: String,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub log_level: String,
}

pub fn config_from_env() -> Config {
    Config {
        region: env_or_default("ISUREAL_REGION", "asia"),
        db_host: env_or_default("ISUREAL_DB_HOST", "127.0.0.1"),
        db_port: env_or_default("ISUREAL_DB_PORT", "5432"),
        db_user: env_or_default("ISUREAL_DB_USER", "isureal"),
        db_password: env_or_default("ISUREAL_DB_PASSWORD", "isureal"),
        db_name: env_or_default("ISUREAL_DB_NAME", "isureal"),
        log_level: env_or_default("ISUREAL_LOG_LEVEL", "debug"),
    }
}

const USER_PASSWORD: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b',')
    .remove(b';')
    .remove(b'=');

const PATH: &AsciiSet = &USER_PASSWORD.remove(b'/').remove(b':').remove(b'@');

impl Config {
    pub fn database_dsn(&self) -> String {
        let host = if self.db_host.contains(':') {
            format!("[{}]:{}", self.db_host, self.db_port)
        } else {
            format!("{}:{}", self.db_host, self.db_port)
        };
        format!(
            "postgres://{}:{}@{}/{}",
            utf8_percent_encode(&self.db_user, USER_PASSWORD),
            utf8_percent_encode(&self.db_password, USER_PASSWORD),
            host,
            utf8_percent_encode(&self.db_name, PATH),
        )
    }
}

fn env_or_default(key: &str, default_value: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => default_value.to_owned(),
    }
}
