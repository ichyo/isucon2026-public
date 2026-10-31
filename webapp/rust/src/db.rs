use std::str::FromStr;

use bigdecimal::BigDecimal;

macro_rules! args {
    ($($arg:expr),* $(,)?) => {{
        let mut args = sqlx::postgres::PgArguments::default();
        $(sqlx::Arguments::add(&mut args, $arg).map_err(|err| anyhow::anyhow!(err))?;)*
        args
    }};
}
pub(crate) use args;

pub fn in_placeholders(start: usize, count: usize) -> String {
    (start..start + count)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn numeric_to_f64(value: Option<BigDecimal>) -> Option<f64> {
    value.and_then(|value| value.to_string().parse().ok())
}

pub fn f64_to_numeric(value: Option<f64>) -> Option<BigDecimal> {
    value.and_then(|value| BigDecimal::from_str(&value.to_string()).ok())
}
