use serde::{Deserialize, Deserializer, Serialize};
use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradingRecord {
    #[serde(rename = "Security / ISIN")]
    pub security_isin: String,
    #[serde(rename = "Transaction Type")]
    pub transaction_type: String,
    #[serde(rename = "Quantity")]
    #[serde(deserialize_with = "deserialize_decimal")]
    pub quantity: Decimal,
    #[serde(rename = "Share Price")]
    #[serde(deserialize_with = "deserialize_currency")]
    pub share_price: Decimal,
    #[serde(rename = "Total Trade Value")]
    #[serde(deserialize_with = "deserialize_currency")]
    pub total_trade_value: Decimal,
    #[serde(rename = "Trade Date/Time")]
    #[serde(deserialize_with = "deserialize_datetime")]
    pub trade_date_time: NaiveDateTime,
    #[serde(rename = "Settlement Date")]
    #[serde(deserialize_with = "deserialize_date_to_datetime")]
    pub settlement_date: NaiveDateTime,
    #[serde(rename = "Broker")]
    pub broker: String,
    #[serde(default, rename = "Account_Type")]
    pub account_type: String,
    #[serde(skip_deserializing)]
    pub ticker: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CashRecord {
    #[serde(rename = "Date")]
    #[serde(deserialize_with = "deserialize_date")]
    pub date: NaiveDate,
    #[serde(rename = "Activity")]
    pub activity: String,
    #[serde(rename = "Credit")]
    #[serde(deserialize_with = "deserialize_optional_currency")]
    pub credit: Option<Decimal>,
    #[serde(rename = "Debit")]
    #[serde(deserialize_with = "deserialize_optional_currency")]
    pub debit: Option<Decimal>,
    #[serde(rename = "Balance")]
    #[serde(deserialize_with = "deserialize_currency")]
    pub balance: Decimal,
    #[serde(default, rename = "Account_Type")]
    pub account_type: String,
    #[serde(default)]
    pub net_flow: Decimal,
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let s = s.replace(",", "");
    Decimal::from_str(&s).map_err(serde::de::Error::custom)
}

fn deserialize_currency<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let clean = s.replace("£", "").replace(",", "").trim().to_string();
    if clean.is_empty() {
        return Ok(Decimal::ZERO);
    }
    Decimal::from_str(&clean).map_err(serde::de::Error::custom)
}

fn deserialize_optional_currency<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    match s {
        Some(s) => {
            let clean = s.replace("£", "").replace(",", "").trim().to_string();
            if clean.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Decimal::from_str(&clean).map_err(serde::de::Error::custom)?))
            }
        }
        None => Ok(None),
    }
}

fn deserialize_datetime<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    // Try "%d/%m/%y %H:%M:%S"
    if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%d/%m/%y %H:%M:%S") {
        return Ok(dt);
    }
    // Try "%d/%m/%y"
    if let Ok(d) = NaiveDate::parse_from_str(&s, "%d/%m/%y") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap());
    }
    Err(serde::de::Error::custom(format!("Invalid datetime format: {}", s)))
}

fn deserialize_date_to_datetime<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if let Ok(d) = NaiveDate::parse_from_str(&s, "%d/%m/%y") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%d/%m/%y %H:%M:%S") {
        return Ok(dt);
    }
    Err(serde::de::Error::custom(format!("Invalid date format: {}", s)))
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, "%d/%m/%y").map_err(serde::de::Error::custom)
}
