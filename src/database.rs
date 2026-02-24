use anyhow::Result;
use rusqlite::{params, Connection};
use crate::models::{TradingRecord, CashRecord};
use rust_decimal::Decimal;
use chrono::{NaiveDate, NaiveDateTime};
use std::str::FromStr;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Database { conn };
        db.create_tables()?;
        Ok(db)
    }

    fn create_tables(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS trades (
                security_isin TEXT,
                transaction_type TEXT,
                quantity TEXT,
                share_price TEXT,
                total_trade_value TEXT,
                trade_date_time TEXT,
                settlement_date TEXT,
                broker TEXT,
                account_type TEXT,
                ticker TEXT
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cash_flows (
                date TEXT,
                activity TEXT,
                credit TEXT,
                debit TEXT,
                balance TEXT,
                account_type TEXT,
                net_flow TEXT
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS isin_to_ticker (
                isin TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                security_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS prices (
                ticker TEXT,
                date TEXT,
                close TEXT,
                PRIMARY KEY (ticker, date)
            )",
            [],
        )?;

        // Precomputed tables
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS precomputed_portfolio_values (
                date TEXT PRIMARY KEY,
                daily_value TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS precomputed_monthly_contributions (
                month TEXT PRIMARY KEY,
                net_value TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS precomputed_ticker_prices (
                ticker TEXT,
                date TEXT,
                original_currency TEXT,
                original_price TEXT,
                converted_price_gbp TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS precomputed_ticker_daily_values (
                date TEXT,
                ticker TEXT,
                daily_value TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, ticker)
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS precomputed_portfolio_metrics (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                irr TEXT,
                twr TEXT,
                total_invested TEXT,
                current_value TEXT,
                profit_loss TEXT,
                return_percentage TEXT,
                calc_date TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS precompute_status (
                id INTEGER PRIMARY KEY,
                status TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                total_tickers INTEGER,
                last_error TEXT
            )",
            [],
        )?;

        Ok(())
    }

    pub fn get_external_cash_flows(&self) -> Result<Vec<(NaiveDate, Decimal)>> {
        let mut stmt = self.conn.prepare(
            "SELECT date, net_flow FROM cash_flows 
             WHERE UPPER(activity) LIKE '%PAYMENT RECEIVED%' 
                OR UPPER(activity) LIKE '%WITHDRAWAL%' 
                OR UPPER(activity) LIKE '%ISA TRANSFER IN%'
             ORDER BY date"
        )?;
        let rows = stmt.query_map([], |row| {
            let date_str: String = row.get(0)?;
            let net_flow_str: String = row.get(1)?;
            Ok((
                NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").unwrap_or_default(),
                Decimal::from_str(&net_flow_str).unwrap_or_default()
            ))
        })?;
        
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn update_precompute_status(&self, status: &str, total_tickers: Option<usize>, error: Option<&str>) -> Result<i64> {
        if status == "in_progress" {
            self.conn.execute(
                "INSERT INTO precompute_status (status, started_at, total_tickers) VALUES (?1, CURRENT_TIMESTAMP, ?2)",
                params![status, total_tickers.map(|t| t as i64)],
            )?;
            Ok(self.conn.last_insert_rowid())
        } else {
            // Find last in_progress
            let mut stmt = self.conn.prepare("SELECT id FROM precompute_status ORDER BY id DESC LIMIT 1")?;
            let id: i64 = stmt.query_row([], |r| r.get(0))?;
            
            if status == "completed" {
                self.conn.execute(
                    "UPDATE precompute_status SET status = ?1, completed_at = CURRENT_TIMESTAMP WHERE id = ?2",
                    params![status, id],
                )?;
            } else {
                self.conn.execute(
                    "UPDATE precompute_status SET status = ?1, last_error = ?2 WHERE id = ?3",
                    params![status, error, id],
                )?;
            }
            Ok(id)
        }
    }

    pub fn get_precompute_status(&self) -> Result<serde_json::Value> {
        let mut stmt = self.conn.prepare(
            "SELECT status, started_at, completed_at, total_tickers, last_error 
             FROM precompute_status 
             ORDER BY id DESC LIMIT 1"
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            Ok(serde_json::json!({
                "status": row.get::<_, String>(0)?,
                "started_at": row.get::<_, String>(1)?,
                "completed_at": row.get::<_, Option<String>>(2)?,
                "total_tickers": row.get::<_, Option<i64>>(3)?,
                "last_error": row.get::<_, Option<String>>(4)?,
                "has_data": true,
            }))
        } else {
            Ok(serde_json::json!({
                "status": "not_started",
                "has_data": false,
            }))
        }
    }

    pub fn get_portfolio_values_precomputed(&self) -> Result<Option<serde_json::Value>> {
        // Daily values
        let mut stmt = self.conn.prepare(
            "SELECT date, daily_value FROM precomputed_portfolio_values ORDER BY date"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?.collect::<Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut daily_dates = Vec::new();
        let mut daily_values = Vec::new();
        for (date, val) in rows {
            daily_dates.push(date);
            daily_values.push(val.parse::<f64>().unwrap_or(0.0));
        }

        // Monthly contributions
        let mut stmt = self.conn.prepare(
            "SELECT month, net_value FROM precomputed_monthly_contributions ORDER BY month"
        )?;
        let monthly_net = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "Month": row.get::<_, String>(0)?,
                "Net_Value": row.get::<_, String>(1)?.parse::<f64>().unwrap_or(0.0),
            }))
        })?.collect::<Result<Vec<_>, _>>()?;

        // Ticker daily values
        let mut stmt = self.conn.prepare(
            "SELECT ticker, daily_value FROM precomputed_ticker_daily_values ORDER BY date, ticker"
        )?;
        let ticker_rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?.collect::<Result<Vec<_>, _>>()?;

        let mut daily_ticker_values: std::collections::HashMap<String, Vec<f64>> = std::collections::HashMap::new();
        for (ticker, val) in ticker_rows {
            daily_ticker_values.entry(ticker).or_default().push(val.parse::<f64>().unwrap_or(0.0));
        }

        // Metrics
        let mut stmt = self.conn.prepare(
            "SELECT irr, twr, total_invested, current_value, profit_loss, return_percentage, calc_date, last_updated 
             FROM precomputed_portfolio_metrics WHERE id = 1"
        )?;
        let portfolio_stats = stmt.query_row([], |row| {
            Ok(serde_json::json!({
                "irr": row.get::<_, String>(0)?.parse::<f64>().unwrap_or(0.0),
                "twr": row.get::<_, String>(1)?.parse::<f64>().unwrap_or(0.0),
                "total_invested": row.get::<_, String>(2)?.parse::<f64>().unwrap_or(0.0),
                "current_value": row.get::<_, String>(3)?.parse::<f64>().unwrap_or(0.0),
                "profit_loss": row.get::<_, String>(4)?.parse::<f64>().unwrap_or(0.0),
                "return_percentage": row.get::<_, String>(5)?.parse::<f64>().unwrap_or(0.0),
                "calc_date": row.get::<_, String>(6)?,
                "last_updated": row.get::<_, String>(7)?,
            }))
        }).unwrap_or(serde_json::json!({}));

        Ok(Some(serde_json::json!({
            "monthly_net": monthly_net,
            "daily_dates": daily_dates,
            "daily_values": daily_values,
            "daily_ticker_values": daily_ticker_values,
            "portfolio_stats": portfolio_stats,
        })))
    }

    pub fn get_all_precomputed_data(&self) -> Result<serde_json::Value> {
        // Ticker prices
        let mut stmt = self.conn.prepare(
            "SELECT ticker, date, original_currency, original_price, converted_price_gbp, last_updated 
             FROM precomputed_ticker_prices 
             ORDER BY ticker, date"
        )?;
        let ticker_prices = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "ticker": row.get::<_, String>(0)?,
                "date": row.get::<_, String>(1)?,
                "original_currency": row.get::<_, String>(2)?,
                "original_price": row.get::<_, String>(3)?,
                "converted_price_gbp": row.get::<_, String>(4)?,
                "last_updated": row.get::<_, String>(5)?,
            }))
        })?.collect::<Result<Vec<_>, _>>()?;

        // Ticker daily values
        let mut stmt = self.conn.prepare(
            "SELECT date, ticker, daily_value, last_updated 
             FROM precomputed_ticker_daily_values 
             ORDER BY date, ticker"
        )?;
        let ticker_daily_values = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "date": row.get::<_, String>(0)?,
                "ticker": row.get::<_, String>(1)?,
                "daily_value": row.get::<_, String>(2)?,
                "last_updated": row.get::<_, String>(3)?,
            }))
        })?.collect::<Result<Vec<_>, _>>()?;

        // Portfolio values
        let mut stmt = self.conn.prepare(
            "SELECT date, daily_value, last_updated 
             FROM precomputed_portfolio_values 
             ORDER BY date"
        )?;
        let portfolio_values = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "date": row.get::<_, String>(0)?,
                "daily_value": row.get::<_, String>(1)?,
                "last_updated": row.get::<_, String>(2)?,
            }))
        })?.collect::<Result<Vec<_>, _>>()?;

        // Monthly contributions
        let mut stmt = self.conn.prepare(
            "SELECT month, net_value, last_updated 
             FROM precomputed_monthly_contributions 
             ORDER BY month"
        )?;
        let monthly_contributions = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "month": row.get::<_, String>(0)?,
                "net_value": row.get::<_, String>(1)?,
                "last_updated": row.get::<_, String>(2)?,
            }))
        })?.collect::<Result<Vec<_>, _>>()?;

        // Metrics
        let mut stmt = self.conn.prepare(
            "SELECT irr, twr, total_invested, current_value, profit_loss, return_percentage, calc_date, last_updated 
             FROM precomputed_portfolio_metrics WHERE id = 1"
        )?;
        let metrics = stmt.query_row([], |row| {
            Ok(serde_json::json!({
                "irr": row.get::<_, String>(0)?,
                "twr": row.get::<_, String>(1)?,
                "total_invested": row.get::<_, String>(2)?,
                "current_value": row.get::<_, String>(3)?,
                "profit_loss": row.get::<_, String>(4)?,
                "return_percentage": row.get::<_, String>(5)?,
                "calc_date": row.get::<_, String>(6)?,
                "last_updated": row.get::<_, String>(7)?,
            }))
        }).unwrap_or(serde_json::json!({}));

        let status = self.get_precompute_status()?;

        Ok(serde_json::json!({
            "ticker_prices": ticker_prices,
            "ticker_daily_values": ticker_daily_values,
            "portfolio_values": portfolio_values,
            "monthly_contributions": monthly_contributions,
            "metrics": metrics,
            "status": status,
            "count": {
                "ticker_prices": ticker_prices.len(),
                "ticker_daily_values": ticker_daily_values.len(),
                "portfolio_values": portfolio_values.len(),
                "monthly_contributions": monthly_contributions.len(),
            }
        }))
    }

    pub fn clear_precomputed_data(&self) -> Result<()> {
        self.conn.execute("DELETE FROM precomputed_portfolio_values", [])?;
        self.conn.execute("DELETE FROM precomputed_monthly_contributions", [])?;
        self.conn.execute("DELETE FROM precomputed_ticker_prices", [])?;
        self.conn.execute("DELETE FROM precomputed_ticker_daily_values", [])?;
        self.conn.execute("DELETE FROM precomputed_portfolio_metrics", [])?;
        Ok(())
    }

    pub fn save_precomputed_ticker_price(&self, ticker: &str, date: NaiveDate, currency: &str, original: Decimal, converted: Decimal) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO precomputed_ticker_prices (ticker, date, original_currency, original_price, converted_price_gbp) 
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![ticker, date.to_string(), currency, original.to_string(), converted.to_string()],
        )?;
        Ok(())
    }

    pub fn save_precomputed_portfolio_value(&self, date: NaiveDate, value: Decimal) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO precomputed_portfolio_values (date, daily_value) VALUES (?1, ?2)",
            params![date.to_string(), value.to_string()],
        )?;
        Ok(())
    }

    pub fn save_precomputed_ticker_daily_value(&self, date: NaiveDate, ticker: &str, value: Decimal) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO precomputed_ticker_daily_values (date, ticker, daily_value) VALUES (?1, ?2, ?3)",
            params![date.to_string(), ticker, value.to_string()],
        )?;
        Ok(())
    }

    pub fn save_precomputed_monthly_contribution(&self, month: &str, value: Decimal) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO precomputed_monthly_contributions (month, net_value) VALUES (?1, ?2)",
            params![month, value.to_string()],
        )?;
        Ok(())
    }

    pub fn save_precomputed_metrics(&self, irr: Decimal, twr: Decimal, invested: Decimal, current: Decimal, pl: Decimal, ret_pct: Decimal, calc_date: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO precomputed_portfolio_metrics (id, irr, twr, total_invested, current_value, profit_loss, return_percentage, calc_date) 
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![irr.to_string(), twr.to_string(), invested.to_string(), current.to_string(), pl.to_string(), ret_pct.to_string(), calc_date],
        )?;
        Ok(())
    }

    pub fn save_trades(&self, records: &[TradingRecord]) -> Result<()> {
        self.conn.execute("DELETE FROM trades", [])?;
        let mut stmt = self.conn.prepare(
            "INSERT INTO trades (
                security_isin, transaction_type, quantity, share_price, 
                total_trade_value, trade_date_time, settlement_date, 
                broker, account_type, ticker
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;

        for r in records {
            stmt.execute(params![
                r.security_isin,
                r.transaction_type,
                r.quantity.to_string(),
                r.share_price.to_string(),
                r.total_trade_value.to_string(),
                r.trade_date_time.to_string(),
                r.settlement_date.to_string(),
                r.broker,
                r.account_type,
                r.ticker,
            ])?;
        }
        Ok(())
    }

    pub fn load_trades(&self) -> Result<Vec<TradingRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT t.*, m.ticker as mapped_ticker 
             FROM trades t 
             LEFT JOIN isin_to_ticker m ON t.security_isin = m.isin"
        )?;
        let records = stmt.query_map([], |row| {
            let dt_str: String = row.get(5)?;
            let trade_dt = NaiveDateTime::parse_from_str(&dt_str, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| {
                    let mut s = dt_str.clone();
                    if !s.contains(':') { s.push_str(" 00:00:00"); }
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                })
                .unwrap_or_default();
            
            let sett_str: String = row.get(6)?;
            let sett_dt = NaiveDateTime::parse_from_str(&sett_str, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| {
                    let mut s = sett_str.clone();
                    if !s.contains(':') { s.push_str(" 00:00:00"); }
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                })
                .unwrap_or_default();

            // Prefer mapped_ticker (from JOIN) over t.ticker
            let ticker: Option<String> = row.get::<_, Option<String>>(10)?
                .or_else(|| row.get::<_, Option<String>>(9).unwrap_or(None));

            Ok(TradingRecord {
                security_isin: row.get(0)?,
                transaction_type: row.get(1)?,
                quantity: Decimal::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                share_price: Decimal::from_str(&row.get::<_, String>(3)?).unwrap_or_default(),
                total_trade_value: Decimal::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                trade_date_time: trade_dt,
                settlement_date: sett_dt,
                broker: row.get(7)?,
                account_type: row.get(8)?,
                ticker,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    pub fn save_cash_flows(&self, records: &[CashRecord]) -> Result<()> {
        self.conn.execute("DELETE FROM cash_flows", [])?;
        let mut stmt = self.conn.prepare(
            "INSERT INTO cash_flows (
                date, activity, credit, debit, balance, account_type, net_flow
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;

        for r in records {
            stmt.execute(params![
                r.date.to_string(),
                r.activity,
                r.credit.map(|c| c.to_string()),
                r.debit.map(|d| d.to_string()),
                r.balance.to_string(),
                r.account_type,
                r.net_flow.to_string(),
            ])?;
        }
        Ok(())
    }

    pub fn load_cash_flows(&self) -> Result<Vec<CashRecord>> {
        let mut stmt = self.conn.prepare("SELECT * FROM cash_flows")?;
        let records = stmt.query_map([], |row| {
            Ok(CashRecord {
                date: NaiveDate::parse_from_str(&row.get::<_, String>(0)?, "%Y-%m-%d").unwrap_or_default(),
                activity: row.get(1)?,
                credit: row.get::<_, Option<String>>(2)?.and_then(|s| Decimal::from_str(&s).ok()),
                debit: row.get::<_, Option<String>>(3)?.and_then(|s| Decimal::from_str(&s).ok()),
                balance: Decimal::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                account_type: row.get(5)?,
                net_flow: Decimal::from_str(&row.get::<_, String>(6)?).unwrap_or_default(),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    pub fn save_isin_ticker_mapping(&self, isin: &str, ticker: &str, security_name: Option<&str>) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO isin_to_ticker (isin, ticker, security_name, updated_at) 
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
            params![isin, ticker, security_name],
        )?;
        Ok(())
    }

    pub fn get_ticker_for_isin(&self, isin: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare("SELECT ticker FROM isin_to_ticker WHERE isin = ?1")?;
        let mut rows = stmt.query(params![isin])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_all_isin_ticker_mappings(&self) -> Result<Vec<serde_json::Value>> {
        let mut stmt = self.conn.prepare("SELECT isin, ticker, security_name, created_at, updated_at FROM isin_to_ticker ORDER BY isin")?;
        let rows = stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "isin": row.get::<_, String>(0)?,
                "ticker": row.get::<_, String>(1)?,
                "security_name": row.get::<_, Option<String>>(2)?,
                "created_at": row.get::<_, String>(3)?,
                "updated_at": row.get::<_, String>(4)?,
            }))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn get_isins_without_mappings(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT security_isin FROM trades 
             WHERE security_isin IS NOT NULL AND security_isin != '' 
             AND security_isin NOT IN (SELECT isin FROM isin_to_ticker)
             ORDER BY security_isin"
        )?;
        let rows = stmt.query_map([], |row| row.get(0))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn delete_isin_ticker_mapping(&self, isin: &str) -> Result<bool> {
        let rows_affected = self.conn.execute(
            "DELETE FROM isin_to_ticker WHERE isin = ?1",
            params![isin],
        )?;
        Ok(rows_affected > 0)
    }

    pub fn save_price(&self, ticker: &str, date: NaiveDate, close: Decimal) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO prices (ticker, date, close) VALUES (?1, ?2, ?3)",
            params![ticker, date.to_string(), close.to_string()],
        )?;
        Ok(())
    }

    pub fn get_price(&self, ticker: &str, date: NaiveDate) -> Result<Option<Decimal>> {
        let mut stmt = self.conn.prepare("SELECT close FROM prices WHERE ticker = ?1 AND date = ?2")?;
        let mut rows = stmt.query(params![ticker, date.to_string()])?;
        if let Some(row) = rows.next()? {
            let s: String = row.get(0)?;
            Ok(Some(Decimal::from_str(&s).unwrap_or_default()))
        } else {
            Ok(None)
        }
    }

    pub fn reset(&self) -> Result<()> {
        self.conn.execute("DROP TABLE IF EXISTS trades", [])?;
        self.conn.execute("DROP TABLE IF EXISTS cash_flows", [])?;
        self.conn.execute("DROP TABLE IF EXISTS prices", [])?;
        // We keep isin_to_ticker mapping as it is valuable to keep
        self.create_tables()?;
        Ok(())
    }

    pub fn has_trades_data(&self) -> Result<bool> {
        let mut stmt = self.conn.prepare("SELECT count(*) FROM trades")?;
        let count: i64 = stmt.query_row([], |r| r.get(0))?;
        Ok(count > 0)
    }
}
