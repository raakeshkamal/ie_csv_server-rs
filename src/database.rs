use anyhow::Result;
use mongodb::{Client, Database as MongoDatabase, bson::{doc, Bson}};
use mongodb::options::{UpdateOptions, FindOptions, IndexOptions};
use mongodb::IndexModel;
use futures::stream::StreamExt;
use crate::models::{TradingRecord, CashRecord};
use rust_decimal::Decimal;
use chrono::{NaiveDate, NaiveDateTime, Utc};
use std::str::FromStr;
use tracing::info;

pub struct Database {
    db: MongoDatabase,
}

impl Database {
    pub async fn new(uri: &str) -> Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        
        // Extract database name from URI or default to "bot_db"
        let db_name = if let Some(path) = uri.split('/').last() {
            if path.is_empty() { "bot_db" } else { path.split('?').next().unwrap_or("bot_db") }
        } else {
            "bot_db"
        };
        
        info!("Connecting to MongoDB database: {}", db_name);
        let db = client.database(db_name);
        let database = Database { db };
        database.create_indexes().await?;
        Ok(database)
    }

    async fn create_indexes(&self) -> Result<()> {
        // prices: ticker, date
        let prices_coll = self.db.collection::<Bson>("prices");
        prices_coll.create_index(
            IndexModel::builder()
                .keys(doc! { "ticker": 1, "date": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()
        ).await?;

        // isin_to_ticker: isin
        let isin_coll = self.db.collection::<Bson>("isin_to_ticker");
        isin_coll.create_index(
            IndexModel::builder()
                .keys(doc! { "isin": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()
        ).await?;

        // precomputed tables unique keys
        let coll = self.db.collection::<Bson>("precomputed_portfolio_values");
        coll.create_index(
            IndexModel::builder()
                .keys(doc! { "date": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()
        ).await?;

        let coll = self.db.collection::<Bson>("precomputed_monthly_contributions");
        coll.create_index(
            IndexModel::builder()
                .keys(doc! { "month": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()
        ).await?;

        let coll = self.db.collection::<Bson>("precomputed_ticker_prices");
        coll.create_index(
            IndexModel::builder()
                .keys(doc! { "ticker": 1, "date": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()
        ).await?;

        let coll = self.db.collection::<Bson>("precomputed_ticker_daily_values");
        coll.create_index(
            IndexModel::builder()
                .keys(doc! { "date": 1, "ticker": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()
        ).await?;

        Ok(())
    }

    pub async fn get_external_cash_flows(&self) -> Result<Vec<(NaiveDate, Decimal)>> {
        let coll = self.db.collection::<mongodb::bson::Document>("cash_flows");
        let filter = doc! {
            "$or": [
                { "activity": { "$regex": "PAYMENT RECEIVED", "$options": "i" } },
                { "activity": { "$regex": "WITHDRAWAL", "$options": "i" } },
                { "activity": { "$regex": "ISA TRANSFER IN", "$options": "i" } }
            ]
        };
        let find_options = FindOptions::builder().sort(doc! { "date": 1 }).build();
        let mut cursor = coll.find(filter).with_options(find_options).await?;
        
        let mut results = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            let date_str = doc.get_str("date")?;
            let net_flow_str = doc.get_str("net_flow")?;
            results.push((
                NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap_or_default(),
                Decimal::from_str(net_flow_str).unwrap_or_default()
            ));
        }
        Ok(results)
    }

    pub async fn update_precompute_status(&self, status: &str, total_tickers: Option<usize>, error: Option<&str>) -> Result<String> {
        let coll = self.db.collection::<mongodb::bson::Document>("precompute_status");
        
        if status == "in_progress" {
            let mut doc = doc! {
                "status": status,
                "started_at": Utc::now().to_rfc3339(),
            };
            if let Some(t) = total_tickers {
                doc.insert("total_tickers", t as i64);
            }
            let res = coll.insert_one(doc).await?;
            Ok(res.inserted_id.to_string())
        } else {
            // Find latest
            let find_options = FindOptions::builder().sort(doc! { "_id": -1 }).limit(1).build();
            let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
            if let Some(result) = cursor.next().await {
                let doc = result?;
                let id = doc.get_object_id("_id")?;
                
                let mut update = doc! {
                    "status": status,
                };
                if status == "completed" {
                    update.insert("completed_at", Utc::now().to_rfc3339());
                } else if let Some(err) = error {
                    update.insert("last_error", err);
                }
                
                coll.update_one(doc! { "_id": id }, doc! { "$set": update }).await?;
                Ok(id.to_string())
            } else {
                // Should not happen if in_progress was called
                let mut doc = doc! {
                    "status": status,
                    "completed_at": Utc::now().to_rfc3339(),
                };
                if let Some(err) = error {
                    doc.insert("last_error", err);
                }
                let res = coll.insert_one(doc).await?;
                Ok(res.inserted_id.to_string())
            }
        }
    }

    pub async fn get_precompute_status(&self) -> Result<serde_json::Value> {
        let coll = self.db.collection::<mongodb::bson::Document>("precompute_status");
        let find_options = FindOptions::builder().sort(doc! { "_id": -1 }).limit(1).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        
        if let Some(result) = cursor.next().await {
            let doc = result?;
            Ok(serde_json::json!({
                "status": doc.get_str("status").unwrap_or("not_started"),
                "started_at": doc.get_str("started_at").unwrap_or(""),
                "completed_at": doc.get_str("completed_at").ok(),
                "total_tickers": doc.get_i64("total_tickers").ok(),
                "last_error": doc.get_str("last_error").ok(),
                "has_data": true,
            }))
        } else {
            Ok(serde_json::json!({
                "status": "not_started",
                "has_data": false,
            }))
        }
    }

    pub async fn get_portfolio_values_precomputed(&self) -> Result<Option<serde_json::Value>> {
        // Daily values
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_portfolio_values");
        let find_options = FindOptions::builder().sort(doc! { "date": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        
        let mut daily_dates = Vec::new();
        let mut daily_values = Vec::new();
        let mut daily_invested = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            daily_dates.push(doc.get_str("date")?.to_string());
            daily_values.push(doc.get_str("daily_value")?.parse::<f64>().unwrap_or(0.0));
            daily_invested.push(doc.get_str("invested_value").unwrap_or("0").parse::<f64>().unwrap_or(0.0));
        }

        if daily_dates.is_empty() {
            return Ok(None);
        }

        // Monthly contributions
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_monthly_contributions");
        let find_options = FindOptions::builder().sort(doc! { "month": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        let mut monthly_net = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            monthly_net.push(serde_json::json!({
                "Month": doc.get_str("month")?,
                "Net_Value": doc.get_str("net_value")?.parse::<f64>().unwrap_or(0.0),
            }));
        }

        // Ticker daily values
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_ticker_daily_values");
        let find_options = FindOptions::builder().sort(doc! { "date": 1, "ticker": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        let mut daily_ticker_values: std::collections::HashMap<String, Vec<f64>> = std::collections::HashMap::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            let ticker = doc.get_str("ticker")?;
            let val = doc.get_str("daily_value")?.parse::<f64>().unwrap_or(0.0);
            daily_ticker_values.entry(ticker.to_string()).or_default().push(val);
        }

        // Metrics
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_portfolio_metrics");
        let doc_opt = coll.find_one(doc! { "id": 1 }).await?;
        let portfolio_stats = if let Some(doc) = doc_opt {
            serde_json::json!({
                "irr": doc.get_str("irr")?.parse::<f64>().unwrap_or(0.0),
                "twr": doc.get_str("twr")?.parse::<f64>().unwrap_or(0.0),
                "total_invested": doc.get_str("total_invested")?.parse::<f64>().unwrap_or(0.0),
                "current_value": doc.get_str("current_value")?.parse::<f64>().unwrap_or(0.0),
                "profit_loss": doc.get_str("profit_loss")?.parse::<f64>().unwrap_or(0.0),
                "return_percentage": doc.get_str("return_percentage")?.parse::<f64>().unwrap_or(0.0),
                "calc_date": doc.get_str("calc_date")?,
                "last_updated": doc.get_str("last_updated")?,
            })
        } else {
            serde_json::json!({})
        };

        Ok(Some(serde_json::json!({
            "monthly_net": monthly_net,
            "daily_dates": daily_dates,
            "daily_values": daily_values,
            "daily_invested": daily_invested,
            "daily_ticker_values": daily_ticker_values,
            "portfolio_stats": portfolio_stats,
        })))
    }

    pub async fn get_all_precomputed_data(&self) -> Result<serde_json::Value> {
        // Ticker prices
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_ticker_prices");
        let find_options = FindOptions::builder().sort(doc! { "ticker": 1, "date": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        let mut ticker_prices = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            ticker_prices.push(serde_json::json!({
                "ticker": doc.get_str("ticker")?,
                "date": doc.get_str("date")?,
                "original_currency": doc.get_str("original_currency")?,
                "original_price": doc.get_str("original_price")?,
                "converted_price_gbp": doc.get_str("converted_price_gbp")?,
                "last_updated": doc.get_str("last_updated")?,
            }));
        }

        // Ticker daily values
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_ticker_daily_values");
        let find_options = FindOptions::builder().sort(doc! { "date": 1, "ticker": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        let mut ticker_daily_values = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            ticker_daily_values.push(serde_json::json!({
                "date": doc.get_str("date")?,
                "ticker": doc.get_str("ticker")?,
                "daily_value": doc.get_str("daily_value")?,
                "last_updated": doc.get_str("last_updated")?,
            }));
        }

        // Portfolio values
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_portfolio_values");
        let find_options = FindOptions::builder().sort(doc! { "date": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        let mut portfolio_values = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            portfolio_values.push(serde_json::json!({
                "date": doc.get_str("date")?,
                "daily_value": doc.get_str("daily_value")?,
                "last_updated": doc.get_str("last_updated")?,
            }));
        }

        // Monthly contributions
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_monthly_contributions");
        let find_options = FindOptions::builder().sort(doc! { "month": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        let mut monthly_contributions = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            monthly_contributions.push(serde_json::json!({
                "month": doc.get_str("month")?,
                "net_value": doc.get_str("net_value")?,
                "last_updated": doc.get_str("last_updated")?,
            }));
        }

        // Metrics
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_portfolio_metrics");
        let doc_opt = coll.find_one(doc! { "id": 1 }).await?;
        let metrics = if let Some(doc) = doc_opt {
            serde_json::json!({
                "irr": doc.get_str("irr")?,
                "twr": doc.get_str("twr")?,
                "total_invested": doc.get_str("total_invested")?,
                "current_value": doc.get_str("current_value")?,
                "profit_loss": doc.get_str("profit_loss")?,
                "return_percentage": doc.get_str("return_percentage")?,
                "calc_date": doc.get_str("calc_date")?,
                "last_updated": doc.get_str("last_updated")?,
            })
        } else {
            serde_json::json!({})
        };

        let status = self.get_precompute_status().await?;

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

    pub async fn clear_precomputed_data(&self) -> Result<()> {
        self.db.collection::<Bson>("precomputed_portfolio_values").delete_many(doc! {}).await?;
        self.db.collection::<Bson>("precomputed_monthly_contributions").delete_many(doc! {}).await?;
        self.db.collection::<Bson>("precomputed_ticker_prices").delete_many(doc! {}).await?;
        self.db.collection::<Bson>("precomputed_ticker_daily_values").delete_many(doc! {}).await?;
        self.db.collection::<Bson>("precomputed_portfolio_metrics").delete_many(doc! {}).await?;
        Ok(())
    }

    pub async fn save_precomputed_ticker_price(&self, ticker: &str, date: NaiveDate, currency: &str, original: Decimal, converted: Decimal) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_ticker_prices");
        let filter = doc! { "ticker": ticker, "date": date.to_string() };
        let update = doc! {
            "$set": {
                "ticker": ticker,
                "date": date.to_string(),
                "original_currency": currency,
                "original_price": original.to_string(),
                "converted_price_gbp": converted.to_string(),
                "last_updated": Utc::now().to_rfc3339(),
            }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn save_precomputed_portfolio_value(&self, date: NaiveDate, value: Decimal, invested: Decimal) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_portfolio_values");
        let filter = doc! { "date": date.to_string() };
        let update = doc! {
            "$set": {
                "date": date.to_string(),
                "daily_value": value.to_string(),
                "invested_value": invested.to_string(),
                "last_updated": Utc::now().to_rfc3339(),
            }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn save_precomputed_ticker_daily_value(&self, date: NaiveDate, ticker: &str, value: Decimal) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_ticker_daily_values");
        let filter = doc! { "date": date.to_string(), "ticker": ticker };
        let update = doc! {
            "$set": {
                "date": date.to_string(),
                "ticker": ticker,
                "daily_value": value.to_string(),
                "last_updated": Utc::now().to_rfc3339(),
            }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn save_precomputed_monthly_contribution(&self, month: &str, value: Decimal) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_monthly_contributions");
        let filter = doc! { "month": month };
        let update = doc! {
            "$set": {
                "month": month,
                "net_value": value.to_string(),
                "last_updated": Utc::now().to_rfc3339(),
            }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn save_precomputed_metrics(&self, irr: Decimal, twr: Decimal, invested: Decimal, current: Decimal, pl: Decimal, ret_pct: Decimal, calc_date: &str) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("precomputed_portfolio_metrics");
        let filter = doc! { "id": 1 };
        let update = doc! {
            "$set": {
                "id": 1,
                "irr": irr.to_string(),
                "twr": twr.to_string(),
                "total_invested": invested.to_string(),
                "current_value": current.to_string(),
                "profit_loss": pl.to_string(),
                "return_percentage": ret_pct.to_string(),
                "calc_date": calc_date,
                "last_updated": Utc::now().to_rfc3339(),
            }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn save_trades(&self, records: &[TradingRecord]) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("trades");
        coll.delete_many(doc! {}).await?;
        
        if records.is_empty() {
            return Ok(());
        }

        let docs: Vec<mongodb::bson::Document> = records.iter().map(|r| {
            doc! {
                "security_isin": &r.security_isin,
                "transaction_type": &r.transaction_type,
                "quantity": r.quantity.to_string(),
                "share_price": r.share_price.to_string(),
                "total_trade_value": r.total_trade_value.to_string(),
                "trade_date_time": r.trade_date_time.format("%Y-%m-%d %H:%M:%S").to_string(),
                "settlement_date": r.settlement_date.format("%Y-%m-%d %H:%M:%S").to_string(),
                "broker": &r.broker,
                "account_type": &r.account_type,
                "ticker": &r.ticker,
            }
        }).collect();

        coll.insert_many(docs).await?;
        Ok(())
    }

    pub async fn load_trades(&self) -> Result<Vec<TradingRecord>> {
        // Load mappings first for manual JOIN equivalent
        let mappings = self.get_isin_ticker_map().await?;
        
        let coll = self.db.collection::<mongodb::bson::Document>("trades");
        let mut cursor = coll.find(doc! {}).await?;
        
        let mut results = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            
            let dt_str = doc.get_str("trade_date_time")?;
            let trade_dt = NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| {
                    let mut s = dt_str.to_string();
                    if !s.contains(':') { s.push_str(" 00:00:00"); }
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                })
                .unwrap_or_default();
            
            let sett_str = doc.get_str("settlement_date")?;
            let sett_dt = NaiveDateTime::parse_from_str(sett_str, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| {
                    let mut s = sett_str.to_string();
                    if !s.contains(':') { s.push_str(" 00:00:00"); }
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                })
                .unwrap_or_default();

            let isin = doc.get_str("security_isin")?;
            // Prefer mapped_ticker from isin_to_ticker collection
            let ticker = mappings.get(isin).cloned()
                .or_else(|| doc.get_str("ticker").ok().map(|s| s.to_string()));

            results.push(TradingRecord {
                security_isin: isin.to_string(),
                transaction_type: doc.get_str("transaction_type")?.to_string(),
                quantity: Decimal::from_str(doc.get_str("quantity")?).unwrap_or_default(),
                share_price: Decimal::from_str(doc.get_str("share_price")?).unwrap_or_default(),
                total_trade_value: Decimal::from_str(doc.get_str("total_trade_value")?).unwrap_or_default(),
                trade_date_time: trade_dt,
                settlement_date: sett_dt,
                broker: doc.get_str("broker")?.to_string(),
                account_type: doc.get_str("account_type")?.to_string(),
                ticker,
            });
        }
        Ok(results)
    }

    async fn get_isin_ticker_map(&self) -> Result<std::collections::HashMap<String, String>> {
        let coll = self.db.collection::<mongodb::bson::Document>("isin_to_ticker");
        let mut cursor = coll.find(doc! {}).await?;
        let mut map = std::collections::HashMap::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            if let (Ok(isin), Ok(ticker)) = (doc.get_str("isin"), doc.get_str("ticker")) {
                map.insert(isin.to_string(), ticker.to_string());
            }
        }
        Ok(map)
    }

    pub async fn save_cash_flows(&self, records: &[CashRecord]) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("cash_flows");
        coll.delete_many(doc! {}).await?;
        
        if records.is_empty() {
            return Ok(());
        }

        let docs: Vec<mongodb::bson::Document> = records.iter().map(|r| {
            doc! {
                "date": r.date.to_string(),
                "activity": &r.activity,
                "credit": r.credit.map(|c| c.to_string()),
                "debit": r.debit.map(|d| d.to_string()),
                "balance": r.balance.to_string(),
                "account_type": &r.account_type,
                "net_flow": r.net_flow.to_string(),
            }
        }).collect();

        coll.insert_many(docs).await?;
        Ok(())
    }

    pub async fn load_cash_flows(&self) -> Result<Vec<CashRecord>> {
        let coll = self.db.collection::<mongodb::bson::Document>("cash_flows");
        let mut cursor = coll.find(doc! {}).await?;
        
        let mut results = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            results.push(CashRecord {
                date: NaiveDate::parse_from_str(doc.get_str("date")?, "%Y-%m-%d").unwrap_or_default(),
                activity: doc.get_str("activity")?.to_string(),
                credit: doc.get_str("credit").ok().and_then(|s| Decimal::from_str(s).ok()),
                debit: doc.get_str("debit").ok().and_then(|s| Decimal::from_str(s).ok()),
                balance: Decimal::from_str(doc.get_str("balance")?).unwrap_or_default(),
                account_type: doc.get_str("account_type")?.to_string(),
                net_flow: Decimal::from_str(doc.get_str("net_flow")?).unwrap_or_default(),
            });
        }
        Ok(results)
    }

    pub async fn save_isin_ticker_mapping(&self, isin: &str, ticker: &str, security_name: Option<&str>) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("isin_to_ticker");
        let filter = doc! { "isin": isin };
        let mut set_doc = doc! {
            "isin": isin,
            "ticker": ticker,
            "updated_at": Utc::now().to_rfc3339(),
        };
        if let Some(name) = security_name {
            set_doc.insert("security_name", name);
        }
        
        let update = doc! {
            "$set": set_doc,
            "$setOnInsert": { "created_at": Utc::now().to_rfc3339() }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn get_ticker_for_isin(&self, isin: &str) -> Result<Option<String>> {
        let coll = self.db.collection::<mongodb::bson::Document>("isin_to_ticker");
        let doc_opt = coll.find_one(doc! { "isin": isin }).await?;
        Ok(doc_opt.and_then(|d| d.get_str("ticker").ok().map(|s| s.to_string())))
    }

    pub async fn get_all_isin_ticker_mappings(&self) -> Result<Vec<serde_json::Value>> {
        let coll = self.db.collection::<mongodb::bson::Document>("isin_to_ticker");
        let find_options = FindOptions::builder().sort(doc! { "isin": 1 }).build();
        let mut cursor = coll.find(doc! {}).with_options(find_options).await?;
        
        let mut results = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result?;
            results.push(serde_json::json!({
                "isin": doc.get_str("isin")?,
                "ticker": doc.get_str("ticker")?,
                "security_name": doc.get_str("security_name").ok(),
                "created_at": doc.get_str("created_at").unwrap_or(""),
                "updated_at": doc.get_str("updated_at").unwrap_or(""),
            }));
        }
        Ok(results)
    }

    pub async fn get_isins_without_mappings(&self) -> Result<Vec<String>> {
        // This is a bit more complex in Mongo if we want to do it in one query, 
        // but we can just get all unique ISINs from trades and subtract mapped ones.
        let trades_coll = self.db.collection::<mongodb::bson::Document>("trades");
        let distinct_isins = trades_coll.distinct("security_isin", doc! { "security_isin": { "$ne": "" } }).await?;
        
        let mapped_coll = self.db.collection::<mongodb::bson::Document>("isin_to_ticker");
        let mut results = Vec::new();
        for val in distinct_isins {
            if let Some(isin) = val.as_str() {
                let count = mapped_coll.count_documents(doc! { "isin": isin }).await?;
                if count == 0 {
                    results.push(isin.to_string());
                }
            }
        }
        results.sort();
        Ok(results)
    }

    pub async fn delete_isin_ticker_mapping(&self, isin: &str) -> Result<bool> {
        let coll = self.db.collection::<mongodb::bson::Document>("isin_to_ticker");
        let res = coll.delete_one(doc! { "isin": isin }).await?;
        Ok(res.deleted_count > 0)
    }

    pub async fn save_price(&self, ticker: &str, date: NaiveDate, close: Decimal) -> Result<()> {
        let coll = self.db.collection::<mongodb::bson::Document>("prices");
        let filter = doc! { "ticker": ticker, "date": date.to_string() };
        let update = doc! {
            "$set": {
                "ticker": ticker,
                "date": date.to_string(),
                "close": close.to_string(),
            }
        };
        coll.update_one(filter, update).with_options(UpdateOptions::builder().upsert(true).build()).await?;
        Ok(())
    }

    pub async fn get_price(&self, ticker: &str, date: NaiveDate) -> Result<Option<Decimal>> {
        let coll = self.db.collection::<mongodb::bson::Document>("prices");
        let doc_opt = coll.find_one(doc! { "ticker": ticker, "date": date.to_string() }).await?;
        if let Some(doc) = doc_opt {
            Ok(Some(Decimal::from_str(doc.get_str("close")?).unwrap_or_default()))
        } else {
            Ok(None)
        }
    }

    pub async fn reset(&self) -> Result<()> {
        self.db.collection::<Bson>("trades").delete_many(doc! {}).await?;
        self.db.collection::<Bson>("cash_flows").delete_many(doc! {}).await?;
        self.db.collection::<Bson>("prices").delete_many(doc! {}).await?;
        // We keep isin_to_ticker mapping as it is valuable to keep
        self.clear_precomputed_data().await?;
        Ok(())
    }

    pub async fn has_trades_data(&self) -> Result<bool> {
        let coll = self.db.collection::<Bson>("trades");
        let count = coll.count_documents(doc! {}).await?;
        Ok(count > 0)
    }
}
