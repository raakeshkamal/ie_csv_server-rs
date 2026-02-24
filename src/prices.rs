use anyhow::{Result, anyhow};
use chrono::{NaiveDate, Duration};
use rust_decimal::Decimal;
use yfinance_rs::{Ticker, YfClient, Range, Interval};
use std::collections::HashMap;
use crate::database::Database;
use std::str::FromStr;

pub struct PriceFetcher {
    client: YfClient,
}

impl PriceFetcher {
    pub fn new() -> Self {
        Self {
            client: YfClient::default(),
        }
    }

    pub async fn get_historical_prices(
        &self,
        ticker_symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<(NaiveDate, Decimal, String)>> {
        let ticker = Ticker::new(&self.client, ticker_symbol);
        
        // Fetch with Max range
        let history = ticker.history(Some(Range::Max), Some(Interval::D1), false).await
            .map_err(|e| anyhow!("Failed to fetch history for {}: {:?}", ticker_symbol, e))?;
        
        tracing::debug!("yfinance-rs returned {} bars for {}", history.len(), ticker_symbol);
        
        let mut prices = Vec::new();
        for (i, bar) in history.iter().enumerate() {
            let date = bar.ts.date_naive();
            
            if date >= start_date && date <= end_date {
                let close_str = bar.close.to_string();
                // Extract currency if present (e.g., "15.53 USD" -> "USD")
                let currency = close_str.split_whitespace().last().unwrap_or("GBP").to_string();
                let clean_close = close_str.chars().filter(|c| c.is_digit(10) || *c == '.').collect::<String>();
                
                if let Ok(mut dec) = Decimal::from_str(&clean_close) {
                    // AUTO-DETECT: LSE (.L) tickers are usually Pence (GBX) if > 250 and labeled GBP.
                    // This handles the common Yahoo Finance inconsistency where Pence are labeled GBP.
                    if ticker_symbol.ends_with(".L") && currency == "GBP" && dec > Decimal::from(250) {
                        dec = dec / Decimal::from(100);
                    }
                    prices.push((date, dec, currency));
                } else {
                    if i == 0 {
                        tracing::error!("Failed to parse decimal from '{}' (cleaned: '{}')", close_str, clean_close);
                    }
                }
            }
        }
        
        if prices.is_empty() {
            tracing::warn!("No prices found for {} between {} and {} (History range: {} to {})", 
                ticker_symbol, start_date, end_date,
                history.first().map(|b| b.ts.date_naive().to_string()).unwrap_or_else(|| "N/A".to_string()),
                history.last().map(|b| b.ts.date_naive().to_string()).unwrap_or_else(|| "N/A".to_string())
            );
        } else {
            tracing::info!("Found {} prices for {} (First: {}, Last: {})", prices.len(), ticker_symbol, prices[0].0, prices.last().unwrap().0);
        }
        
        Ok(prices)
    }

    // Removed fetch_and_cache_prices from here to keep this Sync if possible, 
    // or we'll just handle it in background_processor.
}

pub struct CurrencyConverter {
    fetcher: PriceFetcher,
    fx_config: HashMap<String, (String, bool)>, // Currency -> (FX Ticker, Multiply)
}

impl CurrencyConverter {
    pub fn new() -> Self {
        let mut fx_config = HashMap::new();
        fx_config.insert("USD".to_string(), ("GBPUSD=X".to_string(), false));
        fx_config.insert("EUR".to_string(), ("EURGBP=X".to_string(), true));
        
        Self {
            fetcher: PriceFetcher::new(),
            fx_config,
        }
    }

    pub async fn convert_to_gbp(
        &self,
        amount: Decimal,
        currency: &str,
        date: NaiveDate,
        fx_rate: Option<Decimal>, // Pass rate externally
    ) -> Result<Decimal> {
        if currency == "GBP" || currency == "GBp" {
            if currency == "GBp" {
                return Ok(amount / Decimal::from(100));
            }
            return Ok(amount);
        }

        if let Some((_fx_ticker, multiply)) = self.fx_config.get(currency) {
            let rate = fx_rate.ok_or_else(|| anyhow!("FX rate required for {}", currency))?;
            if rate.is_zero() {
                return Err(anyhow!("FX rate is zero for {} on {}", currency, date));
            }
            if *multiply {
                Ok(amount * rate)
            } else {
                Ok(amount / rate)
            }
        } else {
            Err(anyhow!("Unknown currency: {}", currency))
        }
    }
    
    pub fn get_fx_ticker(&self, currency: &str) -> Option<String> {
        self.fx_config.get(currency).map(|(t, _)| t.clone())
    }
}
