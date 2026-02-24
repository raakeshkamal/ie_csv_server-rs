use anyhow::Result;
use chrono::{NaiveDate, Utc, Duration};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::collections::{HashMap, HashSet};
use tracing::{info, error};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::database::Database;
use crate::prices::{PriceFetcher, CurrencyConverter};
use crate::portfolio_stats::calculate_portfolio_stats;

pub async fn precompute_portfolio_data(db_arc: Arc<Mutex<Database>>) -> Result<()> {
    // 1. Initial status - need to hold lock briefly to update status
    let status_id = {
        let db = db_arc.lock().await;
        db.update_precompute_status("in_progress", None, None)?
    };
    info!("Starting background precomputation (status_id: {})", status_id);

    // 2. Load basic data from DB
    let (trades, external_cfs) = {
        let db = db_arc.lock().await;
        (db.load_trades()?, db.get_external_cash_flows()?)
    };

    if trades.is_empty() {
        return Ok(());
    }

    // 3. Identify unique tickers and date range
    let mut tickers = HashSet::new();
    let mut min_date = trades[0].trade_date_time.date();
    let max_date = Utc::now().date_naive();

    for t in &trades {
        if let Some(ref ticker) = t.ticker {
            tickers.insert(ticker.clone());
        }
        if t.trade_date_time.date() < min_date {
            min_date = t.trade_date_time.date();
        }
    }

    for (cf_date, _) in &external_cfs {
        if *cf_date < min_date {
            min_date = *cf_date;
        }
    }

    // 4. Fetch Prices and FX asynchronously (outside DB lock)
    let price_fetcher = PriceFetcher::new();
    let currency_converter = CurrencyConverter::new();
    
    let mut raw_prices: HashMap<String, HashMap<NaiveDate, Decimal>> = HashMap::new();
    let mut ticker_currencies: HashMap<String, String> = HashMap::new();
    
    // Add FX tickers to fetch
    let mut currencies_needed = HashSet::new();
    let tickers_to_fetch: Vec<String> = tickers.iter().cloned().collect();

    for ticker in &tickers_to_fetch {
        info!("Fetching prices for {}", ticker);
        match price_fetcher.get_historical_prices(ticker, min_date - Duration::days(7), max_date).await {
            Ok(prices) => {
                info!("Fetched {} prices for {}", prices.len(), ticker);
                let mut p_map = HashMap::new();
                let mut detected_currency = "GBP".to_string();
                for (d, p, c) in prices {
                    p_map.insert(d, p);
                    detected_currency = c;
                }
                raw_prices.insert(ticker.clone(), p_map);
                ticker_currencies.insert(ticker.clone(), detected_currency.clone());
                
                // If it's not a GBP/GBp price, we need an FX rate
                if detected_currency != "GBP" && detected_currency != "GBp" {
                    if let Some(fx) = currency_converter.get_fx_ticker(&detected_currency) {
                        currencies_needed.insert(fx);
                    }
                }
            }
            Err(e) => {
                error!("Failed to fetch prices for {}: {}", ticker, e);
            }
        }
    }
    
    // Fetch any newly discovered FX tickers
    for fx in currencies_needed {
        if raw_prices.contains_key(&fx) { continue; }
        info!("Fetching FX rates for {}", fx);
        if let Ok(prices) = price_fetcher.get_historical_prices(&fx, min_date - Duration::days(7), max_date).await {
            let mut p_map = HashMap::new();
            for (d, p, _) in prices {
                p_map.insert(d, p);
            }
            raw_prices.insert(fx, p_map);
        }
    }

    // 5. Perform the heavy computation and DB updates
    // Since Database is not Sync, we do this in a single block while holding the lock
    
    let db_lock = db_arc.lock().await;
    
    // Clear old data
    db_lock.clear_precomputed_data()?;

    // Process each date and ticker
    let dates: Vec<NaiveDate> = min_date.iter_days().take_while(|&d| d <= max_date).collect();
    let mut daily_ticker_values: HashMap<String, Vec<Decimal>> = HashMap::new();
    let mut total_daily_values: Vec<Decimal> = Vec::new();

    for ticker in &tickers {
        daily_ticker_values.insert(ticker.clone(), vec![Decimal::ZERO; dates.len()]);
    }

    // Pre-calculate converted prices and save them
    let mut converted_prices: HashMap<String, HashMap<NaiveDate, Decimal>> = HashMap::new();
    for ticker in &tickers {
        let reported_currency = ticker_currencies.get(ticker).map(|s| s.as_str()).unwrap_or("GBP");
        let fx_ticker = currency_converter.get_fx_ticker(reported_currency);
        
        let mut ticker_conv = HashMap::new();
        for &date in &dates {
            let price = get_price_with_fallback(&raw_prices, ticker, date);
            let fx_rate = fx_ticker.as_ref().and_then(|fx| {
                let r = get_price_with_fallback(&raw_prices, fx, date);
                if r.is_zero() { None } else { Some(r) }
            });
            
            let converted = match currency_converter.convert_to_gbp(price, reported_currency, date, fx_rate).await {
                Ok(c) => c,
                Err(e) => {
                    if !price.is_zero() {
                        error!("Conversion failed for {} on {}: {}. Using raw price.", ticker, date, e);
                    }
                    price
                }
            };
            ticker_conv.insert(date, converted);
            
            if !price.is_zero() {
                db_lock.save_precomputed_ticker_price(ticker, date, reported_currency, price, converted)?;
            }
        }
        converted_prices.insert(ticker.clone(), ticker_conv);
    }

    // Simulate Holdings
    let mut sorted_trades = trades.clone();
    sorted_trades.sort_by_key(|t| t.trade_date_time);
    let mut current_holdings: HashMap<String, Decimal> = HashMap::new();
    let mut trade_idx = 0;

    for (d_idx, &date) in dates.iter().enumerate() {
        while trade_idx < sorted_trades.len() && sorted_trades[trade_idx].trade_date_time.date() <= date {
            let t = &sorted_trades[trade_idx];
            if let Some(ref ticker) = t.ticker {
                let entry = current_holdings.entry(ticker.clone()).or_insert(Decimal::ZERO);
                let quantity = t.quantity;
                let t_type = t.transaction_type.to_uppercase();
                if t_type.contains("BUY") || t_type.contains("DIVIDEND REINVESTMENT") {
                    *entry += quantity;
                } else if t_type.contains("SELL") {
                    *entry -= quantity;
                }
            }
            trade_idx += 1;
        }

        let mut total_val = Decimal::ZERO;
        for ticker in &tickers {
            let shares = *current_holdings.get(ticker).unwrap_or(&Decimal::ZERO);
            
            let price = converted_prices.get(ticker).and_then(|m| m.get(&date)).cloned().unwrap_or(Decimal::ZERO);
            let val = shares * price;
            daily_ticker_values.get_mut(ticker).unwrap()[d_idx] = val;
            total_val += val;
            
            // Save value for every ticker on every date to ensure vector alignment in API
            db_lock.save_precomputed_ticker_daily_value(date, ticker, val)?;
        }
        total_daily_values.push(total_val);
        db_lock.save_precomputed_portfolio_value(date, total_val)?;
    }

    // Monthly Contributions
    let mut monthly_net: HashMap<String, Decimal> = HashMap::new();
    for t in &trades {
        let month = t.trade_date_time.format("%Y-%m").to_string();
        let val = t.total_trade_value;
        let entry = monthly_net.entry(month).or_insert(Decimal::ZERO);
        let t_type = t.transaction_type.to_uppercase();
        if t_type.contains("BUY") { *entry += val; } else if t_type.contains("SELL") { *entry -= val; }
    }
    for (date, net_flow) in &external_cfs {
        let month = date.format("%Y-%m").to_string();
        *monthly_net.entry(month).or_insert(Decimal::ZERO) += *net_flow;
    }
    for (month, val) in monthly_net {
        db_lock.save_precomputed_monthly_contribution(&month, val)?;
    }

    // Stats
    let current_value = *total_daily_values.last().unwrap_or(&Decimal::ZERO);
    let mut stats_cfs = Vec::new();
    for (d, f) in external_cfs {
        stats_cfs.push((d, f, "External".to_string()));
    }
    let stats = calculate_portfolio_stats(&stats_cfs, current_value, max_date, Some((&dates, &total_daily_values)));

    db_lock.save_precomputed_metrics(
        Decimal::from_f64(stats.irr).unwrap_or_default(),
        Decimal::from_f64(stats.twr).unwrap_or_default(),
        stats.total_invested,
        stats.current_value,
        stats.profit_loss,
        stats.return_percentage,
        &max_date.to_string()
    )?;

    db_lock.update_precompute_status("completed", None, None)?;
    info!("Precomputation completed successfully");

    Ok(())
}

fn get_price_with_fallback(raw_prices: &HashMap<String, HashMap<NaiveDate, Decimal>>, ticker: &str, date: NaiveDate) -> Decimal {
    if let Some(p_map) = raw_prices.get(ticker) {
        // 1. Try looking backwards (Standard "Last Known Price")
        let mut check_date = date;
        for _ in 0..90 {
            if let Some(&p) = p_map.get(&check_date) {
                if !p.is_zero() { return p; }
            }
            if let Some(prev) = check_date.pred_opt() {
                check_date = prev;
            } else {
                break;
            }
        }

        // 2. If no backward price found, look forward (Handle "Start of History" gap)
        let mut check_date = date;
        for _ in 0..30 {
            if let Some(&p) = p_map.get(&check_date) {
                if !p.is_zero() { return p; }
            }
            if let Some(next) = check_date.succ_opt() {
                check_date = next;
            } else {
                break;
            }
        }
    }
    Decimal::ZERO
}
