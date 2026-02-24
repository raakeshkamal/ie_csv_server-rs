use anyhow::Result;
use yfinance_rs::{YfClient, search};

pub async fn search_ticker_for_isin(security_name: &str, isin: &str) -> Result<Option<String>> {
    let client = YfClient::default();

    // Primary: Search with security name
    if let Some(symbol) = perform_search(&client, security_name, isin).await? {
        return Ok(Some(symbol));
    }

    // Fallback: Search with ISIN
    if let Some(symbol) = perform_search(&client, isin, isin).await? {
        return Ok(Some(symbol));
    }

    Ok(None)
}

async fn perform_search(client: &YfClient, query: &str, isin: &str) -> Result<Option<String>> {
    let response = search(client, query).await?;
    
    // Prefer LSE exchange or .L suffix, and ETF/Equity types
    // SearchResult from paft has: symbol, name, exchange, kind
    
    let lse_candidates: Vec<_> = response.results.iter()
        .filter(|r| {
            let sym_str = r.symbol.to_string();
            let exch_str = r.exchange.as_ref().map(|e| e.to_string()).unwrap_or_default();
            
            (exch_str.contains("LSE") || sym_str.ends_with(".L")) && 
            (format!("{:?}", r.kind) == "Etf" || format!("{:?}", r.kind) == "Equity")
        })
        .collect();

    if !lse_candidates.is_empty() {
        let symbol = lse_candidates[0].symbol.to_string();
        if symbol != isin {
            return Ok(Some(symbol));
        }
    }

    // Fallback to any valid quote if no LSE candidate
    for r in response.results {
        let symbol = r.symbol.to_string();
        let kind_str = format!("{:?}", r.kind);
        if symbol != isin && (kind_str == "Etf" || kind_str == "Equity" || kind_str == "MutualFund" || kind_str == "Currency") {
            return Ok(Some(symbol));
        }
    }

    Ok(None)
}
