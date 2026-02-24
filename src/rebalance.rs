use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize)]
pub struct RebalanceInvestment {
    pub ticker: String,
    #[serde(rename = "current_value")]
    pub current_value: f64,
    #[serde(rename = "target_value")]
    pub target_value: f64,
    #[serde(rename = "investment_amount")]
    pub investment_amount: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RebalanceSummary {
    #[serde(rename = "total_current")]
    pub total_current: f64,
    #[serde(rename = "new_total")]
    pub new_total: f64,
    #[serde(rename = "total_investment")]
    pub total_investment: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RebalanceResult {
    pub investments: Vec<RebalanceInvestment>,
    pub summary: RebalanceSummary,
}

pub fn calculate_rebalancing(
    new_capital: Decimal,
    current_values: &HashMap<String, Decimal>,
    target_allocations: &HashMap<String, Decimal>,
) -> anyhow::Result<RebalanceResult> {
    // ... logic remains same ...
    let current_keys: HashSet<_> = current_values.keys().collect();
    let target_keys: HashSet<_> = target_allocations.keys().collect();
    let common_tickers: Vec<_> = current_keys.intersection(&target_keys).collect();

    if common_tickers.is_empty() {
        return Err(anyhow::anyhow!("No common tickers between current portfolio and target allocations"));
    }

    // Normalize target allocations to sum to 100%
    let total_target_pct: Decimal = common_tickers.iter()
        .map(|&&t| target_allocations.get(t).copied().unwrap_or(Decimal::ZERO))
        .sum();

    if total_target_pct.is_zero() {
        return Err(anyhow::anyhow!("Target allocations sum to zero"));
    }

    let normalized_targets: HashMap<String, Decimal> = common_tickers.iter()
        .map(|&&t| {
            let pct = target_allocations.get(t).copied().unwrap_or(Decimal::ZERO);
            let normalized = (pct / total_target_pct) * Decimal::from(100);
            (t.clone(), normalized)
        })
        .collect();

    // Current total value of the common tickers
    let total_current: Decimal = common_tickers.iter()
        .map(|&&t| current_values.get(t).copied().unwrap_or(Decimal::ZERO))
        .sum();

    // New total portfolio value
    let new_total = total_current + new_capital;

    // Calculate target values and investments
    let mut investments = Vec::new();
    let mut total_investment = Decimal::ZERO;

    for &ticker in common_tickers {
        let current_val = current_values.get(ticker).copied().unwrap_or(Decimal::ZERO);
        let target_pct = normalized_targets.get(ticker).copied().unwrap_or(Decimal::ZERO);
        
        let target_val = new_total * (target_pct / Decimal::from(100));
        let investment = (target_val - current_val).max(Decimal::ZERO);

        investments.push(RebalanceInvestment {
            ticker: ticker.clone(),
            current_value: current_val.round_dp(2).to_f64().unwrap_or(0.0),
            target_value: target_val.round_dp(2).to_f64().unwrap_or(0.0),
            investment_amount: investment.round_dp(2).to_f64().unwrap_or(0.0),
        });
        total_investment += investment;
    }

    // Summary
    let summary = RebalanceSummary {
        total_current: total_current.round_dp(2).to_f64().unwrap_or(0.0),
        new_total: new_total.round_dp(2).to_f64().unwrap_or(0.0),
        total_investment: total_investment.round_dp(2).to_f64().unwrap_or(0.0),
    };

    Ok(RebalanceResult {
        investments,
        summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_calculate_rebalancing() {
        let mut current_values = HashMap::new();
        current_values.insert("VWRP.L".to_string(), dec!(1000.0));
        current_values.insert("VUSA.L".to_string(), dec!(500.0));

        let mut target_allocations = HashMap::new();
        target_allocations.insert("VWRP.L".to_string(), dec!(50.0));
        target_allocations.insert("VUSA.L".to_string(), dec!(50.0));

        let new_capital = dec!(500.0);
        let result = calculate_rebalancing(new_capital, &current_values, &target_allocations).unwrap();

        assert_eq!(result.summary.total_current, 1500.0);
        assert_eq!(result.summary.new_total, 2000.0);
        assert_eq!(result.summary.total_investment, 500.0);

        let vusa = result.investments.iter().find(|i| i.ticker == "VUSA.L").unwrap();
        assert_eq!(vusa.investment_amount, 500.0);
        assert_eq!(vusa.target_value, 1000.0);
    }

    #[test]
    fn test_calculate_rebalancing_normalization() {
        let mut current_values = HashMap::new();
        current_values.insert("VWRP.L".to_string(), dec!(1000.0));

        let mut target_allocations = HashMap::new();
        target_allocations.insert("VWRP.L".to_string(), dec!(1.0));

        let new_capital = dec!(500.0);
        let result = calculate_rebalancing(new_capital, &current_values, &target_allocations).unwrap();

        assert_eq!(result.summary.new_total, 1500.0);
        let vwrp = result.investments.iter().find(|i| i.ticker == "VWRP.L").unwrap();
        assert_eq!(vwrp.investment_amount, 500.0);
    }
}
