use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;

pub struct PortfolioStats {
    pub irr: f64,
    pub twr: f64,
    pub total_invested: Decimal,
    pub total_withdrawn: Decimal,
    pub current_value: Decimal,
    pub profit_loss: Decimal,
    pub return_percentage: Decimal,
    pub calc_date: NaiveDate,
}

pub fn calculate_xirr(dates: &[NaiveDate], amounts: &[f64], guess: f64) -> f64 {
    if dates.len() != amounts.len() || dates.len() < 2 {
        return 0.0;
    }

    // Sort by date
    let mut data: Vec<(NaiveDate, f64)> = dates.iter().cloned().zip(amounts.iter().cloned()).collect();
    data.sort_by_key(|x| x.0);

    let start_date = data[0].0;
    let years: Vec<f64> = data.iter().map(|(d, _)| (*d - start_date).num_days() as f64 / 365.25).collect();
    let amounts_vec: Vec<f64> = data.iter().map(|(_, a)| *a).collect();

    // Check validity: must have at least one positive and one negative value
    let mut has_pos = false;
    let mut has_neg = false;
    for &a in &amounts_vec {
        if a > 0.0 { has_pos = true; }
        if a < 0.0 { has_neg = true; }
    }
    if !has_pos || !has_neg {
        return 0.0;
    }

    let mut rate = guess;
    let max_iter = 100;
    let tol = 1e-6;

    for _ in 0..max_iter {
        if rate <= -1.0 {
            rate = -0.99;
        }

        let mut f_val = 0.0;
        let mut df_val = 0.0;
        let base = 1.0 + rate;

        for i in 0..amounts_vec.len() {
            let factor = base.powf(-years[i]);
            f_val += amounts_vec[i] * factor;
            df_val += amounts_vec[i] * -years[i] * factor / base;
        }

        if f_val.abs() < tol {
            return rate;
        }

        if df_val.abs() < 1e-9 {
            break;
        }

        let new_rate = rate - f_val / df_val;
        if (new_rate - rate).abs() < tol {
            return new_rate;
        }
        rate = new_rate;
    }

    rate
}

pub fn calculate_twr(
    daily_dates: &[NaiveDate],
    daily_values: &[Decimal],
    cash_flow_events: &[(NaiveDate, Decimal)],
    current_date: NaiveDate,
) -> f64 {
    if daily_dates.is_empty() || daily_values.is_empty() || daily_dates.len() != daily_values.len() {
        return 0.0;
    }

    let date_to_value: HashMap<NaiveDate, Decimal> = daily_dates.iter().cloned().zip(daily_values.iter().cloned()).collect();
    let mut sorted_dates: Vec<NaiveDate> = daily_dates.to_vec();
    sorted_dates.sort();

    if sorted_dates.len() < 2 {
        return 0.0;
    }

    let start_date = sorted_dates[0];
    let mut cash_flows_by_date: HashMap<NaiveDate, Decimal> = HashMap::new();
    for (date, amount) in cash_flow_events {
        *cash_flows_by_date.entry(*date).or_insert(Decimal::ZERO) += *amount;
    }

    let mut period_dates: Vec<NaiveDate> = cash_flows_by_date.keys().cloned().collect();
    period_dates.push(start_date);
    period_dates.push(current_date);
    period_dates.sort();
    period_dates.dedup();
    
    // Filter to dates we actually have values for
    let period_dates: Vec<NaiveDate> = period_dates.into_iter().filter(|d| date_to_value.contains_key(d)).collect();

    if period_dates.len() < 2 {
        return 0.0;
    }

    let mut twr = 1.0;
    for i in 0..period_dates.len() - 1 {
        let period_start = period_dates[i];
        let period_end = period_dates[i + 1];

        let start_val = date_to_value[&period_start];
        let end_val = date_to_value[&period_end];

        if start_val.is_zero() {
            continue;
        }

        let cash_flow_at_end = cash_flows_by_date.get(&period_end).cloned().unwrap_or(Decimal::ZERO);
        let end_val_before_cf = end_val - cash_flow_at_end;
        
        let period_return = (end_val_before_cf / start_val).to_f64().unwrap() - 1.0;
        twr *= 1.0 + period_return;
    }

    twr -= 1.0;
    let days = (current_date - start_date).num_days() as f64;
    if days <= 0.0 || (1.0 + twr) <= 0.0 {
        return 0.0;
    }

    (1.0 + twr).powf(365.25 / days) - 1.0
}

pub fn calculate_portfolio_stats(
    cash_flow_events: &[(NaiveDate, Decimal, String)], // date, net_flow, activity
    current_value: Decimal,
    current_date: NaiveDate,
    daily_portfolio_values: Option<(&[NaiveDate], &[Decimal])>,
) -> PortfolioStats {
    let mut total_invested = Decimal::ZERO;
    let mut total_withdrawn = Decimal::ZERO;
    let mut xirr_dates = Vec::new();
    let mut xirr_amounts = Vec::new();
    let mut twr_events = Vec::new();

    for (date, net_flow, _activity) in cash_flow_events {
        if *net_flow > Decimal::ZERO {
            total_invested += *net_flow;
        } else {
            total_withdrawn += net_flow.abs();
        }

        xirr_dates.push(*date);
        xirr_amounts.push(-net_flow.to_f64().unwrap());
        twr_events.push((*date, *net_flow));
    }

    xirr_dates.push(current_date);
    xirr_amounts.push(current_value.to_f64().unwrap());

    let irr = calculate_xirr(&xirr_dates, &xirr_amounts, 0.1);
    
    let mut twr = 0.0;
    if let Some((daily_dates, daily_values)) = daily_portfolio_values {
        twr = calculate_twr(daily_dates, daily_values, &twr_events, current_date);
    }

    let profit_loss = current_value + total_withdrawn - total_invested;
    let return_percentage = if total_invested.is_zero() {
        Decimal::ZERO
    } else {
        profit_loss / total_invested
    };

    PortfolioStats {
        irr,
        twr,
        total_invested,
        total_withdrawn,
        current_value,
        profit_loss,
        return_percentage,
        calc_date: current_date,
    }
}
