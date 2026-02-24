use crate::models::{CashRecord, TradingRecord};
use anyhow::{Context, Result};
use std::io::Cursor;

pub enum FileType {
    Trading,
    Cash,
}

pub fn detect_file_type(filename: &str) -> FileType {
    let filename_upper = filename.to_uppercase();
    if filename_upper.contains("_CASH_") {
        FileType::Cash
    } else {
        FileType::Trading
    }
}

pub fn extract_account_type(filename: &str) -> String {
    let filename_upper = filename.to_uppercase();
    if filename_upper.starts_with("GIA_") || filename_upper.contains("_GIA_") {
        "GIA".to_string()
    } else if filename_upper.starts_with("ISA_") || filename_upper.contains("_ISA_") {
        "ISA".to_string()
    } else if filename_upper.contains("GIA") {
        "GIA".to_string()
    } else if filename_upper.contains("ISA") {
        "ISA".to_string()
    } else {
        "Unknown".to_string()
    }
}

pub fn merge_trading_files(file_data: Vec<(String, String)>) -> Result<Vec<TradingRecord>> {
    let mut all_records = Vec::new();

    for (filename, content) in file_data {
        let account_type = extract_account_type(&filename);
        
        // Skip first line (title)
        let mut lines = content.lines();
        lines.next(); // skip "Transaction Statement: ..."
        let remaining_content = lines.collect::<Vec<_>>().join("
");

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_reader(Cursor::new(remaining_content));

        for result in rdr.deserialize::<TradingRecord>() {
            let mut record: TradingRecord = result.with_context(|| format!("Failed to deserialize trading record in {}", filename))?;
            record.account_type = account_type.clone();
            all_records.push(record);
        }
    }

    // Sort by Trade Date/Time
    all_records.sort_by_key(|r| r.trade_date_time);

    Ok(all_records)
}

pub fn merge_cash_files(file_data: Vec<(String, String)>) -> Result<Vec<CashRecord>> {
    let mut all_records = Vec::new();

    for (filename, content) in file_data {
        let account_type = extract_account_type(&filename);
        let mut current_df_lines = Vec::new();
        let mut headers = None;
        let mut skip_section = false;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.starts_with("Cash Statement:") {
                // Process previous section if any
                if !current_df_lines.is_empty() && headers.is_some() {
                    let section_records = parse_cash_section(headers.take().unwrap(), &current_df_lines, &account_type)?;
                    all_records.extend(section_records);
                }
                current_df_lines.clear();

                if line.contains("Portfolio: Cash") {
                    skip_section = true;
                } else {
                    skip_section = false;
                }
                continue;
            }

            if skip_section {
                continue;
            }

            if headers.is_none() {
                if line.starts_with("Date,Activity") {
                    headers = Some(line.to_string());
                }
            } else {
                current_df_lines.push(line.to_string());
            }
        }

        // Process last section
        if !current_df_lines.is_empty() && headers.is_some() {
            let section_records = parse_cash_section(headers.unwrap(), &current_df_lines, &account_type)?;
            all_records.extend(section_records);
        }
    }

    // Sort by Date
    all_records.sort_by_key(|r| r.date);

    Ok(all_records)
}

fn parse_cash_section(headers: String, lines: &[String], account_type: &str) -> Result<Vec<CashRecord>> {
    let csv_content = format!("{}
{}", headers, lines.join("
"));
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(Cursor::new(csv_content));

    let mut records = Vec::new();
    for result in rdr.deserialize::<CashRecord>() {
        let mut record: CashRecord = result.context("Failed to deserialize cash record")?;
        record.account_type = account_type.to_string();
        
        // Calculate net_flow
        let credit = record.credit.unwrap_or_default();
        let debit = record.debit.unwrap_or_default();
        record.net_flow = credit - debit;

        // Filter to external cash flow activities (similar to extract_cash_flows_only in Python)
        let activity = record.activity.to_uppercase();
        if activity.contains("PAYMENT RECEIVED") || 
           activity.contains("WITHDRAWAL") || 
           activity.contains("ISA TRANSFER IN") {
            records.push(record);
        }
    }
    Ok(records)
}
