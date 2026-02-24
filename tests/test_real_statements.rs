use investengine_csv_server_rs::merge_csv::{merge_trading_files, merge_cash_files};
use investengine_csv_server_rs::database::Database;
use std::fs;
use std::path::PathBuf;

#[test]
fn test_merge_real_trading_statements() {
    let mut file_data = Vec::new();
    let paths = [
        "statements/GIA_Trading_statement_5_Jun_2022_to_21_Feb_2026.csv",
        "statements/ISA_Trading_statement_5_Jun_2022_to_21_Feb_2026.csv",
    ];

    for path in paths {
        let full_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
        if full_path.exists() {
            let content = fs::read_to_string(&full_path).expect(&format!("Failed to read {}", path));
            let filename = full_path.file_name().unwrap().to_str().unwrap().to_string();
            file_data.push((filename, content));
        }
    }

    if file_data.is_empty() {
        panic!("No trading statement files found in statements/ directory");
    }

    let result = merge_trading_files(file_data);
    assert!(result.is_ok(), "Failed to merge real trading files: {:?}", result.err());
    
    let records = result.unwrap();
    assert!(!records.is_empty(), "Should have at least some trading records");
    
    // Check if sorted by date
    for i in 0..records.len() - 1 {
        assert!(records[i].trade_date_time <= records[i+1].trade_date_time, 
            "Records not sorted by date at index {}", i);
    }

    // Test database saving
    let db = Database::new(":memory:").expect("Failed to create in-memory database");
    db.save_trades(&records).expect("Failed to save trades to DB");
    let loaded = db.load_trades().expect("Failed to load trades from DB");
    assert_eq!(records.len(), loaded.len());

    println!("Successfully merged and saved {} trading records", records.len());
}

#[test]
fn test_merge_real_cash_statements() {
    let mut file_data = Vec::new();
    let paths = [
        "statements/GIA_Cash_statement_5_Jun_2022_to_21_Feb_2026.csv",
        "statements/ISA_Cash_statement_5_Jun_2022_to_21_Feb_2026.csv",
    ];

    for path in paths {
        let full_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
        if full_path.exists() {
            let content = fs::read_to_string(&full_path).expect(&format!("Failed to read {}", path));
            let filename = full_path.file_name().unwrap().to_str().unwrap().to_string();
            file_data.push((filename, content));
        }
    }

    if file_data.is_empty() {
        panic!("No cash statement files found in statements/ directory");
    }

    let result = merge_cash_files(file_data);
    assert!(result.is_ok(), "Failed to merge real cash files: {:?}", result.err());
    
    let records = result.unwrap();
    assert!(!records.is_empty(), "Should have at least some cash records");

    // Check if sorted by date
    for i in 0..records.len() - 1 {
        assert!(records[i].date <= records[i+1].date, 
            "Records not sorted by date at index {}", i);
    }

    // Verify net_flow calculation
    for record in &records {
        let expected_net = record.credit.unwrap_or_default() - record.debit.unwrap_or_default();
        assert_eq!(record.net_flow, expected_net, "net_flow mismatch for activity: {}", record.activity);
    }

    // Test database saving
    let db = Database::new(":memory:").expect("Failed to create in-memory database");
    db.save_cash_flows(&records).expect("Failed to save cash flows to DB");
    let loaded = db.load_cash_flows().expect("Failed to load cash flows from DB");
    assert_eq!(records.len(), loaded.len());

    println!("Successfully merged and saved {} cash records", records.len());
}
