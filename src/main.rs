use axum::{
    extract::{Multipart, State, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, delete},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error};
use tracing_subscriber;
use investengine_csv_server_rs::database::Database;
use investengine_csv_server_rs::merge_csv::{detect_file_type, FileType, merge_trading_files, merge_cash_files};
use investengine_csv_server_rs::security_parser::extract_security_and_isin;
use investengine_csv_server_rs::tickers::search_ticker_for_isin;
use investengine_csv_server_rs::background_processor::precompute_portfolio_data;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use askama::Template;
use axum::response::Html;

use tower_http::trace::TraceLayer;

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {}

#[derive(Template)]
#[template(path = "upload.html")]
struct UploadTemplate {}

#[derive(Template)]
#[template(path = "mappings.html")]
struct MappingsTemplate {}

#[derive(Template)]
#[template(path = "rebalance.html")]
struct RebalanceTemplate {}

struct AppState {
    db: Arc<Mutex<Database>>,
}

async fn index_handler() -> impl IntoResponse {
    match (IndexTemplate {}).render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Template error: {}", e)).into_response(),
    }
}

async fn upload_page_handler() -> impl IntoResponse {
    match (UploadTemplate {}).render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Template error: {}", e)).into_response(),
    }
}

async fn mappings_page_handler() -> impl IntoResponse {
    match (MappingsTemplate {}).render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Template error: {}", e)).into_response(),
    }
}

async fn rebalance_page_handler() -> impl IntoResponse {
    match (RebalanceTemplate {}).render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Template error: {}", e)).into_response(),
    }
}

use investengine_csv_server_rs::rebalance::calculate_rebalancing;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_path = std::env::var("CSV_DATABASE_URL")
        .unwrap_or_else(|_| "/app/data/investengine.db".to_string());
    let db = Database::new(&db_path).expect("Failed to initialize database");
    let shared_state = Arc::new(AppState { db: Arc::new(Mutex::new(db)) });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/upload/", get(upload_page_handler).post(upload_files_handler))
        .route("/mappings/", get(mappings_page_handler))
        .route("/rebalance/", get(rebalance_page_handler))
        .route("/reset/", post(reset_database_handler))
        .route("/mapping/", get(get_mappings_handler).post(create_mapping_handler))
        .route("/mapping/missing/", get(get_missing_mappings_handler))
        .route("/mapping/{isin}/", delete(delete_mapping_handler))
        .route("/export/prices/", get(export_prices_handler))
        .route("/export/trades/", get(export_trades_handler))
        .route("/portfolio-values/", get(get_portfolio_values_handler))
        .route("/rebalance/data/", get(get_rebalance_data_handler))
        .route("/rebalance/calculate/", post(calculate_rebalance_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(shared_state);

    let port = std::env::var("CSV_SERVER_PORT")
        .unwrap_or_else(|_| "8000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

#[derive(Serialize)]
struct RebalanceDataTicker {
    ticker: String,
    #[serde(rename = "current_value")]
    current_value: f64,
    #[serde(rename = "current_allocation_pct")]
    current_allocation_pct: f64,
}

#[derive(Serialize)]
struct RebalanceDataResponse {
    success: bool,
    tickers: Vec<RebalanceDataTicker>,
    #[serde(rename = "total_value")]
    total_value: f64,
}

async fn get_rebalance_data_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;

    // 1. Validate mappings
    match db.get_isins_without_mappings() {
        Ok(missing) if !missing.is_empty() => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "success": false,
                "error": "Missing ticker mappings",
                "missing_isins": missing
            }))).into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "success": false,
                "error": e.to_string()
            }))).into_response();
        }
        _ => {}
    }

    // 2. Get precomputed data
    let portfolio_data = match db.get_portfolio_values_precomputed() {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({
                "success": false,
                "error": "No precomputed data. Please wait for processing."
            }))).into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "success": false,
                "error": e.to_string()
            }))).into_response();
        }
    };

    // 3. Extract last values
    let mut last_values = HashMap::new();
    if let Some(daily_ticker_values) = portfolio_data.get("daily_ticker_values").and_then(|v| v.as_object()) {
        for (ticker, values) in daily_ticker_values {
            if let Some(val_arr) = values.as_array() {
                let last_val_f64 = val_arr.last()
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                
                let last_val = Decimal::from_f64(last_val_f64).unwrap_or(Decimal::ZERO);
                last_values.insert(ticker.clone(), last_val);
            }
        }
    }

    // 4. Filter to invested tickers (value > 0 after rounding)
    let invested_tickers: Vec<RebalanceDataTicker> = {
        let mut invested = Vec::new();
        let total_value: Decimal = last_values.values().sum();
        
        if !total_value.is_zero() {
            for (ticker, value) in last_values {
                if value.round_dp(2) > Decimal::ZERO {
                    let pct = (value / total_value) * Decimal::from(100);
                    invested.push(RebalanceDataTicker {
                        ticker,
                        current_value: value.round_dp(2).to_f64().unwrap_or(0.0),
                        current_allocation_pct: pct.round_dp(2).to_f64().unwrap_or(0.0),
                    });
                }
            }
        }
        invested
    };

    let total_value: f64 = invested_tickers.iter().map(|t| t.current_value).sum();

    Json(RebalanceDataResponse {
        success: true,
        tickers: invested_tickers,
        total_value,
    }).into_response()
}

#[derive(Deserialize)]
struct CalculateRebalanceRequest {
    #[serde(rename = "new_capital")]
    new_capital: Decimal,
    #[serde(rename = "target_allocations")]
    target_allocations: HashMap<String, Decimal>,
    #[serde(rename = "current_tickers")]
    current_tickers: Vec<serde_json::Value>, // {ticker, current_value}
}

async fn calculate_rebalance_handler(
    Json(req): Json<CalculateRebalanceRequest>,
) -> impl IntoResponse {
    let mut current_values = HashMap::new();
    for item in req.current_tickers {
        if let (Some(ticker), Some(val)) = (
            item.get("ticker").and_then(|v| v.as_str()),
            item.get("current_value").and_then(|v| {
                if v.is_string() {
                    v.as_str().and_then(|s| Decimal::from_str(s).ok())
                } else {
                    v.as_f64().and_then(|f| Decimal::from_f64(f))
                }
            })
        ) {
            current_values.insert(ticker.to_string(), val);
        }
    }

    if req.new_capital < Decimal::ZERO {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "success": false,
            "error": "New capital must be non-negative"
        }))).into_response();
    }

    match calculate_rebalancing(req.new_capital, &current_values, &req.target_allocations) {
        Ok(result) => {
            Json(serde_json::json!({
                "success": true,
                "investments": result.investments,
                "summary": result.summary
            })).into_response()
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "success": false,
                "error": e.to_string()
            }))).into_response()
        }
    }
}


#[derive(Serialize)]
struct GenericResponse {
    success: bool,
    message: String,
}

#[derive(Serialize)]
struct TradesResponse {
    success: bool,
    trades: Vec<investengine_csv_server_rs::models::TradingRecord>,
}

async fn export_trades_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.load_trades() {
        Ok(trades) => {
            if trades.is_empty() {
                return (StatusCode::NOT_FOUND, Json(serde_json::json!({
                    "success": false,
                    "error": "No trades data in database"
                }))).into_response();
            }
            Json(TradesResponse {
                success: true,
                trades,
            }).into_response()
        }
        Err(e) => {
            error!("Error exporting trades: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "success": false,
                "error": format!("Error exporting trades: {}", e)
            }))).into_response()
        }
    }
}

async fn get_portfolio_values_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;

    // 1. Validate that all ISINs have ticker mappings
    match db.get_isins_without_mappings() {
        Ok(missing) if !missing.is_empty() => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
                "success": false,
                "error": "Cannot calculate portfolio: missing ticker mappings for ISINs",
                "missing_isins": missing
            }))).into_response();
        }
        Err(e) => {
            error!("Error checking mappings: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to check mappings: {}", e),
            })).into_response();
        }
        _ => {}
    }

    // 2. Try to get precomputed data first
    let mut data = match db.get_portfolio_values_precomputed() {
        Ok(Some(d)) => d,
        Ok(None) => {
            // No precomputed data yet
            // Check if there are even trades
            match db.has_trades_data() {
                Ok(true) => {
                    // Trades exist, but no precomputed data. Trigger it and return error/in_progress
                    info!("No precomputed data but trades exist. Triggering precomputation...");
                    let db_arc = Arc::clone(&state.db);
                    tokio::spawn(async move {
                        if let Err(e) = precompute_portfolio_data(db_arc).await {
                            error!("Background precomputation failed: {}", e);
                        }
                    });

                    return (StatusCode::ACCEPTED, Json(serde_json::json!({
                        "success": true,
                        "data_extended": true,
                        "extension_in_progress": true,
                        "message": "Precomputation started. Please wait a moment."
                    }))).into_response();
                }
                _ => {
                    return (StatusCode::NOT_FOUND, Json(serde_json::json!({
                        "success": false,
                        "error": "No trades data in database. Please upload files first."
                    }))).into_response();
                }
            }
        }
        Err(e) => {
            error!("Error retrieving portfolio values: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to retrieve portfolio values: {}", e),
            })).into_response();
        }
    };

    // 3. Check if precomputed data is up to date
    let status = match db.get_precompute_status() {
        Ok(s) => s,
        Err(_) => serde_json::json!({}),
    };

    let last_updated_str = status.get("completed_at")
        .or_else(|| status.get("started_at"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let today = chrono::Utc::now().date_naive();
    let is_up_to_date = if !last_updated_str.is_empty() {
        last_updated_str.contains(&today.to_string())
    } else {
        false
    };

    if !is_up_to_date && status.get("status").and_then(|s| s.as_str()) != Some("in_progress") {
        info!("Portfolio data not up to date, triggering background precomputation...");
        let db_arc = Arc::clone(&state.db);
        tokio::spawn(async move {
            if let Err(e) = precompute_portfolio_data(db_arc).await {
                error!("Background precomputation failed: {}", e);
            }
        });

        if let Some(obj) = data.as_object_mut() {
            obj.insert("data_extended".to_string(), serde_json::json!(true));
            obj.insert("extension_in_progress".to_string(), serde_json::json!(true));
            obj.insert("last_data_date".to_string(), serde_json::json!(last_updated_str));
        }
    } else {
        if let Some(obj) = data.as_object_mut() {
            obj.insert("data_extended".to_string(), serde_json::json!(false));
        }
    }

    if let Some(obj) = data.as_object_mut() {
        obj.insert("success".to_string(), serde_json::json!(true));
    }

    Json(data).into_response()
}

async fn reset_database_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.reset() {
        Ok(_) => {
            info!("Database reset successfully");
            (StatusCode::OK, Json(GenericResponse {
                success: true,
                message: "Database reset successfully. You can now upload new files.".to_string(),
            }))
        }
        Err(e) => {
            error!("Error resetting database: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to reset database: {}", e),
            }))
        }
    }
}

#[derive(Serialize)]
struct MappingsResponse {
    success: bool,
    mappings: Vec<serde_json::Value>,
    count: usize,
}

#[derive(Serialize)]
struct MissingMappingsResponse {
    success: bool,
    missing_isins: Vec<String>,
    count: usize,
}

#[derive(Deserialize)]
struct MappingUpdate {
    isin: String,
    ticker: String,
    security_name: Option<String>,
}

#[derive(Serialize)]
struct MappingResult {
    success: bool,
    isin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ticker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn get_mappings_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.get_all_isin_ticker_mappings() {
        Ok(mappings) => {
            let count = mappings.len();
            Json(MappingsResponse {
                success: true,
                mappings,
                count,
            }).into_response()
        }
        Err(e) => {
            error!("Error retrieving mappings: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to retrieve mappings: {}", e),
            })).into_response()
        }
    }
}

async fn create_mapping_handler(
    State(state): State<Arc<AppState>>,
    Json(updates): Json<Vec<MappingUpdate>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    let mut results = Vec::new();
    let isin_regex = regex::Regex::new(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$").unwrap();
    
    for update in updates {
        let isin = update.isin.clone();
        
        // Validate ISIN format
        if !isin_regex.is_match(&isin) {
            results.push(MappingResult {
                success: false,
                isin: isin,
                ticker: None,
                message: None,
                error: Some("Invalid ISIN format".to_string()),
            });
            continue;
        }

        match db.save_isin_ticker_mapping(&update.isin, &update.ticker, update.security_name.as_deref()) {
            Ok(_) => {
                results.push(MappingResult {
                    success: true,
                    isin: isin,
                    ticker: Some(update.ticker),
                    message: Some("Mapping created/updated successfully".to_string()),
                    error: None,
                });
            }
            Err(e) => {
                error!("Error saving mapping for {}: {}", update.isin, e);
                results.push(MappingResult {
                    success: false,
                    isin: isin,
                    ticker: None,
                    message: None,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    Json(results).into_response()
}

async fn get_missing_mappings_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.get_isins_without_mappings() {
        Ok(missing_isins) => {
            let count = missing_isins.len();
            Json(MissingMappingsResponse {
                success: true,
                missing_isins,
                count,
            }).into_response()
        }
        Err(e) => {
            error!("Error retrieving missing mappings: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to retrieve missing mappings: {}", e),
            })).into_response()
        }
    }
}

async fn delete_mapping_handler(
    State(state): State<Arc<AppState>>,
    Path(isin): Path<String>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.delete_isin_ticker_mapping(&isin) {
        Ok(true) => {
            Json(GenericResponse {
                success: true,
                message: format!("Mapping for {} deleted", isin),
            }).into_response()
        }
        Ok(false) => {
            (StatusCode::NOT_FOUND, Json(GenericResponse {
                success: false,
                message: format!("No mapping found for {}", isin),
            })).into_response()
        }
        Err(e) => {
            error!("Error deleting mapping for {}: {}", isin, e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to delete mapping: {}", e),
            })).into_response()
        }
    }
}

async fn export_prices_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    
    // 1. Get current precomputed data
    let mut data = match db.get_all_precomputed_data() {
        Ok(d) => d,
        Err(e) => {
            error!("Error retrieving precomputed data: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(GenericResponse {
                success: false,
                message: format!("Failed to retrieve precomputed data: {}", e),
            })).into_response();
        }
    };

    // 2. Check if data is up to date
    let status = data.get("status").cloned().unwrap_or(serde_json::json!({}));
    let last_updated_str = status.get("completed_at")
        .or_else(|| status.get("started_at"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let today = chrono::Utc::now().date_naive();
    let is_up_to_date = if !last_updated_str.is_empty() {
        // Simple check: if it was updated today, we consider it up to date
        // In a more robust implementation, we'd check the max(date) in ticker_prices
        last_updated_str.contains(&today.to_string())
    } else {
        false
    };

    if !is_up_to_date && status.get("status").and_then(|s| s.as_str()) != Some("in_progress") {
        info!("Data not up to date, triggering background precomputation...");
        let db_arc = Arc::clone(&state.db);
        tokio::spawn(async move {
            if let Err(e) = precompute_portfolio_data(db_arc).await {
                error!("Background precomputation failed: {}", e);
            }
        });

        // Add extra info to response
        if let Some(obj) = data.as_object_mut() {
            obj.insert("data_extended".to_string(), serde_json::json!(true));
            obj.insert("extension_in_progress".to_string(), serde_json::json!(true));
        }
    } else {
        if let Some(obj) = data.as_object_mut() {
            obj.insert("data_extended".to_string(), serde_json::json!(false));
        }
    }

    if let Some(obj) = data.as_object_mut() {
        obj.insert("success".to_string(), serde_json::json!(true));
    }

    Json(data).into_response()
}

#[derive(Serialize)]
struct UploadResponse {
    success: bool,
    message: String,
    total_trading_transactions: usize,
    total_cash_flows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing_isins: Option<Vec<String>>,
}

async fn upload_files_handler(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    info!("Endpoint /upload/ called");

    let db = state.db.lock().await;

    // Check if database has existing data
    match db.has_trades_data() {
        Ok(true) => {
            return (StatusCode::BAD_REQUEST, Json(UploadResponse {
                success: false,
                message: "Database contains existing data. Please call /reset/ first.".to_string(),
                total_trading_transactions: 0,
                total_cash_flows: 0,
                missing_isins: None,
            })).into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse {
                success: false,
                message: format!("Database error: {}", e),
                total_trading_transactions: 0,
                total_cash_flows: 0,
                missing_isins: None,
            })).into_response();
        }
        _ => {}
    }

    let mut trading_files = Vec::new();
    let mut cash_files = Vec::new();

    while let Ok(Some(field)) = multipart.next_field().await {
        let filename = field.file_name().unwrap_or_default().to_string();
        
        if filename.is_empty() || !filename.ends_with(".csv") {
            continue;
        }

        let data = field.bytes().await.unwrap_or_default();
        let content = String::from_utf8_lossy(&data).to_string();

        match detect_file_type(&filename) {
            FileType::Trading => trading_files.push((filename, content)),
            FileType::Cash => cash_files.push((filename, content)),
        }
    }

    if trading_files.is_empty() && cash_files.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(UploadResponse {
            success: false,
            message: "No valid CSV files uploaded".to_string(),
            total_trading_transactions: 0,
            total_cash_flows: 0,
            missing_isins: None,
        })).into_response();
    }

    let mut all_trading_records = Vec::new();
    let mut all_cash_records = Vec::new();

    // Process trading files
    if !trading_files.is_empty() {
        match merge_trading_files(trading_files) {
            Ok(records) => {
                let mut missing_isins = Vec::new();
                let mut processed_records = records;

                // 1. Normalize ISINs first
                for record in &mut processed_records {
                    let (_name, isin_opt) = extract_security_and_isin(&record.security_isin);
                    record.security_isin = isin_opt.unwrap_or_default();
                }

                // 2. Identify unique ISINs that need mapping
                let unique_isins: std::collections::HashSet<String> = processed_records.iter()
                    .map(|r| r.security_isin.clone())
                    .filter(|s| !s.is_empty())
                    .collect();

                // 3. Check existing mappings and search for missing ones once per ISIN
                let mut mapping_cache = std::collections::HashMap::new();
                for isin in unique_isins {
                    match db.get_ticker_for_isin(&isin) {
                        Ok(Some(ticker)) => {
                            mapping_cache.insert(isin, Some(ticker));
                        }
                        Ok(None) => {
                            info!("Searching ticker for ISIN: {}", isin);
                            match search_ticker_for_isin("", &isin).await {
                                Ok(Some(ticker)) => {
                                    db.save_isin_ticker_mapping(&isin, &ticker, None).unwrap_or_default();
                                    mapping_cache.insert(isin, Some(ticker));
                                }
                                _ => {
                                    mapping_cache.insert(isin.clone(), None);
                                    missing_isins.push(isin);
                                }
                            }
                        }
                        Err(_) => {
                            mapping_cache.insert(isin, None);
                        }
                    }
                }

                // 4. Assign tickers to records
                for record in &mut processed_records {
                    if let Some(Some(ticker)) = mapping_cache.get(&record.security_isin) {
                        record.ticker = Some(ticker.clone());
                    }
                }

                if !missing_isins.is_empty() {
                    return (StatusCode::BAD_REQUEST, Json(UploadResponse {
                        success: false,
                        message: "Missing ticker mappings for some ISINs".to_string(),
                        total_trading_transactions: 0,
                        total_cash_flows: 0,
                        missing_isins: Some(missing_isins),
                    })).into_response();
                }

                all_trading_records = processed_records;
            }
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse {
                    success: false,
                    message: format!("Failed to process trading files: {}", e),
                    total_trading_transactions: 0,
                    total_cash_flows: 0,
                    missing_isins: None,
                })).into_response();
            }
        }
    }

    // Process cash files
    if !cash_files.is_empty() {
        match merge_cash_files(cash_files) {
            Ok(records) => all_cash_records = records,
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse {
                    success: false,
                    message: format!("Failed to process cash files: {}", e),
                    total_trading_transactions: 0,
                    total_cash_flows: 0,
                    missing_isins: None,
                })).into_response();
            }
        }
    }

    // Save to database
    if let Err(e) = db.save_trades(&all_trading_records) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse {
            success: false,
            message: format!("Failed to save trades: {}", e),
            total_trading_transactions: 0,
            total_cash_flows: 0,
            missing_isins: None,
        })).into_response();
    }

    if let Err(e) = db.save_cash_flows(&all_cash_records) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse {
            success: false,
            message: format!("Failed to save cash flows: {}", e),
            total_trading_transactions: 0,
            total_cash_flows: 0,
            missing_isins: None,
        })).into_response();
    }

    // Trigger background precomputation
    let db_arc = Arc::clone(&state.db);
    tokio::spawn(async move {
        if let Err(e) = precompute_portfolio_data(db_arc).await {
            error!("Background precomputation failed: {}", e);
        }
    });

    (StatusCode::OK, Json(UploadResponse {
        success: true,
        message: format!("Successfully uploaded {} trading transactions and {} cash flows. Background processing started.", all_trading_records.len(), all_cash_records.len()),
        total_trading_transactions: all_trading_records.len(),
        total_cash_flows: all_cash_records.len(),
        missing_isins: None,
    })).into_response()
}
