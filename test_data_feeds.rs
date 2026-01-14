//! Manual test script to verify Kalshi and Polymarket data feeds
//! 
//! This script will:
//! 1. Discover current markets
//! 2. Fetch orderbook data from Kalshi REST API
//! 3. Connect to Polymarket WebSocket and fetch orderbook snapshots
//! 4. Display liquidity information for debugging

use anyhow::{Context, Result};
use poly_kalshi_arb::{
    cache::TeamCache,
    config::ENABLED_LEAGUES,
    discovery::DiscoveryClient,
    kalshi::{KalshiApiClient, KalshiConfig},
    polymarket::GammaClient,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "test_data_feeds=debug,poly_kalshi_arb=debug".into()),
        )
        .init();

    // Load .env
    dotenvy::dotenv().ok();

    info!("🔍 Testing Data Feeds");
    info!("=====================\n");

    // Load credentials
    let kalshi_config = KalshiConfig::from_env()?;
    let kalshi_api = KalshiApiClient::new(kalshi_config);
    
    // Load team cache
    let team_cache = TeamCache::load();
    info!("📂 Loaded {} team mappings\n", team_cache.len());

    // Create discovery client
    let discovery = DiscoveryClient::new(kalshi_api.clone(), team_cache);

    // Run discovery
    info!("🔍 Running market discovery...");
    let result = discovery.discover_all_force(ENABLED_LEAGUES).await;
    
    info!("📊 Discovery complete: {} pairs found\n", result.pairs.len());
    
    if result.pairs.is_empty() {
        info!("❌ No market pairs found!");
        return Ok(());
    }

    // Display discovered pairs
    info!("📋 Discovered Market Pairs:");
    info!("===========================");
    for (i, pair) in result.pairs.iter().enumerate().take(5) {
        info!("{}. {} | {} | Kalshi: {}", 
              i + 1, 
              pair.description, 
              pair.market_type,
              pair.kalshi_market_ticker);
        info!("   Poly: YES={}, NO={}", pair.poly_yes_token, pair.poly_no_token);
    }
    if result.pairs.len() > 5 {
        info!("... and {} more pairs\n", result.pairs.len() - 5);
    }

    // Test Kalshi orderbook for first market
    info!("\n🔵 Testing Kalshi Orderbook:");
    info!("============================");
    let first_pair = &result.pairs[0];
    
    match kalshi_api.get_orderbook(&first_pair.kalshi_market_ticker).await {
        Ok(book) => {
            info!("✅ Kalshi orderbook fetched for {}", first_pair.kalshi_market_ticker);
            
            // Display YES side
            if let Some(yes_book) = book.get("yes") {
                let yes_asks = yes_book.get("asks").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                let yes_bids = yes_book.get("bids").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                info!("  YES side:");
                info!("    Best Ask: {:?}", yes_asks.first());
                info!("    Best Bid: {:?}", yes_bids.first());
                info!("    Total Asks: {}, Total Bids: {}", yes_asks.len(), yes_bids.len());
            }
            
            // Display NO side
            if let Some(no_book) = book.get("no") {
                let no_asks = no_book.get("asks").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                let no_bids = no_book.get("bids").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                info!("  NO side:");
                info!("    Best Ask: {:?}", no_asks.first());
                info!("    Best Bid: {:?}", no_bids.first());
                info!("    Total Asks: {}, Total Bids: {}", no_asks.len(), no_bids.len());
            }
        }
        Err(e) => {
            info!("❌ Failed to fetch Kalshi orderbook: {}", e);
        }
    }

    // Test Polymarket orderbook
    info!("\n🟣 Testing Polymarket Orderbook:");
    info!("================================");
    
    let gamma = GammaClient::new();
    
    // Fetch orderbook for YES token
    let clob_url = format!("https://clob.polymarket.com/book?token_id={}", first_pair.poly_yes_token);
    info!("Fetching YES orderbook from: {}", clob_url);
    
    match reqwest::get(&clob_url).await {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.json::<serde_json::Value>().await {
                    Ok(book) => {
                        info!("✅ Polymarket YES orderbook fetched");
                        let asks = book.get("asks").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                        let bids = book.get("bids").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                        info!("  YES token: {}", first_pair.poly_yes_token);
                        info!("    Best Ask: {:?}", asks.first());
                        info!("    Best Bid: {:?}", bids.first());
                        info!("    Total Asks: {}, Total Bids: {}", asks.len(), bids.len());
                    }
                    Err(e) => info!("❌ Failed to parse YES orderbook: {}", e),
                }
            } else {
                info!("❌ Polymarket YES orderbook request failed: {}", resp.status());
            }
        }
        Err(e) => info!("❌ Failed to fetch Polymarket YES orderbook: {}", e),
    }

    // Fetch orderbook for NO token
    let clob_url = format!("https://clob.polymarket.com/book?token_id={}", first_pair.poly_no_token);
    info!("\nFetching NO orderbook from: {}", clob_url);
    
    match reqwest::get(&clob_url).await {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.json::<serde_json::Value>().await {
                    Ok(book) => {
                        info!("✅ Polymarket NO orderbook fetched");
                        let asks = book.get("asks").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                        let bids = book.get("bids").and_then(|v| v.as_array()).unwrap_or(&vec![]);
                        info!("  NO token: {}", first_pair.poly_no_token);
                        info!("    Best Ask: {:?}", asks.first());
                        info!("    Best Bid: {:?}", bids.first());
                        info!("    Total Asks: {}, Total Bids: {}", asks.len(), bids.len());
                    }
                    Err(e) => info!("❌ Failed to parse NO orderbook: {}", e),
                }
            } else {
                info!("❌ Polymarket NO orderbook request failed: {}", resp.status());
            }
        }
        Err(e) => info!("❌ Failed to fetch Polymarket NO orderbook: {}", e),
    }

    info!("\n✅ Data feed test complete!");
    
    Ok(())
}
