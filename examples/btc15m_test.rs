// examples/btc15m_test.rs
// Test script for BTC 15m market discovery and live orderbook streaming
// Uses the project's existing WebSocket infrastructure

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const KALSHI_API: &str = "https://api.elections.kalshi.com/trade-api/v2";
const PM_GAMMA_API: &str = "https://gamma-api.polymarket.com";
const PM_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

// ============================================================================
// Kalshi Types
// ============================================================================

#[derive(Debug, Deserialize)]
struct KalshiMarketsResponse {
    markets: Vec<KalshiMarket>,
}

#[derive(Debug, Deserialize)]
struct KalshiMarket {
    ticker: String,
    title: Option<String>,
    event_ticker: String,
}

#[derive(Debug, Deserialize)]
struct KalshiOrderbookResponse {
    orderbook: KalshiOrderbook,
}

#[derive(Debug, Deserialize)]
struct KalshiOrderbook {
    yes: Vec<[i64; 2]>,  // [[price, size], ...]
    no: Vec<[i64; 2]>,
}

// ============================================================================
// Polymarket Types
// ============================================================================

#[derive(Debug, Deserialize)]
struct GammaEvent {
    slug: String,
    title: Option<String>,
    markets: Option<Vec<GammaMarket>>,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    slug: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    outcomes: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
}

#[derive(Deserialize, Debug)]
struct BookSnapshot {
    asset_id: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

#[derive(Deserialize, Debug)]
struct PriceLevel {
    price: String,
    size: String,
}

#[derive(Serialize)]
struct SubscribeCmd {
    assets_ids: Vec<String>,
    #[serde(rename = "type")]
    sub_type: &'static str,
}

// ============================================================================
// Discovery
// ============================================================================

async fn discover_kalshi_btc15m() -> Result<Option<KalshiMarket>> {
    let client = reqwest::Client::new();
    let url = format!("{}/markets?series_ticker=KXBTC15M&status=open&limit=1", KALSHI_API);
    
    let resp: KalshiMarketsResponse = client
        .get(&url)
        .send()
        .await?
        .json()
        .await?;
    
    Ok(resp.markets.into_iter().next())
}

async fn discover_pm_btc15m() -> Result<Option<(String, String, String)>> {
    // Calculate current 15-minute window
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let window_start = (now / 900) * 900;
    let slug = format!("btc-updown-15m-{}", window_start);
    
    let client = reqwest::Client::new();
    let url = format!("{}/events?slug={}", PM_GAMMA_API, slug);
    
    let events: Vec<GammaEvent> = client
        .get(&url)
        .send()
        .await?
        .json()
        .await?;
    
    if let Some(event) = events.into_iter().next() {
        if let Some(markets) = event.markets {
            if let Some(market) = markets.into_iter().next() {
                if let Some(tokens_json) = market.clob_token_ids {
                    let tokens: Vec<String> = serde_json::from_str(&tokens_json)?;
                    if tokens.len() >= 2 {
                        return Ok(Some((slug, tokens[0].clone(), tokens[1].clone())));
                    }
                }
            }
        }
    }
    
    Ok(None)
}

async fn fetch_kalshi_orderbook(ticker: &str) -> Result<(Option<i64>, i64, Option<i64>, i64)> {
    let client = reqwest::Client::new();
    let url = format!("{}/markets/{}/orderbook", KALSHI_API, ticker);
    
    let resp: KalshiOrderbookResponse = client
        .get(&url)
        .send()
        .await?
        .json()
        .await?;
    
    let ob = resp.orderbook;
    
    // Best YES ask = 100 - best NO bid
    let best_no_bid = ob.no.iter().max_by_key(|x| x[0]);
    let (yes_ask, yes_size) = if let Some(bid) = best_no_bid {
        (Some(100 - bid[0]), bid[1])
    } else {
        (None, 0)
    };
    
    // Best NO ask = 100 - best YES bid
    let best_yes_bid = ob.yes.iter().max_by_key(|x| x[0]);
    let (no_ask, no_size) = if let Some(bid) = best_yes_bid {
        (Some(100 - bid[0]), bid[1])
    } else {
        (None, 0)
    };
    
    Ok((yes_ask, yes_size, no_ask, no_size))
}

// ============================================================================
// WebSocket Streaming
// ============================================================================

async fn stream_pm_orderbook(up_token: &str, down_token: &str, duration_secs: u64) -> Result<()> {
    let (ws_stream, _) = connect_async(PM_WS_URL)
        .await
        .context("Failed to connect to Polymarket WebSocket")?;
    
    println!("🔌 Connected to Polymarket WebSocket");
    
    let (mut write, mut read) = ws_stream.split();
    
    // Subscribe to both tokens
    let subscribe_msg = SubscribeCmd {
        assets_ids: vec![up_token.to_string(), down_token.to_string()],
        sub_type: "market",
    };
    
    write.send(Message::Text(serde_json::to_string(&subscribe_msg)?)).await?;
    println!("📡 Subscribed to Up and Down tokens");
    
    let start = std::time::Instant::now();
    let mut msg_count = 0;
    
    while start.elapsed() < Duration::from_secs(duration_secs) {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Try to parse as book snapshot
                        if let Ok(books) = serde_json::from_str::<Vec<BookSnapshot>>(&text) {
                            for book in &books {
                                let is_up = book.asset_id == up_token;
                                let label = if is_up { "Up" } else { "Down" };
                                
                                // Best ask (lowest price)
                                if let Some(best_ask) = book.asks.iter()
                                    .filter_map(|a| a.price.parse::<f64>().ok().map(|p| (p, &a.size)))
                                    .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
                                {
                                    let price_cents = (best_ask.0 * 100.0) as i64;
                                    println!("  📊 PM {}: {}¢ ask x {} shares", 
                                             label, price_cents, best_ask.1);
                                    msg_count += 1;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Some(Err(e)) => {
                        eprintln!("WebSocket error: {}", e);
                        break;
                    }
                    None => break,
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    
    println!("📈 Polymarket: Received {} orderbook updates", msg_count);
    Ok(())
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    println!("======================================================================");
    println!("BTC 15m Up/Down LIVE Orderbook Test (Rust)");
    println!("Time: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
    println!("======================================================================");
    
    // Discover markets
    println!("\n📍 Discovering current markets...");
    
    let kalshi = discover_kalshi_btc15m().await?;
    let pm = discover_pm_btc15m().await?;
    
    let kalshi_ticker = match &kalshi {
        Some(m) => {
            println!("  Kalshi:     {}", m.ticker);
            println!("              {}", m.title.as_deref().unwrap_or(""));
            m.ticker.clone()
        }
        None => {
            println!("  Kalshi:     ❌ No open market");
            return Ok(());
        }
    };
    
    let (pm_slug, up_token, down_token) = match &pm {
        Some((slug, up, down)) => {
            println!("  Polymarket: {}", slug);
            println!("              Up: {}...", &up[..40.min(up.len())]);
            println!("              Down: {}...", &down[..40.min(down.len())]);
            (slug.clone(), up.clone(), down.clone())
        }
        None => {
            println!("  Polymarket: ❌ No market found");
            return Ok(());
        }
    };
    
    // Stream for 5 seconds
    println!("\n📈 Streaming LIVE data for 5 seconds...\n");
    
    // Spawn Kalshi polling (REST API, poll every 500ms)
    let kalshi_ticker_clone = kalshi_ticker.clone();
    let kalshi_handle = tokio::spawn(async move {
        for i in 0..10 {
            match fetch_kalshi_orderbook(&kalshi_ticker_clone).await {
                Ok((yes_ask, yes_size, no_ask, no_size)) => {
                    let yes_str = yes_ask.map(|p| format!("{}¢", p)).unwrap_or("N/A".to_string());
                    let no_str = no_ask.map(|p| format!("{}¢", p)).unwrap_or("N/A".to_string());
                    println!("  📊 Kalshi: Yes={} x {} | No={} x {}", 
                             yes_str, yes_size, no_str, no_size);
                }
                Err(e) => eprintln!("  Kalshi error: {}", e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    
    // Stream Polymarket via WebSocket
    let pm_handle = tokio::spawn(async move {
        if let Err(e) = stream_pm_orderbook(&up_token, &down_token, 5).await {
            eprintln!("Polymarket error: {}", e);
        }
    });
    
    // Wait for both
    let _ = tokio::join!(kalshi_handle, pm_handle);
    
    println!("\n======================================================================");
    println!("✅ Complete!");
    println!("======================================================================");
    
    Ok(())
}
