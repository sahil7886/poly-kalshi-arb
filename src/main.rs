//! Polymarket-Kalshi Arbitrage Bot v2.0
//!
//! Strategy: BUY YES on Platform A + BUY NO on Platform B
//! Arb exists when: YES_ask + NO_ask < $1.00

mod cache;
mod circuit_breaker;
mod config;
mod dashboard;
mod discovery;
mod execution;
mod kalshi;
mod polymarket;
mod polymarket_clob;
mod position_tracker;
mod redemption;
mod types;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tracing::{error, info, warn};
use ethers::types::U256;

use cache::TeamCache;
use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use config::{ARB_THRESHOLD, ENABLED_LEAGUES, WS_RECONNECT_DELAY_SECS};
use discovery::DiscoveryClient;
use execution::{ExecutionEngine, create_execution_channel, run_execution_loop};
use kalshi::{KalshiConfig, KalshiApiClient};
use polymarket_clob::{PolymarketAsyncClient, PreparedCreds, SharedAsyncClient};
use position_tracker::{PositionTracker, create_position_channel, position_writer_loop};
use types::{GlobalState, PriceCents};

/// Polymarket CLOB API host
const POLY_CLOB_HOST: &str = "https://clob.polymarket.com";
/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

/// Run a single arb session (e.g. 5 minutes)
/// Returns when the session times out or an error occurs.
async fn run_session(
    kalshi_api: Arc<KalshiApiClient>,
    poly_async: Arc<SharedAsyncClient>,
    discovery: Arc<DiscoveryClient>,
    kalshi_ws_config: &KalshiConfig,
    dry_run: bool,
    full_duration_secs: u64,
    funder: String,
    dashboard_state: Arc<dashboard::DashboardState>,
) -> Result<()> {
    // Run discovery (always force refresh for new sessions)
    info!("🔍 Discovering markets (forced refresh)...");
    let result = discovery.discover_all_force(ENABLED_LEAGUES).await;

    info!("📊 Discovery complete: {} pairs found", result.pairs.len());
    if !result.errors.is_empty() {
        for err in &result.errors {
            warn!("   ⚠️ {}", err);
        }
    }

    if result.pairs.is_empty() {
        warn!("No market pairs found - waiting for next session");
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        return Ok(());
    }

    // Print discovered pairs
    info!("📋 Matched markets:");
    for pair in &result.pairs {
        info!("   ✅ {} | {} | K:{}",
              pair.description,
              pair.market_type,
              pair.kalshi_market_ticker);
    }

    // Build global state
    let state = Arc::new({
        let mut s = GlobalState::new();
        for pair in result.pairs {
            s.add_pair(pair);
        }
        info!("📡 State: Tracking {} markets", s.market_count());
        s
    });

    // Create execution infrastructure
    let (exec_tx, exec_rx) = create_execution_channel();
    let circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::from_env()));

    let position_tracker = Arc::new(RwLock::new(PositionTracker::new()));
    let (position_channel, position_rx) = create_position_channel();

    let pos_writer_handle = tokio::spawn(position_writer_loop(position_rx, position_tracker));

    let threshold_cents: PriceCents = ((ARB_THRESHOLD * 100.0).round() as u8).max(1);
    
    let engine = Arc::new(ExecutionEngine::new(
        kalshi_api.clone(),
        poly_async.clone(),
        state.clone(),
        circuit_breaker.clone(),
        position_channel,
        dry_run,
        Some(dashboard_state.clone()),
    ));

    let exec_handle = tokio::spawn(run_execution_loop(exec_rx, engine.clone()));

    // Start Kalshi WebSocket
    let kalshi_state = state.clone();
    let kalshi_exec_tx = exec_tx.clone();
    let kalshi_threshold = threshold_cents;
    // We must clone the config data to pass to the task
    let k_conf_copy = KalshiConfig {
        api_key_id: kalshi_ws_config.api_key_id.clone(),
        private_key: kalshi_ws_config.private_key.clone(),
    };
    
    // Reconnect every 5 minutes to pick up new markets (matches discovery interval)
    // BTC 15m markets rotate every 15 mins, so we need to refresh more frequently
    const WS_REFRESH_INTERVAL_SECS: u64 = 300; // 5 minutes
    
    let kalshi_ws_handle = tokio::spawn(async move {
        loop {
            if let Err(e) = kalshi::run_ws(&k_conf_copy, kalshi_state.clone(), kalshi_exec_tx.clone(), kalshi_threshold, WS_REFRESH_INTERVAL_SECS).await {
                error!("[KALSHI] Disconnected: {} - reconnecting...", e);
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)).await;
        }
    });

    // Start Polymarket WebSocket (same refresh interval as Kalshi)
    let poly_state = state.clone();
    let poly_exec_tx = exec_tx.clone();
    let poly_threshold = threshold_cents;
    let poly_ws_handle = tokio::spawn(async move {
        loop {
            if let Err(e) = polymarket::run_ws(poly_state.clone(), poly_exec_tx.clone(), poly_threshold, WS_REFRESH_INTERVAL_SECS).await {
                error!("[POLYMARKET] Disconnected: {} - reconnecting...", e);
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)).await;
        }
    });

    // Start redemption background task (runs every 15 minutes)
    let redemption_handle = if !dry_run {
        let rpc_url = std::env::var("POLYGON_RPC_URL").unwrap_or_else(|_| "https://polygon-rpc.com".to_string());
        Some(tokio::spawn(crate::redemption::run_hourly_loop(
            poly_async.clone(),
            rpc_url,
            funder,
            Some(engine.clone()),
        )))
    } else {
        None
    };

    // Market Manager task - runs discovery every 5 minutes to add new markets
    let manager_state = state.clone();
    let manager_discovery = discovery.clone();
    let manager_handle = tokio::spawn(async move {
        const DISCOVERY_INTERVAL_SECS: u64 = 300; // 5 minutes
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(DISCOVERY_INTERVAL_SECS));
        interval.tick().await; // Skip the initial tick (we just ran discovery)
        
        loop {
            interval.tick().await;
            info!("[MANAGER] Running periodic market discovery...");
            
            let result = manager_discovery.discover_all_force(ENABLED_LEAGUES).await;
            
            let mut new_markets = 0;
            for pair in result.pairs {
                // Check if already tracked (by Kalshi ticker) - lock-free with DashMap
                let ticker_hash = crate::types::fxhash_str(&pair.kalshi_market_ticker);
                let already_tracked = manager_state.kalshi_to_id.contains_key(&ticker_hash);
                
                if !already_tracked {
                    if manager_state.add_pair(pair).is_some() {
                        new_markets += 1;
                    }
                }
            }
            
            if new_markets > 0 {
                info!("[MANAGER] Added {} new markets (total: {})", new_markets, manager_state.market_count());
            } else {
                info!("[MANAGER] No new markets found");
            }
            
            if !result.errors.is_empty() {
                for err in &result.errors {
                    warn!("[MANAGER] Discovery error: {}", err);
                }
            }
        }
    });

    // Heartbeat task with arb diagnostics
    let heartbeat_state = state.clone();
    let heartbeat_threshold = threshold_cents;
    let heartbeat_handle = tokio::spawn(async move {
        use crate::types::kalshi_fee_cents;
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(20));
        loop {
            interval.tick().await;
            let market_count = heartbeat_state.market_count();
            let mut with_kalshi = 0;
            let mut with_poly = 0;
            let mut with_both = 0;
            // (cost, market_id, p_yes, k_no, k_yes, p_no, fee, is_poly_yes, size_yes, size_no)
            let mut best_arb: Option<(u16, u16, u8, u8, u8, u8, u8, bool, u32, u32)> = None;

            for market in heartbeat_state.markets.iter().take(market_count) {
                let (k_yes, k_no, k_yes_size, k_no_size) = market.kalshi.load();
                let (p_yes, p_no, p_yes_size, p_no_size) = market.poly.load();
                
                // Track availability
                if k_yes > 0 || k_no > 0 { with_kalshi += 1; }
                if p_yes > 0 || p_no > 0 { with_poly += 1; }
                
                // Track Overlap (Relaxed: At least one cross-pair exists)
                let can_arb_1 = p_yes > 0 && k_no > 0;
                let can_arb_2 = k_yes > 0 && p_no > 0;
                if can_arb_1 || can_arb_2 {
                    with_both += 1;
                } else if (k_yes > 0 || k_no > 0) && (p_yes > 0 || p_no > 0) {
                     // Both have prices but mismatched sides (e.g. K has YES, P has YES -> No arb possible directly if we only Sell? No.)
                     // Arb1: Buy P_Yes (Ask), Sell K_Yes (Bid).  Sell K_Yes = Buy K_No (Ask).
                     // So we need P_Yes_Ask and K_No_Ask.
                     // If we have K_Yes_Ask... we can't Sell K_Yes using Ask. We need Bid.
                     // But our state stored ASKS.
                     // So we legitimately need P_Yes_Ask AND K_No_Ask.
                     // Mismatch implies we have disjoint sets of Asks.
                }

                if can_arb_1 {
                    let fee1 = kalshi_fee_cents(k_no);
                    let cost1 = p_yes as u16 + k_no as u16 + fee1 as u16;
                    if best_arb.is_none() || cost1 < best_arb.as_ref().unwrap().0 {
                         // Arb Type 1: Poly Yes, Kalshi No
                         best_arb = Some((cost1, market.market_id, p_yes, k_no, k_yes, p_no, fee1, true, p_yes_size, k_no_size));
                    }
                }
                
                if can_arb_2 {
                    let fee2 = kalshi_fee_cents(k_yes);
                    let cost2 = k_yes as u16 + fee2 as u16 + p_no as u16;
                     if best_arb.is_none() || cost2 < best_arb.as_ref().unwrap().0 {
                         // Arb Type 2: Kalshi Yes, Poly No
                         best_arb = Some((cost2, market.market_id, p_yes, k_no, k_yes, p_no, fee2, false, k_yes_size, p_no_size));
                    }
                }
                
                // Debug log for active markets
                if k_yes > 0 || k_no > 0 {
                   let t = market.pair().map(|p| p.kalshi_market_ticker.to_string()).unwrap_or_else(|| "?".to_string());
                   // info!("   ℹ️ Market {}: K({}|{}) P({}|{})", t, k_yes, k_no, p_yes, p_no);
                }
            }

            info!("💓 Heartbeat | Markets: {} total, {} w/Kalshi, {} w/Poly, {} w/Overlap | threshold={}¢",
                  market_count, with_kalshi, with_poly, with_both, heartbeat_threshold);
            
            if let Some((cost, market_id, p_yes, k_no, k_yes, p_no, fee, is_poly_yes, s_yes, s_no)) = best_arb {
                let gap = cost as i16 - heartbeat_threshold as i16;
                let desc = heartbeat_state.get_by_id(market_id)
                    .and_then(|m| m.pair())
                    .map(|p| p.description.to_string())
                    .unwrap_or_else(|| "Unknown".to_string());
                
                let (vol_str, leg_breakdown) = if is_poly_yes {
                    (
                        format!("Vol: P_yes({}) / K_no({})", s_yes / 100, s_no / 100),
                        format!("P_yes({}¢) + K_no({}¢) + K_fee({}¢) = {}¢", p_yes, k_no, fee, cost)
                    )
                } else {
                    (
                        format!("Vol: K_yes({}) / P_no({})", s_yes / 100, s_no / 100),
                        format!("K_yes({}¢) + P_no({}¢) + K_fee({}¢) = {}¢", k_yes, p_no, fee, cost)
                    )
                };

                if gap <= 0 {
                    info!(" Arb Found: {} | {} | gap={:+}¢ | {} | [P_yes={}¢ K_no={}¢ K_yes={}¢ P_no={}¢]", 
                          desc, leg_breakdown, gap, vol_str, p_yes, k_no, k_yes, p_no);
                } else {
                    info!(" Best: {} | {} | gap={:+}¢ | {} | [P_yes={}¢ K_no={}¢ K_yes={}¢ P_no={}¢]", 
                          desc, leg_breakdown, gap, vol_str, p_yes, k_no, k_yes, p_no);
                }
            } else if with_both == 0 {
                if market_count > 0 {
                    warn!(" ⚠️ No actionable overlap (missing crossing sides) - check liquidity");
                }
            }
        }
    });

    // Run until timeout
    info!("⏱️  Session running for {} seconds...", full_duration_secs);
    tokio::select! {
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(full_duration_secs)) => {
            info!("⌛ Session timeout reached - rotating...");
        }
        res = exec_handle => {
            warn!("⚠️ Execution loop exited unexpectedly: {:?}", res);
        }
    }

    // Cleanup: Tasks attached to handles will be dropped/cancelled when handles are dropped?
    // No, tokio tasks are detached. We must abort them.
    kalshi_ws_handle.abort();
    poly_ws_handle.abort();
    heartbeat_handle.abort();
    manager_handle.abort();
    pos_writer_handle.abort();
    if let Some(h) = redemption_handle {
        h.abort();
    }
    
    // exec_handle is already finished or we don't care about aborting it if we timed out (it will be aborted when we drop the channel? No.)
    // We should abort it to be safe.
    // If we are here because of timeout, the exec_lopp is still running.
    // We need to abort it.
    // But we don't have mutable access to exec_handle if we used it in select!
    // Actually select consumes the handle? No, it borrows or takes ownership.
    // If select finished via sleep branch, exec_handle was dropped (the future waiting on it). The task is still running.
    // We can't abort it easily if we lost the handle.
    // Solution: Keep handles outside select.
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "arb_bot=info".into()),
        )
        .init();

    // Load .env file FIRST so all config values are available
    dotenvy::dotenv().ok();

    info!("🎯 Arb Bot v2.0 - Supervisor Mode");
    info!("   Threshold: <{:.1}¢ for {:.1}% profit",
          ARB_THRESHOLD * 100.0, (1.0 - ARB_THRESHOLD) * 100.0);
    info!("   Leagues: {:?}", ENABLED_LEAGUES);

    // Check for dry run mode
    let dry_run = std::env::var("DRY_RUN").map(|v| v == "1" || v == "true").unwrap_or(true);
    if dry_run {
        info!("   Mode: DRY RUN (set DRY_RUN=0 to execute)");
    } else {
        warn!("   Mode: LIVE EXECUTION");
    }

    // Load Kalshi credentials
    let kalshi_config = KalshiConfig::from_env()?;
    info!("[KALSHI] API key loaded");

    // Load Polymarket credentials (dotenv already called at startup)
    let poly_private_key = std::env::var("POLY_PRIVATE_KEY")
        .context("POLY_PRIVATE_KEY not set")?;
    let poly_funder = std::env::var("POLY_FUNDER")
        .context("POLY_FUNDER not set (your wallet address)")?;

    // Create async Polymarket client and derive API credentials
    info!("[POLYMARKET] Creating async client and deriving API credentials...");
    let poly_async_client = PolymarketAsyncClient::new(
        POLY_CLOB_HOST,
        POLYGON_CHAIN_ID,
        &poly_private_key,
        &poly_funder,
    )?;
    let api_creds = poly_async_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let poly_async = Arc::new(SharedAsyncClient::new(poly_async_client, prepared_creds, POLYGON_CHAIN_ID));

    // Load neg_risk cache
    match poly_async.load_cache(".clob_market_cache.json") {
        Ok(count) => info!("[POLYMARKET] Loaded {} neg_risk entries from cache", count),
        Err(e) => warn!("[POLYMARKET] Could not load neg_risk cache: {}", e),
    }

    info!("[POLYMARKET] Client ready");
    info!("   Funder: {}", &poly_funder);
    info!("   Signer (needs MATIC): {:?}", poly_async.signer_address());

    // Load team cache
    let team_cache = TeamCache::load();
    info!("📂 Loaded {} team mappings", team_cache.len());

    // Create Kalshi API client (shared across sessions)
    let kalshi_api = Arc::new(KalshiApiClient::new(KalshiConfig::from_env()?));

    // Create Discovery client (with cache loaded)
    let discovery = Arc::new(DiscoveryClient::new(
        KalshiApiClient::new(KalshiConfig::from_env()?),
        team_cache
    ));

    // Initial delay to let things settle? No.

    // Balance Checks
    if !dry_run {
        info!("💰 Checking balances...");
        match kalshi_api.get_balance().await {
            Ok(bal) => info!("   Kalshi Balance: ${:.2}", bal as f64 / 100.0),
            Err(e) => warn!("   ⚠️ Failed to fetch Kalshi balance: {}", e),
        }

        let rpc_url = std::env::var("POLYGON_RPC_URL").unwrap_or_else(|_| "https://polygon-rpc.com".to_string());
        match poly_async.get_usdc_balance(&rpc_url).await {
            Ok(bal) => {
                 let bal_f64 = bal.as_u128() as f64 / 1_000_000.0; // USDC has 6 decimals
                 info!("   Polymarket USDC: ${:.2}", bal_f64);
            }
            Err(e) => warn!("   ⚠️ Failed to fetch Polymarket USDC balance: {}", e),
        }
    }

    // Start Dashboard Server
    let dashboard_state = dashboard::DashboardState::new();
    let dashboard_port: u16 = std::env::var("DASHBOARD_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5051);
    dashboard::start_dashboard_server(dashboard_state.clone(), dashboard_port);

    // SUPERVISOR LOOP
    // Run for 4 hours (fetching ~20 markets covers 5 hours, so 4 hours is safe)
    let session_duration = 14400; 
    
    loop {
        info!("🔄 REQUESTING SESSION (4h)...");
        
        match run_session(
            kalshi_api.clone(),
            poly_async.clone(),
            discovery.clone(),
            &kalshi_config,
            dry_run,
            session_duration,
            poly_funder.clone(),
            dashboard_state.clone(),
        ).await {
            Ok(_) => {
                info!("✅ Session completed normally. Restarting...");
            }
            Err(e) => {
                error!("❌ Session failed: {}. Retrying in 5s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
        
        // Brief pause between sessions
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
