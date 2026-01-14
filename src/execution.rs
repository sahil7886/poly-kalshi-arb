// src/execution.rs
// Execution Engine

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::kalshi::KalshiApiClient;
use crate::polymarket_clob::SharedAsyncClient;
use crate::types::{
    ArbType, MarketPair,
    FastExecutionRequest, GlobalState,
    cents_to_price,
};
use crate::circuit_breaker::CircuitBreaker;
use crate::config::{DRY_RUN_COOLDOWN_SECS, MAX_ORDERBOOK_STALENESS_MS, TEST_MODE_MAX_EXPOSURE_CENTS};
use crate::position_tracker::{FillRecord, PositionChannel};

// =============================================================================
// EXECUTION ENGINE
// =============================================================================

/// Monotonic nanosecond clock for latency measurement
pub struct NanoClock {
    start: Instant,
}

impl NanoClock {
    pub fn new() -> Self {
        Self { start: Instant::now() }
    }

    #[inline(always)]
    pub fn now_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
}

impl Default for NanoClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Execution engine
pub struct ExecutionEngine {
    kalshi: Arc<KalshiApiClient>,
    poly_async: Arc<SharedAsyncClient>,
    state: Arc<GlobalState>,
    circuit_breaker: Arc<CircuitBreaker>,
    position_channel: PositionChannel,
    in_flight: Arc<[AtomicU64; 8]>,
    clock: NanoClock,
    pub dry_run: bool,
    test_mode: bool,
    dashboard: Option<Arc<crate::dashboard::DashboardState>>,
    /// Track last execution time per market (dry_run only)
    dry_run_tracker: Arc<DashMap<u16, Instant>>,
    /// Track current exposure on Polymarket in cents (test mode only)
    poly_exposure_cents: AtomicI64,
    /// Track current exposure on Kalshi in cents (test mode only)
    kalshi_exposure_cents: AtomicI64,
}

impl ExecutionEngine {
    pub fn new(
        kalshi: Arc<KalshiApiClient>,
        poly_async: Arc<SharedAsyncClient>,
        state: Arc<GlobalState>,
        circuit_breaker: Arc<CircuitBreaker>,
        position_channel: PositionChannel,
        dry_run: bool,
        dashboard: Option<Arc<crate::dashboard::DashboardState>>,
    ) -> Self {
        let test_mode = std::env::var("TEST_ARB")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false);

        Self {
            kalshi,
            poly_async,
            state,
            circuit_breaker,
            position_channel,
            in_flight: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            clock: NanoClock::new(),
            dry_run,
            test_mode,
            dashboard,
            dry_run_tracker: Arc::new(DashMap::new()),
            poly_exposure_cents: AtomicI64::new(0),
            kalshi_exposure_cents: AtomicI64::new(0),
        }
    }

    /// Process an execution request
    #[inline]
    pub async fn process(&self, req: FastExecutionRequest) -> Result<ExecutionResult> {
        let market_id = req.market_id;

        // Deduplication check (512 markets via 8x u64 bitmask)
        if market_id < 512 {
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            let mask = 1u64 << bit;
            let prev = self.in_flight[slot].fetch_or(mask, Ordering::AcqRel);
            if prev & mask != 0 {
                return Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: Some("Already in-flight"),
                });
            }
        }

        // Get market pair 
        let market = self.state.get_by_id(market_id)
            .ok_or_else(|| anyhow!("Unknown market_id {}", market_id))?;

        let pair = market.pair()
            .ok_or_else(|| anyhow!("No pair for market_id {}", market_id))?;

        // Calculate profit
        let profit_cents = req.profit_cents();
        if profit_cents < 1 {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Profit below threshold"),
            });
        }

        // Calculate max contracts from size (min of both sides)
        let mut max_contracts = (req.yes_size.min(req.no_size) / 100) as i64;

        // Polymarket requires minimum $1 order value (Kalshi has no minimum)
        // Calculate based on which side is Polymarket for this arb type
        let min_contracts_for_poly = match req.arb_type {
            ArbType::PolyYesKalshiNo => {
                // Poly YES side: ceil(100 / yes_price)
                if req.yes_price > 0 { (99 / req.yes_price as i64) + 1 } else { 1 }
            }
            ArbType::KalshiYesPolyNo => {
                // Poly NO side: ceil(100 / no_price)
                if req.no_price > 0 { (99 / req.no_price as i64) + 1 } else { 1 }
            }
            ArbType::PolyOnly => {
                // Both sides are Poly, need max of both
                let min_yes = if req.yes_price > 0 { (99 / req.yes_price as i64) + 1 } else { 1 };
                let min_no = if req.no_price > 0 { (99 / req.no_price as i64) + 1 } else { 1 };
                min_yes.max(min_no)
            }
            ArbType::KalshiOnly => 1, // No Poly minimum
        };

        // Check $1 minimum BEFORE any capping
        // This happens frequently for low-priced markets, so don't spam logs
        if max_contracts < min_contracts_for_poly {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: None, // Silent skip - not an error, just below minimum
            });
        }

        // SAFETY: In test mode, limit exposure per platform
        if self.test_mode {
            // Calculate exposure per contract for each platform (in cents)
            let (poly_exposure_per_contract, kalshi_exposure_per_contract) = match req.arb_type {
                ArbType::PolyYesKalshiNo => {
                    // Buy Poly YES (pay yes_price), Sell Kalshi NO (liable for 100-no_price)
                    (req.yes_price as i64, (100 - req.no_price) as i64)
                }
                ArbType::KalshiYesPolyNo => {
                    // Buy Kalshi YES (pay yes_price), Sell Poly NO (liable for 100-no_price)
                    ((100 - req.no_price) as i64, req.yes_price as i64)
                }
                ArbType::PolyOnly => {
                    // Buy one side, sell the other - both on Poly
                    // Take max exposure of the two sides
                    let yes_exp = req.yes_price as i64;
                    let no_exp = (100 - req.no_price) as i64;
                    (yes_exp.max(no_exp), 0)
                }
                ArbType::KalshiOnly => {
                    // Buy one side, sell the other - both on Kalshi
                    let yes_exp = req.yes_price as i64;
                    let no_exp = (100 - req.no_price) as i64;
                    (0, yes_exp.max(no_exp))
                }
            };

            // Get current exposure on each platform
            let current_poly = self.poly_exposure_cents.load(Ordering::Relaxed);
            let current_kalshi = self.kalshi_exposure_cents.load(Ordering::Relaxed);

            // Calculate how many contracts we can afford on each platform
            let mut max_affordable = max_contracts;

            if poly_exposure_per_contract > 0 {
                let poly_remaining = TEST_MODE_MAX_EXPOSURE_CENTS - current_poly;
                if poly_remaining <= 0 {
                    self.release_in_flight(market_id);
                    return Ok(ExecutionResult {
                        market_id,
                        success: false,
                        profit_cents: 0,
                        latency_ns: self.clock.now_ns() - req.detected_ns,
                        error: None, // Silent skip - exposure limit
                    });
                }
                let poly_affordable = poly_remaining / poly_exposure_per_contract;
                max_affordable = max_affordable.min(poly_affordable);
            }

            if kalshi_exposure_per_contract > 0 {
                let kalshi_remaining = TEST_MODE_MAX_EXPOSURE_CENTS - current_kalshi;
                if kalshi_remaining <= 0 {
                    self.release_in_flight(market_id);
                    return Ok(ExecutionResult {
                        market_id,
                        success: false,
                        profit_cents: 0,
                        latency_ns: self.clock.now_ns() - req.detected_ns,
                        error: None, // Silent skip - exposure limit
                    });
                }
                let kalshi_affordable = kalshi_remaining / kalshi_exposure_per_contract;
                max_affordable = max_affordable.min(kalshi_affordable);
            }

            // Cap max_contracts but never go below poly minimum
            max_contracts = max_affordable.max(min_contracts_for_poly);

            // If we can't even afford the minimum, skip this trade
            if max_affordable < min_contracts_for_poly {
                self.release_in_flight(market_id);
                return Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: None, // Silent skip - exposure limit
                });
            }
        }

        if max_contracts < 1 {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Insufficient liquidity"),
            });
        }

        // Circuit breaker check
        if let Err(_reason) = self.circuit_breaker.can_execute(&pair.pair_id, max_contracts).await {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Circuit breaker"),
            });
        }

        let latency_to_exec = self.clock.now_ns() - req.detected_ns;

        // DRY RUN COOLDOWN: Skip if this market was executed too recently
        // This prevents spam when liquidity isn't actually consumed
        if self.dry_run {
            if let Some(last_exec) = self.dry_run_tracker.get(&market_id) {
                let elapsed = last_exec.elapsed();
                if elapsed < Duration::from_secs(DRY_RUN_COOLDOWN_SECS) {
                    let _remaining = DRY_RUN_COOLDOWN_SECS.saturating_sub(elapsed.as_secs());
                    self.release_in_flight(market_id);
                    return Ok(ExecutionResult {
                        market_id,
                        success: false,
                        profit_cents: 0,
                        latency_ns: latency_to_exec,
                        error: None, // Silent skip - cooldown active
                    });
                }
            }
        }

        // Check for stale orderbook data
        let kalshi_stale = market.kalshi.is_stale(MAX_ORDERBOOK_STALENESS_MS);
        let poly_stale = market.poly.is_stale(MAX_ORDERBOOK_STALENESS_MS);

        if kalshi_stale || poly_stale {
            let stale_platform = if kalshi_stale && poly_stale {
                "Kalshi+Poly"
            } else if kalshi_stale {
                "Kalshi"
            } else {
                "Poly"
            };
            
            warn!(
                "[EXEC] 🕒 Stale {} data for market_id={} ({}), age: K={}s P={}s",
                stale_platform,
                market_id,
                pair.description,
                market.kalshi.age_ms() / 1000,
                market.poly.age_ms() / 1000
            );
            
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: latency_to_exec,
                error: Some("Stale orderbook"),
            });
        }
        
        // Record arb detection in dashboard (non-blocking)
        // AFTER cooldown check to avoid recording spammed arbs
        if let Some(ref dash) = self.dashboard {
            let min_liq = (req.yes_size.min(req.no_size) / 100) as u32;
            dash.record_arb(profit_cents, min_liq);
        }
        
        info!(
            "[EXEC] 🎯 {} | {:?} y={}¢ n={}¢ | profit={}¢ | {}x | {}µs",
            pair.description,
            req.arb_type,
            req.yes_price,
            req.no_price,
            profit_cents,
            max_contracts,
            latency_to_exec / 1000
        );

        if self.dry_run {
            info!("[EXEC] 🏃 DRY RUN - would execute {} contracts", max_contracts);

            // Update exposure tracking in test mode (simulate successful trade)
            if self.test_mode {
                let (poly_exp, kalshi_exp) = match req.arb_type {
                    ArbType::PolyYesKalshiNo => {
                        (req.yes_price as i64 * max_contracts, (100 - req.no_price) as i64 * max_contracts)
                    }
                    ArbType::KalshiYesPolyNo => {
                        ((100 - req.no_price) as i64 * max_contracts, req.yes_price as i64 * max_contracts)
                    }
                    ArbType::PolyOnly => {
                        let yes_exp = req.yes_price as i64;
                        let no_exp = (100 - req.no_price) as i64;
                        (yes_exp.max(no_exp) * max_contracts, 0)
                    }
                    ArbType::KalshiOnly => {
                        let yes_exp = req.yes_price as i64;
                        let no_exp = (100 - req.no_price) as i64;
                        (0, yes_exp.max(no_exp) * max_contracts)
                    }
                };

                if poly_exp > 0 {
                    let new_poly = self.poly_exposure_cents.fetch_add(poly_exp, Ordering::Relaxed) + poly_exp;
                    info!("[EXPOSURE] Poly: +${:.2} -> ${:.2}/{:.2}",
                        poly_exp as f64 / 100.0,
                        new_poly as f64 / 100.0,
                        TEST_MODE_MAX_EXPOSURE_CENTS as f64 / 100.0);
                }
                if kalshi_exp > 0 {
                    let new_kalshi = self.kalshi_exposure_cents.fetch_add(kalshi_exp, Ordering::Relaxed) + kalshi_exp;
                    info!("[EXPOSURE] Kalshi: +${:.2} -> ${:.2}/{:.2}",
                        kalshi_exp as f64 / 100.0,
                        new_kalshi as f64 / 100.0,
                        TEST_MODE_MAX_EXPOSURE_CENTS as f64 / 100.0);
                }
            }

            // Update last execution time for this market
            self.dry_run_tracker.insert(market_id, Instant::now());
            // Record simulated trade in dashboard
            let total_profit = profit_cents as i16 * max_contracts as i16;
            if let Some(ref dash) = self.dashboard {
                dash.record_trade(total_profit, total_profit > 0);
                dash.push_trade(crate::dashboard::TradeEvent {
                    action: format!("{:?}", req.arb_type),
                    asset: pair.description.to_string(),
                    size: max_contracts as f64,
                    pnl: Some(total_profit as f64 / 100.0),
                    time: chrono::Local::now().format("%H:%M:%S").to_string(),
                });
            }

            self.release_in_flight_delayed(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: true,
                profit_cents: total_profit,
                latency_ns: latency_to_exec,
                error: Some("DRY_RUN"),
            });
        }

        // Execute both legs concurrently 
        let result = self.execute_both_legs_async(&req, &pair, max_contracts).await;

        // Release in-flight after delay
        self.release_in_flight_delayed(market_id);

        match result {
            // Note: For same-platform arbs (PolyOnly/KalshiOnly), these are YES/NO fills, not platform fills
            Ok((yes_filled, no_filled, yes_cost, no_cost, yes_order_id, no_order_id)) => {
                let matched = yes_filled.min(no_filled);
                let success = matched > 0;
                let actual_profit = matched as i16 * 100 - (yes_cost + no_cost) as i16;

                // === AUTO-CLOSE MISMATCHED EXPOSURE (non-blocking) ===
                if yes_filled != no_filled && (yes_filled > 0 || no_filled > 0) {
                    let excess = (yes_filled - no_filled).abs();
                    let (leg1_name, leg2_name) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("P_yes", "K_no"),
                        ArbType::KalshiYesPolyNo => ("K_yes", "P_no"),
                        ArbType::PolyOnly => ("P_yes", "P_no"),
                        ArbType::KalshiOnly => ("K_yes", "K_no"),
                    };
                    warn!("[EXEC] ⚠️ Fill mismatch: {}={} {}={} (excess={})",
                        leg1_name, yes_filled, leg2_name, no_filled, excess);

                    // Spawn auto-close in background (don't block hot path with 2s sleep)
                    let kalshi = self.kalshi.clone();
                    let poly_async = self.poly_async.clone();
                    let arb_type = req.arb_type;
                    let yes_price = req.yes_price;
                    let no_price = req.no_price;
                    let poly_yes_token = pair.poly_yes_token.clone();
                    let poly_no_token = pair.poly_no_token.clone();
                    let kalshi_ticker = pair.kalshi_market_ticker.clone();
                    let original_cost_per_contract = if yes_filled > no_filled {
                        if yes_filled > 0 { yes_cost / yes_filled } else { 0 }
                    } else {
                        if no_filled > 0 { no_cost / no_filled } else { 0 }
                    };

                    tokio::spawn(async move {
                        Self::auto_close_background(
                            kalshi, poly_async, arb_type, yes_filled, no_filled,
                            yes_price, no_price, poly_yes_token, poly_no_token,
                            kalshi_ticker, original_cost_per_contract
                        ).await;
                    });
                }

                if success {
                    self.circuit_breaker.record_success(&pair.pair_id, matched, matched, actual_profit as f64 / 100.0).await;

                    // Update exposure tracking in test mode
                    if self.test_mode {
                        let (poly_exp, kalshi_exp) = match req.arb_type {
                            ArbType::PolyYesKalshiNo => {
                                (req.yes_price as i64 * matched, (100 - req.no_price) as i64 * matched)
                            }
                            ArbType::KalshiYesPolyNo => {
                                ((100 - req.no_price) as i64 * matched, req.yes_price as i64 * matched)
                            }
                            ArbType::PolyOnly => {
                                let yes_exp = req.yes_price as i64;
                                let no_exp = (100 - req.no_price) as i64;
                                (yes_exp.max(no_exp) * matched, 0)
                            }
                            ArbType::KalshiOnly => {
                                let yes_exp = req.yes_price as i64;
                                let no_exp = (100 - req.no_price) as i64;
                                (0, yes_exp.max(no_exp) * matched)
                            }
                        };

                        if poly_exp > 0 {
                            let new_poly = self.poly_exposure_cents.fetch_add(poly_exp, Ordering::Relaxed) + poly_exp;
                            info!("[EXPOSURE] Poly: +${:.2} -> ${:.2}/{:.2}",
                                poly_exp as f64 / 100.0,
                                new_poly as f64 / 100.0,
                                TEST_MODE_MAX_EXPOSURE_CENTS as f64 / 100.0);
                        }
                        if kalshi_exp > 0 {
                            let new_kalshi = self.kalshi_exposure_cents.fetch_add(kalshi_exp, Ordering::Relaxed) + kalshi_exp;
                            info!("[EXPOSURE] Kalshi: +${:.2} -> ${:.2}/{:.2}",
                                kalshi_exp as f64 / 100.0,
                                new_kalshi as f64 / 100.0,
                                TEST_MODE_MAX_EXPOSURE_CENTS as f64 / 100.0);
                        }
                    }

                    // Record trade in dashboard
                    if let Some(ref dash) = self.dashboard {
                        dash.record_trade(actual_profit, actual_profit > 0);
                        dash.push_trade(crate::dashboard::TradeEvent {
                            action: format!("{:?}", req.arb_type),
                            asset: pair.description.to_string(),
                            size: matched as f64,
                            pnl: Some(actual_profit as f64 / 100.0),
                            time: chrono::Local::now().format("%H:%M:%S").to_string(),
                        });
                    }
                }

                if matched > 0 {
                    let (platform1, side1, platform2, side2) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("polymarket", "yes", "kalshi", "no"),
                        ArbType::KalshiYesPolyNo => ("kalshi", "yes", "polymarket", "no"),
                        ArbType::PolyOnly => ("polymarket", "yes", "polymarket", "no"),
                        ArbType::KalshiOnly => ("kalshi", "yes", "kalshi", "no"),
                    };

                    // Log matched fills (both sides)
                    self.position_channel.record_fill(FillRecord::new(
                        &pair.pair_id, &pair.description, platform1, side1,
                        matched as f64, yes_cost as f64 / 100.0 / yes_filled.max(1) as f64,
                        0.0, &yes_order_id,
                    ));
                    self.position_channel.record_fill(FillRecord::new(
                        &pair.pair_id, &pair.description, platform2, side2,
                        matched as f64, no_cost as f64 / 100.0 / no_filled.max(1) as f64,
                        0.0, &no_order_id,
                    ));
                } else {
                    // Log partial/unmatched fills for debugging
                    let (platform1, side1, platform2, side2) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("polymarket", "yes", "kalshi", "no"),
                        ArbType::KalshiYesPolyNo => ("kalshi", "yes", "polymarket", "no"),
                        ArbType::PolyOnly => ("polymarket", "yes", "polymarket", "no"),
                        ArbType::KalshiOnly => ("kalshi", "yes", "kalshi", "no"),
                    };

                    if yes_filled > 0 {
                        self.position_channel.record_fill(FillRecord::new(
                            &pair.pair_id, &pair.description, platform1, side1,
                            yes_filled as f64, yes_cost as f64 / 100.0 / yes_filled as f64,
                            0.0, &yes_order_id,
                        ));
                    }
                    if no_filled > 0 {
                        self.position_channel.record_fill(FillRecord::new(
                            &pair.pair_id, &pair.description, platform2, side2,
                            no_filled as f64, no_cost as f64 / 100.0 / no_filled as f64,
                            0.0, &no_order_id,
                        ));
                    }
                }

                Ok(ExecutionResult {
                    market_id,
                    success,
                    profit_cents: actual_profit,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: if success { None } else { Some("Partial/no fill") },
                })
            }
            Err(_e) => {
                self.circuit_breaker.record_error().await;
                Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: Some("Execution failed"),
                })
            }
        }
    }

    async fn execute_both_legs_async(
        &self,
        req: &FastExecutionRequest,
        pair: &MarketPair,
        contracts: i64,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        match req.arb_type {
            // === CROSS-PLATFORM: Poly YES + Kalshi NO ===
            ArbType::PolyYesKalshiNo => {
                let kalshi_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "no",
                    req.no_price as i64,
                    contracts,
                );
                let poly_fut = self.poly_async.buy_fak(
                    &pair.poly_yes_token,
                    cents_to_price(req.yes_price),
                    contracts as f64,
                );
                let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);
                self.extract_cross_results(kalshi_res, poly_res)
            }

            // === CROSS-PLATFORM: Kalshi YES + Poly NO ===
            ArbType::KalshiYesPolyNo => {
                let kalshi_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "yes",
                    req.yes_price as i64,
                    contracts,
                );
                let poly_fut = self.poly_async.buy_fak(
                    &pair.poly_no_token,
                    cents_to_price(req.no_price),
                    contracts as f64,
                );
                let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);
                self.extract_cross_results(kalshi_res, poly_res)
            }

            // === SAME-PLATFORM: Poly YES + Poly NO ===
            ArbType::PolyOnly => {
                let yes_fut = self.poly_async.buy_fak(
                    &pair.poly_yes_token,
                    cents_to_price(req.yes_price),
                    contracts as f64,
                );
                let no_fut = self.poly_async.buy_fak(
                    &pair.poly_no_token,
                    cents_to_price(req.no_price),
                    contracts as f64,
                );
                let (yes_res, no_res) = tokio::join!(yes_fut, no_fut);
                self.extract_poly_only_results(yes_res, no_res)
            }

            // === SAME-PLATFORM: Kalshi YES + Kalshi NO ===
            ArbType::KalshiOnly => {
                let yes_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "yes",
                    req.yes_price as i64,
                    contracts,
                );
                let no_fut = self.kalshi.buy_ioc(
                    &pair.kalshi_market_ticker,
                    "no",
                    req.no_price as i64,
                    contracts,
                );
                let (yes_res, no_res) = tokio::join!(yes_fut, no_fut);
                self.extract_kalshi_only_results(yes_res, no_res)
            }
        }
    }

    /// Extract results from cross-platform execution
    fn extract_cross_results(
        &self,
        kalshi_res: Result<crate::kalshi::KalshiOrderResponse>,
        poly_res: Result<crate::polymarket_clob::PolyFillAsync>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (kalshi_filled, kalshi_cost, kalshi_order_id) = match kalshi_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi failed: {}", e);
                (0, 0, String::new())
            }
        };

        let (poly_filled, poly_cost, poly_order_id) = match poly_res {
            Ok(fill) => {
                ((fill.filled_size as i64), (fill.fill_cost * 100.0) as i64, fill.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Poly failed: {}", e);
                (0, 0, String::new())
            }
        };

        Ok((kalshi_filled, poly_filled, kalshi_cost, poly_cost, kalshi_order_id, poly_order_id))
    }

    /// Extract results from Poly-only execution (same-platform)
    fn extract_poly_only_results(
        &self,
        yes_res: Result<crate::polymarket_clob::PolyFillAsync>,
        no_res: Result<crate::polymarket_clob::PolyFillAsync>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (yes_filled, yes_cost, yes_order_id) = match yes_res {
            Ok(fill) => {
                ((fill.filled_size as i64), (fill.fill_cost * 100.0) as i64, fill.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Poly YES failed: {}", e);
                (0, 0, String::new())
            }
        };

        let (no_filled, no_cost, no_order_id) = match no_res {
            Ok(fill) => {
                ((fill.filled_size as i64), (fill.fill_cost * 100.0) as i64, fill.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Poly NO failed: {}", e);
                (0, 0, String::new())
            }
        };

        // For same-platform, return YES as "kalshi" slot and NO as "poly" slot
        // This keeps the existing result handling logic working
        Ok((yes_filled, no_filled, yes_cost, no_cost, yes_order_id, no_order_id))
    }

    /// Extract results from Kalshi-only execution (same-platform)
    fn extract_kalshi_only_results(
        &self,
        yes_res: Result<crate::kalshi::KalshiOrderResponse>,
        no_res: Result<crate::kalshi::KalshiOrderResponse>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (yes_filled, yes_cost, yes_order_id) = match yes_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi YES failed: {}", e);
                (0, 0, String::new())
            }
        };

        let (no_filled, no_cost, no_order_id) = match no_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi NO failed: {}", e);
                (0, 0, String::new())
            }
        };

        // For same-platform, return YES as "kalshi" slot and NO as "poly" slot
        Ok((yes_filled, no_filled, yes_cost, no_cost, yes_order_id, no_order_id))
    }

    /// Background auto-close for mismatched fills
    async fn auto_close_background(
        kalshi: Arc<KalshiApiClient>,
        poly_async: Arc<SharedAsyncClient>,
        arb_type: ArbType,
        yes_filled: i64,
        no_filled: i64,
        yes_price: crate::types::PriceCents,
        no_price: crate::types::PriceCents,
        poly_yes_token: Arc<str>,
        poly_no_token: Arc<str>,
        kalshi_ticker: Arc<str>,
        original_cost_per_contract: i64,
    ) {
        let excess = (yes_filled - no_filled).abs();
        if excess == 0 {
            return;
        }

        // Helper to log P&L after close
        let log_close_pnl = |platform: &str, closed: i64, proceeds: i64| {
            if closed > 0 {
                let close_pnl = proceeds - (original_cost_per_contract * excess);
                info!("[EXEC] ✅ Closed {} {} contracts for {}¢ (P&L: {}¢)",
                    closed, platform, proceeds, close_pnl);
            } else {
                warn!("[EXEC] ⚠️ Failed to close {} excess - 0 filled", platform);
            }
        };

        match arb_type {
            ArbType::PolyOnly => {
                let (token, side, price) = if yes_filled > no_filled {
                    (&poly_yes_token, "yes", yes_price)
                } else {
                    (&poly_no_token, "no", no_price)
                };
                let close_price = cents_to_price(1); // Market sell at best available

                info!("[EXEC] 🔄 Waiting 2s for Poly settlement before auto-close ({} {} contracts)", excess, side);
                tokio::time::sleep(Duration::from_secs(2)).await;

                match poly_async.sell_fak(token, close_price, excess as f64).await {
                    Ok(fill) => log_close_pnl("Poly", fill.filled_size as i64, (fill.fill_cost * 100.0) as i64),
                    Err(e) => warn!("[EXEC] ⚠️ Failed to close Poly excess: {}", e),
                }
            }

            ArbType::KalshiOnly => {
                let (side, price) = if yes_filled > no_filled {
                    ("yes", yes_price as i64)
                } else {
                    ("no", no_price as i64)
                };
                let close_price = 1; // Market sell at best available

                match kalshi.sell_ioc(&kalshi_ticker, side, close_price, excess).await {
                    Ok(resp) => {
                        let proceeds = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                        log_close_pnl("Kalshi", resp.order.filled_count(), proceeds);
                    }
                    Err(e) => warn!("[EXEC] ⚠️ Failed to close Kalshi excess: {}", e),
                }
            }

            ArbType::PolyYesKalshiNo => {
                if yes_filled > no_filled {
                    // Poly YES excess
                    let close_price = cents_to_price(1); // Market sell at best available
                    info!("[EXEC] 🔄 Waiting 2s for Poly settlement before auto-close ({} yes contracts)", excess);
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    match poly_async.sell_fak(&poly_yes_token, close_price, excess as f64).await {
                        Ok(fill) => log_close_pnl("Poly", fill.filled_size as i64, (fill.fill_cost * 100.0) as i64),
                        Err(e) => warn!("[EXEC] ⚠️ Failed to close Poly excess: {}", e),
                    }
                } else {
                    // Kalshi NO excess
                    let close_price = 1; // Market sell at best available
                    match kalshi.sell_ioc(&kalshi_ticker, "no", close_price, excess).await {
                        Ok(resp) => {
                            let proceeds = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                            log_close_pnl("Kalshi", resp.order.filled_count(), proceeds);
                        }
                        Err(e) => warn!("[EXEC] ⚠️ Failed to close Kalshi excess: {}", e),
                    }
                }
            }

            ArbType::KalshiYesPolyNo => {
                if yes_filled > no_filled {
                    // Kalshi YES excess
                    let close_price = 1; // Market sell at best available
                    match kalshi.sell_ioc(&kalshi_ticker, "yes", close_price, excess).await {
                        Ok(resp) => {
                            let proceeds = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                            log_close_pnl("Kalshi", resp.order.filled_count(), proceeds);
                        }
                        Err(e) => warn!("[EXEC] ⚠️ Failed to close Kalshi excess: {}", e),
                    }
                } else {
                    // Poly NO excess
                    let close_price = cents_to_price(1); // Market sell at best available
                    info!("[EXEC] 🔄 Waiting 2s for Poly settlement before auto-close ({} no contracts)", excess);
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    match poly_async.sell_fak(&poly_no_token, close_price, excess as f64).await {
                        Ok(fill) => log_close_pnl("Poly", fill.filled_size as i64, (fill.fill_cost * 100.0) as i64),
                        Err(e) => warn!("[EXEC] ⚠️ Failed to close Poly excess: {}", e),
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn release_in_flight(&self, market_id: u16) {
        if market_id < 512 {
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            let mask = !(1u64 << bit);
            self.in_flight[slot].fetch_and(mask, Ordering::Release);
        }
    }

    fn release_in_flight_delayed(&self, market_id: u16) {
        if market_id < 512 {
            let in_flight = self.in_flight.clone();
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let mask = !(1u64 << bit);
                in_flight[slot].fetch_and(mask, Ordering::Release);
            });
        }
    }

    /// Reset exposure counters (called after redemption to free up capacity)
    /// This assumes settled positions have been redeemed and exposure is now zero
    pub fn reset_exposure(&self) {
        if self.test_mode {
            let old_poly = self.poly_exposure_cents.swap(0, Ordering::Relaxed);
            let old_kalshi = self.kalshi_exposure_cents.swap(0, Ordering::Relaxed);

            if old_poly > 0 || old_kalshi > 0 {
                info!(
                    "[EXPOSURE] Reset after redemption: Poly ${:.2} -> $0, Kalshi ${:.2} -> $0",
                    old_poly as f64 / 100.0,
                    old_kalshi as f64 / 100.0
                );
            }
        }
    }
}

/// Execution result
#[derive(Debug, Clone, Copy)]
pub struct ExecutionResult {
    pub market_id: u16,
    pub success: bool,
    pub profit_cents: i16,
    pub latency_ns: u64,
    pub error: Option<&'static str>,
}

/// Create execution channel
pub fn create_execution_channel() -> (mpsc::Sender<FastExecutionRequest>, mpsc::Receiver<FastExecutionRequest>) {
    mpsc::channel(256)
}

/// Execution loop
pub async fn run_execution_loop(
    mut rx: mpsc::Receiver<FastExecutionRequest>,
    engine: Arc<ExecutionEngine>,
) {
    info!("[EXEC] Execution engine started (dry_run={})", engine.dry_run);

    while let Some(req) = rx.recv().await {
        let engine = engine.clone();

        // Process immediately in spawned task
        tokio::spawn(async move {
            match engine.process(req).await {
                Ok(result) if result.success => {
                    info!(
                        "[EXEC] ✅ market_id={} profit={}¢ latency={}µs",
                        result.market_id, result.profit_cents, result.latency_ns / 1000
                    );
                }
                Ok(result) => {
                    // Only log actual errors (not silent skips like cooldown or already-in-flight)
                    if let Some(error_msg) = result.error {
                        if error_msg != "Already in-flight" {
                            warn!(
                                "[EXEC] ⚠️ market_id={}: {}",
                                result.market_id, error_msg
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("[EXEC] ❌ Error: {}", e);
                }
            }
        });
    }

    info!("[EXEC] Execution engine stopped");
}