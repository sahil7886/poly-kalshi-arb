// src/discovery.rs
// Market discovery - matches Kalshi events to Polymarket markets

use anyhow::Result;
use futures_util::{stream, StreamExt};
use governor::{Quota, RateLimiter, state::NotKeyed, clock::DefaultClock, middleware::NoOpMiddleware};
use serde::{Serialize, Deserialize};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::cache::TeamCache;
use crate::config::{LeagueConfig, get_league_configs, get_league_config};
use crate::kalshi::KalshiApiClient;
use crate::polymarket::GammaClient;
use crate::types::{MarketPair, MarketType, DiscoveryResult, KalshiMarket, KalshiEvent};

/// Max concurrent Gamma API requests
const GAMMA_CONCURRENCY: usize = 20;

/// Kalshi rate limit: 2 requests per second (very conservative - they rate limit aggressively)
/// Must be conservative because discovery runs many leagues/series in parallel
const KALSHI_RATE_LIMIT_PER_SEC: u32 = 2;

/// Max concurrent Kalshi API requests GLOBALLY across all leagues/series
/// This is the hard cap - prevents bursting even when rate limiter has tokens
const KALSHI_GLOBAL_CONCURRENCY: usize = 1;

/// Cache file path
const DISCOVERY_CACHE_PATH: &str = ".discovery_cache.json";

/// Cache TTL in seconds (2 hours - new markets appear every ~2 hours)
const CACHE_TTL_SECS: u64 = 2 * 60 * 60;

/// Task for parallel Gamma lookup
struct GammaLookupTask {
    event: Arc<KalshiEvent>,
    market: KalshiMarket,
    poly_slug: String,
    market_type: MarketType,
    league: String,
}

/// Type alias for Kalshi rate limiter
type KalshiRateLimiter = RateLimiter<NotKeyed, governor::state::InMemoryState, DefaultClock, NoOpMiddleware>;

/// Persistent cache for discovered market pairs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryCache {
    /// Unix timestamp when cache was created
    timestamp_secs: u64,
    /// Cached market pairs
    pairs: Vec<MarketPair>,
    /// Set of known Kalshi market tickers (for incremental updates)
    known_kalshi_tickers: Vec<String>,
}

impl DiscoveryCache {
    fn new(pairs: Vec<MarketPair>) -> Self {
        let known_kalshi_tickers: Vec<String> = pairs.iter()
            .map(|p| p.kalshi_market_ticker.to_string())
            .collect();
        Self {
            timestamp_secs: current_unix_secs(),
            pairs,
            known_kalshi_tickers,
        }
    }

    fn is_expired(&self) -> bool {
        let now = current_unix_secs();
        now.saturating_sub(self.timestamp_secs) > CACHE_TTL_SECS
    }

    fn age_secs(&self) -> u64 {
        current_unix_secs().saturating_sub(self.timestamp_secs)
    }

    fn has_ticker(&self, ticker: &str) -> bool {
        self.known_kalshi_tickers.iter().any(|t| t == ticker)
    }
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Market discovery client
pub struct DiscoveryClient {
    kalshi: Arc<KalshiApiClient>,
    gamma: Arc<GammaClient>,
    pub team_cache: Arc<TeamCache>,
    kalshi_limiter: Arc<KalshiRateLimiter>,
    kalshi_semaphore: Arc<Semaphore>,  // Global concurrency limit for Kalshi
    gamma_semaphore: Arc<Semaphore>,
}

impl DiscoveryClient {
    pub fn new(kalshi: KalshiApiClient, team_cache: TeamCache) -> Self {
        // Create token bucket rate limiter for Kalshi
        let quota = Quota::per_second(NonZeroU32::new(KALSHI_RATE_LIMIT_PER_SEC).unwrap());
        let kalshi_limiter = Arc::new(RateLimiter::direct(quota));

        Self {
            kalshi: Arc::new(kalshi),
            gamma: Arc::new(GammaClient::new()),
            team_cache: Arc::new(team_cache),
            kalshi_limiter,
            kalshi_semaphore: Arc::new(Semaphore::new(KALSHI_GLOBAL_CONCURRENCY)),
            gamma_semaphore: Arc::new(Semaphore::new(GAMMA_CONCURRENCY)),
        }
    }

    /// Load cache from disk (async)
    async fn load_cache() -> Option<DiscoveryCache> {
        let data = tokio::fs::read_to_string(DISCOVERY_CACHE_PATH).await.ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Save cache to disk (async)
    async fn save_cache(cache: &DiscoveryCache) -> Result<()> {
        let data = serde_json::to_string_pretty(cache)?;
        tokio::fs::write(DISCOVERY_CACHE_PATH, data).await?;
        Ok(())
    }
    
    /// Discover all market pairs with caching support
    ///
    /// Strategy:
    /// 1. Try to load cache from disk
    /// 2. If cache exists and is fresh (<2 hours), use it directly
    /// 3. If cache exists but is stale, load it + fetch incremental updates
    /// 4. If no cache, do full discovery
    pub async fn discover_all(&self, leagues: &[&str]) -> DiscoveryResult {
        // Try to load existing cache
        let cached = Self::load_cache().await;

        match cached {
            Some(cache) if !cache.is_expired() => {
                // Cache is fresh - use it directly
                info!("📂 Loaded {} pairs from cache (age: {}s)",
                      cache.pairs.len(), cache.age_secs());
                // Still run mention discovery so mention markets show up even when sports cache is fresh.
                // Merge mention pairs into cached pairs and refresh cache if new ones are found.
                let mention_result = self.discover_mention_markets().await;
                let mut all_pairs = cache.pairs;
                let mut new_mentions = 0usize;

                for pair in mention_result.pairs {
                    if !all_pairs.iter().any(|p| *p.kalshi_market_ticker == *pair.kalshi_market_ticker) {
                        all_pairs.push(pair);
                        new_mentions += 1;
                    }
                }

                if new_mentions > 0 {
                    info!("🆕 Added {} mention market pairs on top of fresh cache", new_mentions);
                    let new_cache = DiscoveryCache::new(all_pairs.clone());
                    if let Err(e) = Self::save_cache(&new_cache).await {
                        warn!("Failed to update discovery cache with mention pairs: {}", e);
                    }
                }

                return DiscoveryResult {
                    pairs: all_pairs,
                    kalshi_events_found: new_mentions,
                    poly_matches: new_mentions,
                    poly_misses: 0,
                    errors: mention_result.errors,
                };
            }
            Some(cache) => {
                // Cache is stale - do incremental discovery
                info!("📂 Cache expired (age: {}s), doing incremental refresh...", cache.age_secs());
                return self.discover_incremental(leagues, cache).await;
            }
            None => {
                // No cache - do full discovery
                info!("📂 No cache found, doing full discovery...");
            }
        }

        // Full discovery (no cache)
        let result = self.discover_full(leagues).await;

        // Save to cache
        if !result.pairs.is_empty() {
            let cache = DiscoveryCache::new(result.pairs.clone());
            if let Err(e) = Self::save_cache(&cache).await {
                warn!("Failed to save discovery cache: {}", e);
            } else {
                info!("💾 Saved {} pairs to cache", result.pairs.len());
            }
        }

        result
    }

    /// Force full discovery (ignores cache)
    pub async fn discover_all_force(&self, leagues: &[&str]) -> DiscoveryResult {
        info!("🔄 Forced full discovery (ignoring cache)...");
        let result = self.discover_full(leagues).await;

        // Save to cache
        if !result.pairs.is_empty() {
            let cache = DiscoveryCache::new(result.pairs.clone());
            if let Err(e) = Self::save_cache(&cache).await {
                warn!("Failed to save discovery cache: {}", e);
            } else {
                info!("💾 Saved {} pairs to cache", result.pairs.len());
            }
        }

        result
    }

    /// Full discovery without cache
    async fn discover_full(&self, leagues: &[&str]) -> DiscoveryResult {
        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues.iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Parallel discovery across all leagues
        let league_futures: Vec<_> = configs.iter()
            .map(|config| self.discover_league(config, None))
            .collect();

        let league_results = futures_util::future::join_all(league_futures).await;

        // Merge results
        let mut result = DiscoveryResult::default();
        for league_result in league_results {
            result.pairs.extend(league_result.pairs);
            result.poly_matches += league_result.poly_matches;
            result.errors.extend(league_result.errors);
        }
        
        // Also discover mention markets (runs after sports to avoid rate limit conflicts)
        let mention_result = self.discover_mention_markets().await;
        result.pairs.extend(mention_result.pairs);
        result.poly_matches += mention_result.poly_matches;
        result.errors.extend(mention_result.errors);
        
        result.kalshi_events_found = result.pairs.len();

        result
    }

    /// Incremental discovery - merge cached pairs with newly discovered ones
    async fn discover_incremental(&self, leagues: &[&str], cache: DiscoveryCache) -> DiscoveryResult {
        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues.iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Discover with filter for known tickers
        let league_futures: Vec<_> = configs.iter()
            .map(|config| self.discover_league(config, Some(&cache)))
            .collect();

        let league_results = futures_util::future::join_all(league_futures).await;

        // Merge cached pairs with newly discovered ones
        let mut all_pairs = cache.pairs;
        let mut new_count = 0;

        for league_result in league_results {
            for pair in league_result.pairs {
                if !all_pairs.iter().any(|p| *p.kalshi_market_ticker == *pair.kalshi_market_ticker) {
                    all_pairs.push(pair);
                    new_count += 1;
                }
            }
        }
        
        // Also discover mention markets (incrementally)
        let mention_result = self.discover_mention_markets().await;
        for pair in mention_result.pairs {
            if !all_pairs.iter().any(|p| *p.kalshi_market_ticker == *pair.kalshi_market_ticker) {
                all_pairs.push(pair);
                new_count += 1;
            }
        }

        if new_count > 0 {
            info!("🆕 Found {} new market pairs", new_count);

            // Update cache
            let new_cache = DiscoveryCache::new(all_pairs.clone());
            if let Err(e) = Self::save_cache(&new_cache).await {
                warn!("Failed to update discovery cache: {}", e);
            } else {
                info!("💾 Updated cache with {} total pairs", all_pairs.len());
            }
        } else {
            info!("✅ No new markets found, using {} cached pairs", all_pairs.len());

            // Just update timestamp to extend TTL
            let refreshed_cache = DiscoveryCache::new(all_pairs.clone());
            let _ = Self::save_cache(&refreshed_cache).await;
        }

        DiscoveryResult {
            pairs: all_pairs,
            kalshi_events_found: new_count,
            poly_matches: new_count,
            poly_misses: 0,
            errors: vec![],
        }
    }
    
    /// Discover all market types for a single league (PARALLEL)
    /// If cache is provided, only discovers markets not already in cache
    async fn discover_league(&self, config: &LeagueConfig, cache: Option<&DiscoveryCache>) -> DiscoveryResult {
        info!("🔍 Discovering {} markets...", config.league_code);

        let market_types = [MarketType::Moneyline, MarketType::Spread, MarketType::Total, MarketType::Btts];

        // Parallel discovery across market types
        let type_futures: Vec<_> = market_types.iter()
            .filter_map(|market_type| {
                let series = self.get_series_for_type(config, *market_type)?;
                Some(self.discover_series(config, series, *market_type, cache))
            })
            .collect();

        let type_results = futures_util::future::join_all(type_futures).await;

        let mut result = DiscoveryResult::default();
        for (pairs_result, market_type) in type_results.into_iter().zip(market_types.iter()) {
            match pairs_result {
                Ok(pairs) => {
                    let count = pairs.len();
                    if count > 0 {
                        info!("  ✅ {} {}: {} pairs", config.league_code, market_type, count);
                    }
                    result.poly_matches += count;
                    result.pairs.extend(pairs);
                }
                Err(e) => {
                    result.errors.push(format!("{} {}: {}", config.league_code, market_type, e));
                }
            }
        }

        result
    }
    
    fn get_series_for_type(&self, config: &LeagueConfig, market_type: MarketType) -> Option<&'static str> {
        match market_type {
            MarketType::Moneyline => Some(config.kalshi_series_game),
            MarketType::Spread => config.kalshi_series_spread,
            MarketType::Total => config.kalshi_series_total,
            MarketType::Btts => config.kalshi_series_btts,
            MarketType::Mention => None,  // Mention markets are discovered separately
        }
    }
    
    /// Discover markets for a specific series (PARALLEL Kalshi + Gamma lookups)
    /// If cache is provided, skips markets already in cache
    async fn discover_series(
        &self,
        config: &LeagueConfig,
        series: &str,
        market_type: MarketType,
        cache: Option<&DiscoveryCache>,
    ) -> Result<Vec<MarketPair>> {
        // Fetch Kalshi events
        {
            let _permit = self.kalshi_semaphore.acquire().await.map_err(|e| anyhow::anyhow!("semaphore closed: {}", e))?;
            self.kalshi_limiter.until_ready().await;
        }
        let events = self.kalshi.get_events(series, 50).await?;

        // PHASE 2: Parallel market fetching 
        let kalshi = self.kalshi.clone();
        let limiter = self.kalshi_limiter.clone();
        let semaphore = self.kalshi_semaphore.clone();

        // Parse events first, filtering out unparseable ones
        let parsed_events: Vec<_> = events.into_iter()
            .filter_map(|event| {
                let parsed = match parse_kalshi_event_ticker(&event.event_ticker) {
                    Some(p) => p,
                    None => {
                        warn!("  ⚠️ Could not parse event ticker {}", event.event_ticker);
                        return None;
                    }
                };
                Some((parsed, event))
            })
            .collect();

        // Execute market fetches with GLOBAL concurrency limit
        let market_results: Vec<_> = stream::iter(parsed_events)
            .map(|(parsed, event)| {
                let kalshi = kalshi.clone();
                let limiter = limiter.clone();
                let semaphore = semaphore.clone();
                let event_ticker = event.event_ticker.clone();
                async move {
                    let _permit = semaphore.acquire().await.ok();
                    // rate limit
                    limiter.until_ready().await;
                    let markets_result = kalshi.get_markets(&event_ticker).await;
                    (parsed, Arc::new(event), markets_result)
                }
            })
            .buffer_unordered(KALSHI_GLOBAL_CONCURRENCY * 2)  // Allow some buffering, semaphore is the real limit
            .collect()
            .await;

        // Collect all (event, market) pairs
        let mut event_markets = Vec::with_capacity(market_results.len() * 3);
        for (parsed, event, markets_result) in market_results {
            match markets_result {
                Ok(markets) => {
                    for market in markets {
                        // Skip if already in cache
                        if let Some(c) = cache {
                            if c.has_ticker(&market.ticker) {
                                continue;
                            }
                        }
                        event_markets.push((parsed.clone(), event.clone(), market));
                    }
                }
                Err(e) => {
                    warn!("  ⚠️ Failed to get markets for {}: {}", event.event_ticker, e);
                }
            }
        }
        
        // Parallel Gamma lookups with semaphore
        let lookup_futures: Vec<_> = event_markets
            .into_iter()
            .map(|(parsed, event, market)| {
                let poly_slug = self.build_poly_slug(config.poly_prefix, &parsed, market_type, &market);
                
                GammaLookupTask {
                    event,
                    market,
                    poly_slug,
                    market_type,
                    league: config.league_code.to_string(),
                }
            })
            .collect();
        
        // Execute lookups in parallel 
        let pairs: Vec<MarketPair> = stream::iter(lookup_futures)
            .map(|task| {
                let gamma = self.gamma.clone();
                let semaphore = self.gamma_semaphore.clone();
                async move {
                    let _permit = semaphore.acquire().await.ok()?;
                    match gamma.lookup_market(&task.poly_slug).await {
                        Ok(Some((yes_token, no_token))) => {
                            let team_suffix = extract_team_suffix(&task.market.ticker);
                            Some(MarketPair {
                                pair_id: format!("{}-{}", task.poly_slug, task.market.ticker).into(),
                                league: task.league.into(),
                                market_type: task.market_type,
                                description: format!("{} - {}", task.event.title, task.market.title).into(),
                                kalshi_event_ticker: task.event.event_ticker.clone().into(),
                                kalshi_market_ticker: task.market.ticker.into(),
                                poly_slug: task.poly_slug.into(),
                                poly_yes_token: yes_token.into(),
                                poly_no_token: no_token.into(),
                                line_value: task.market.floor_strike,
                                team_suffix: team_suffix.map(|s| s.into()),
                            })
                        }
                        Ok(None) => None,
                        Err(e) => {
                            warn!("  ⚠️ Gamma lookup failed for {}: {}", task.poly_slug, e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(GAMMA_CONCURRENCY)
            .filter_map(|x| async { x })
            .collect()
            .await;
        
        Ok(pairs)
    }
    
    /// Build Polymarket slug from Kalshi event data
    fn build_poly_slug(
        &self,
        poly_prefix: &str,
        parsed: &ParsedKalshiTicker,
        market_type: MarketType,
        market: &KalshiMarket,
    ) -> String {
        // Convert Kalshi team codes to Polymarket codes using cache
        let poly_team1 = self.team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team1)
            .unwrap_or_else(|| parsed.team1.to_lowercase());
        let poly_team2 = self.team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team2)
            .unwrap_or_else(|| parsed.team2.to_lowercase());
        
        // Convert date from "25DEC27" to "2025-12-27"
        let date_str = kalshi_date_to_iso(&parsed.date);
        
        // Base slug: league-team1-team2-date
        let base = format!("{}-{}-{}-{}", poly_prefix, poly_team1, poly_team2, date_str);
        
        match market_type {
            MarketType::Moneyline => {
                if let Some(suffix) = extract_team_suffix(&market.ticker) {
                    if suffix.to_lowercase() == "tie" {
                        format!("{}-draw", base)
                    } else {
                        let poly_suffix = self.team_cache
                            .kalshi_to_poly(poly_prefix, &suffix)
                            .unwrap_or_else(|| suffix.to_lowercase());
                        format!("{}-{}", base, poly_suffix)
                    }
                } else {
                    base
                }
            }
            MarketType::Spread => {
                if let Some(floor) = market.floor_strike {
                    let floor_str = format!("{:.1}", floor).replace(".", "pt");
                    format!("{}-spread-{}", base, floor_str)
                } else {
                    format!("{}-spread", base)
                }
            }
            MarketType::Total => {
                if let Some(floor) = market.floor_strike {
                    let floor_str = format!("{:.1}", floor).replace(".", "pt");
                    format!("{}-total-{}", base, floor_str)
                } else {
                    format!("{}-total", base)
                }
            }
            MarketType::Btts => {
                format!("{}-btts", base)
            }
            MarketType::Mention => {
                // Mention markets don't use this method
                base
            }
        }
    }
    
    /// Discover mention/"say" markets across Polymarket and Kalshi
    /// These are markets about what someone will say at an event
    pub async fn discover_mention_markets(&self) -> DiscoveryResult {
        info!("🎤 Discovering mention markets...");
        
        let mut result = DiscoveryResult::default();
        
        // Step 1: Search Polymarket for "say" markets
        info!("  🔍 Searching Polymarket for mention markets...");
        let poly_markets = match self.gamma.search_mention_markets().await {
            Ok(markets) => {
                info!("  ✅ Found {} Polymarket mention markets", markets.len());
                markets
            }
            Err(e) => {
                result.errors.push(format!("Polymarket search failed: {}", e));
                warn!("  ❌ Polymarket search failed: {}", e);
                return result;
            }
        };
        
        if poly_markets.is_empty() {
            info!("  ℹ️ No Polymarket mention markets found");
            return result;
        }
        
        // Step 2: Parse Polymarket markets to extract metadata.
        // IMPORTANT: public-search does NOT include clobTokenIds, so we must follow up by slug
        // using GammaClient.lookup_market() to get (yes_token, no_token).
        let poly_candidates: Vec<(String, String, String, MentionMarketMeta)> = poly_markets
            .iter()
            .filter_map(|m| {
                let slug = m.slug.as_ref()?.to_string();
                let question = m.question.as_ref()?.to_string();
                let term = m.group_item_title.as_ref()?.to_string();
                let meta = extract_mention_metadata(&question, Some(&term))?;
                Some((slug, question, term, meta))
            })
            .collect();

        let parsed_poly: Vec<PolyMentionMarket> = stream::iter(poly_candidates.into_iter())
            .map(|(slug, question, term, meta)| {
                let gamma = self.gamma.clone();
                let semaphore = self.gamma_semaphore.clone();
                async move {
                    let _permit = semaphore.acquire().await.ok()?;
                    match gamma.lookup_market(&slug).await {
                        Ok(Some((yes_token, no_token))) => Some(PolyMentionMarket {
                            slug,
                            question,
                            yes_token,
                            no_token,
                            term,
                            meta,
                        }),
                        Ok(None) => None,
                        Err(e) => {
                            warn!("  ⚠️ Gamma lookup failed for {}: {}", slug, e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(GAMMA_CONCURRENCY)
            .filter_map(|x| async { x })
            .collect()
            .await;

        info!("  ✅ Parsed {} Polymarket mention markets with metadata", parsed_poly.len());
        
        // Step 3: Fetch Kalshi mention events
        info!("  🔍 Fetching Kalshi mention events...");
        let kalshi_events = {
            let _permit = self.kalshi_semaphore.acquire().await.ok();
            self.kalshi_limiter.until_ready().await;
            match self.kalshi.get_mention_events().await {
                Ok(events) => {
                    info!("  ✅ Found {} Kalshi mention events", events.len());
                    events
                }
                Err(e) => {
                    result.errors.push(format!("Kalshi events fetch failed: {}", e));
                    warn!("  ❌ Kalshi events fetch failed: {}", e);
                    return result;
                }
            }
        };
        
        if kalshi_events.is_empty() {
            info!("  ℹ️ No Kalshi mention events found");
            return result;
        }
        
        result.kalshi_events_found = kalshi_events.len();
        
        // Step 4: Fetch markets for each Kalshi event and build metadata
        let mut kalshi_mention_markets: Vec<(KalshiEvent, Vec<KalshiMarket>, MentionMarketMeta)> = Vec::new();
        
        for event in &kalshi_events {
            // Rate-limited fetch of markets
            {
                let _permit = self.kalshi_semaphore.acquire().await.ok();
                self.kalshi_limiter.until_ready().await;
            }
            
            let markets = match self.kalshi.get_markets(&event.event_ticker).await {
                Ok(m) => m,
                Err(e) => {
                    warn!("  ⚠️ Failed to get markets for {}: {}", event.event_ticker, e);
                    continue;
                }
            };
            
            // Parse event metadata
            if let Some(meta) = extract_mention_metadata(&event.title, None) {
                // Also try to extract date from event ticker
                let meta = MentionMarketMeta {
                    event_date: meta.event_date.or_else(|| extract_kalshi_mention_date(&event.event_ticker)),
                    ..meta
                };
                kalshi_mention_markets.push((event.clone(), markets, meta));
            }
        }
        
        info!("  ✅ Parsed {} Kalshi mention events with metadata", kalshi_mention_markets.len());
        
        // Step 5: Match Polymarket to Kalshi markets
        for poly in &parsed_poly {
            for (kalshi_event, kalshi_markets, kalshi_meta) in &kalshi_mention_markets {
                // Check if events match (same speaker + date)
                let match_score = match_mention_markets(&poly.meta, kalshi_event, kalshi_meta);
                
                if match_score < 0.5 {
                    continue; // Not a good enough match
                }
                
                info!("  🎯 Potential match: {} <-> {} (score: {:.2})", 
                      poly.question, kalshi_event.title, match_score);
                
                // Step 6: Find Kalshi market matching the single PM term
                let common_terms = find_common_terms(&[poly.term.clone()], kalshi_markets);

                for (term, _poly_idx, kalshi_ticker) in common_terms {
                    
                    // Create MarketPair for this term
                    let pair_id = format!("mention-{}-{}-{}", 
                        normalize_speaker(&poly.meta.who),
                        poly.meta.event_date.as_deref().unwrap_or("unknown"),
                        term.to_lowercase()
                    );
                    
                    let description = format!("{} - {} - {}", 
                        poly.meta.who,
                        poly.meta.event_name,
                        term
                    );
                    
                    let pair = MarketPair {
                        pair_id: pair_id.into(),
                        league: "mention".into(),
                        market_type: MarketType::Mention,
                        description: description.into(),
                        kalshi_event_ticker: kalshi_event.event_ticker.clone().into(),
                        kalshi_market_ticker: kalshi_ticker.into(),
                        poly_slug: poly.slug.clone().into(),
                        poly_yes_token: poly.yes_token.clone().into(),
                        poly_no_token: poly.no_token.clone().into(),
                        line_value: None,
                        team_suffix: Some(term.clone().into()),  // Store the term as team_suffix
                    };
                    
                    info!("  ✅ Matched mention market: {} @ {} <-> {}", 
                          term, pair.poly_slug, pair.kalshi_market_ticker);
                    
                    result.pairs.push(pair);
                    result.poly_matches += 1;
                }
            }
        }
        
        info!("🎤 Mention market discovery complete: {} pairs found", result.pairs.len());
        result
    }
}

// === Helpers ===

#[derive(Debug, Clone)]
struct ParsedKalshiTicker {
    date: String,  // "25DEC27"
    team1: String, // "CFC"
    team2: String, // "AVL"
}

/// Parse Kalshi event ticker like "KXEPLGAME-25DEC27CFCAVL" or "KXNCAAFGAME-25DEC27M-OHFRES"
fn parse_kalshi_event_ticker(ticker: &str) -> Option<ParsedKalshiTicker> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    // Handle two formats:
    // 1. "KXEPLGAME-25DEC27CFCAVL" - date+teams in parts[1]
    // 2. "KXNCAAFGAME-25DEC27M-OHFRES" - date in parts[1], teams in parts[2]
    let (date, teams_part) = if parts.len() >= 3 && parts[2].len() >= 4 {
        // Format 2: 3-part ticker with separate teams section
        // parts[1] is like "25DEC27M" (date + optional suffix)
        let date_part = parts[1];
        let date = if date_part.len() >= 7 {
            date_part[..7].to_uppercase()
        } else {
            return None;
        };
        (date, parts[2])
    } else {
        // Format 1: 2-part ticker with combined date+teams
        let date_teams = parts[1];
        // Minimum: 7 (date) + 2 + 2 (min team codes) = 11
        if date_teams.len() < 11 {
            return None;
        }
        let date = date_teams[..7].to_uppercase();
        let teams = &date_teams[7..];
        (date, teams)
    };

    // Split team codes - try to find the best split point
    // Team codes range from 2-4 chars (e.g., OM, CFC, FRES)
    let (team1, team2) = split_team_codes(teams_part);

    Some(ParsedKalshiTicker { date, team1, team2 })
}

/// Split a combined team string into two team codes
/// Tries multiple split strategies based on string length
fn split_team_codes(teams: &str) -> (String, String) {
    let len = teams.len();

    // For 6 chars, could be 3+3, 2+4, or 4+2
    // For 5 chars, could be 2+3 or 3+2
    // For 4 chars, must be 2+2
    // For 7 chars, could be 3+4 or 4+3
    // For 8 chars, could be 4+4, 3+5, 5+3

    match len {
        4 => (teams[..2].to_uppercase(), teams[2..].to_uppercase()),
        5 => {
            // Prefer 2+3 (common for OM+ASM, OL+PSG)
            (teams[..2].to_uppercase(), teams[2..].to_uppercase())
        }
        6 => {
            // Check if it looks like 2+4 pattern (e.g., OHFRES = OH+FRES)
            // Common 2-letter codes: OM, OL, OH, SF, LA, NY, KC, TB, etc.
            let first_two = &teams[..2].to_uppercase();
            if is_likely_two_letter_code(first_two) {
                (first_two.clone(), teams[2..].to_uppercase())
            } else {
                // Default to 3+3
                (teams[..3].to_uppercase(), teams[3..].to_uppercase())
            }
        }
        7 => {
            // Could be 3+4 or 4+3 - prefer 3+4
            (teams[..3].to_uppercase(), teams[3..].to_uppercase())
        }
        _ if len >= 8 => {
            // 4+4 or longer
            (teams[..4].to_uppercase(), teams[4..].to_uppercase())
        }
        _ => {
            let mid = len / 2;
            (teams[..mid].to_uppercase(), teams[mid..].to_uppercase())
        }
    }
}

/// Check if a 2-letter code is a known/likely team abbreviation
fn is_likely_two_letter_code(code: &str) -> bool {
    matches!(
        code,
        // European football (Ligue 1, etc.)
        "OM" | "OL" | "FC" |
        // US sports common abbreviations
        "OH" | "SF" | "LA" | "NY" | "KC" | "TB" | "GB" | "NE" | "NO" | "LV" |
        // Generic short codes
        "BC" | "SC" | "AC" | "AS" | "US"
    )
}

/// Convert Kalshi date "25DEC27" to ISO "2025-12-27"
fn kalshi_date_to_iso(kalshi_date: &str) -> String {
    if kalshi_date.len() != 7 {
        return kalshi_date.to_string();
    }
    
    let year = format!("20{}", &kalshi_date[..2]);
    let month = match &kalshi_date[2..5].to_uppercase()[..] {
        "JAN" => "01", "FEB" => "02", "MAR" => "03", "APR" => "04",
        "MAY" => "05", "JUN" => "06", "JUL" => "07", "AUG" => "08",
        "SEP" => "09", "OCT" => "10", "NOV" => "11", "DEC" => "12",
        _ => "01",
    };
    let day = &kalshi_date[5..7];
    
    format!("{}-{}-{}", year, month, day)
}

/// Extract team suffix from market ticker (e.g., "KXEPLGAME-25DEC27CFCAVL-CFC" -> "CFC")
fn extract_team_suffix(ticker: &str) -> Option<String> {
    let mut splits = ticker.splitn(3, '-');
    splits.next()?; // series
    splits.next()?; // event
    splits.next().map(|s| s.to_uppercase())
}

// === Mention Market Helpers ===

/// Metadata extracted from a mention/"say" market title
#[derive(Debug, Clone)]
pub struct MentionMarketMeta {
    /// The speaker (e.g., "Trump", "Biden")
    pub who: String,
    /// The event name (e.g., "Address to the Nation", "State of the Union")
    pub event_name: String,
    /// The event date in various formats
    pub event_date: Option<String>,
    /// Individual terms/outcomes being bet on (e.g., ["Nuclear", "Tariff", "China"])
    #[allow(dead_code)]
    pub terms: Vec<String>,
}

/// Extract mention market metadata from a market title
/// Parses titles like "What will Trump say during the address on December 17?"
/// For Polymarket search results, term is the specific word being bet on
pub fn extract_mention_metadata(title: &str, term: Option<&str>) -> Option<MentionMarketMeta> {
    let title_lower = title.to_lowercase();
    
    // Must contain "say" to be a mention market
    if !title_lower.contains("say") {
        return None;
    }
    
    // Extract the speaker (who)
    let who = extract_speaker(&title_lower)?;
    
    // Extract the event name
    let event_name = extract_event_name(&title_lower);
    
    // Extract the date
    let event_date = extract_mention_date(title);
    
    // The term is the specific word being bet on (e.g., "Nuclear")
    let terms = term.map(|t| vec![t.to_string()]).unwrap_or_default();
    
    Some(MentionMarketMeta {
        who,
        event_name,
        event_date,
        terms,
    })
}

/// Extract speaker name from market title
/// Looks for patterns like "will X say" or "X say"
fn extract_speaker(title: &str) -> Option<String> {
    // Common patterns:
    // "Will Trump say..."
    // "What will Trump say..."
    // "Will President Trump say..."
    
    // Known speakers to look for (case-insensitive matching)
    let known_speakers = [
        "trump", "biden", "harris", "obama", "putin", "zelensky",
        "xi", "musk", "powell", "yellen", "lagarde", "macron",
        "modi", "netanyahu", "scholz", "sunak", "starmer",
        "vance", "pelosi", "schumer", "mccarthy", "johnson",
    ];
    
    let title_lower = title.to_lowercase();
    
    // First try to find known speakers
    for speaker in &known_speakers {
        if title_lower.contains(speaker) {
            // Capitalize first letter
            let mut chars = speaker.chars();
            let capitalized = match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().chain(chars).collect(),
            };
            return Some(capitalized);
        }
    }
    
    // Try pattern matching for "will X say" - extract word between "will" and "say"
    if let Some(will_idx) = title_lower.find("will ") {
        let after_will = &title_lower[will_idx + 5..];
        if let Some(say_idx) = after_will.find(" say") {
            let between = after_will[..say_idx].trim();
            // Could be "Trump" or "President Trump" - take last word
            let speaker_word = between.split_whitespace().last()?;
            // Skip common words
            if !["the", "a", "he", "she", "they", "it"].contains(&speaker_word) {
                let mut chars = speaker_word.chars();
                let capitalized = match chars.next() {
                    None => String::new(),
                    Some(c) => c.to_uppercase().chain(chars).collect(),
                };
                return Some(capitalized);
            }
        }
    }
    
    None
}

/// Extract event name from market title
fn extract_event_name(title: &str) -> String {
    // Common event patterns to look for
    let event_patterns = [
        ("address to the nation", "Address to the Nation"),
        ("addressing the nation", "Address to the Nation"),
        ("state of the union", "State of the Union"),
        ("press conference", "Press Conference"),
        ("speech", "Speech"),
        ("debate", "Debate"),
        ("interview", "Interview"),
        ("rally", "Rally"),
        ("briefing", "Briefing"),
        ("announcement", "Announcement"),
        ("inauguration", "Inauguration"),
        ("address", "Address"),
    ];
    
    let title_lower = title.to_lowercase();
    
    for (pattern, name) in &event_patterns {
        if title_lower.contains(pattern) {
            return name.to_string();
        }
    }
    
    // Default to generic "Speech/Address"
    "Speech".to_string()
}

/// Extract date from mention market title
/// Handles formats like "December 17", "Dec 17", "12/17", "2025-12-17"
fn extract_mention_date(title: &str) -> Option<String> {
    let title_lower = title.to_lowercase();
    
    // Month names and their numbers
    let months = [
        ("january", "01"), ("february", "02"), ("march", "03"), ("april", "04"),
        ("may", "05"), ("june", "06"), ("july", "07"), ("august", "08"),
        ("september", "09"), ("october", "10"), ("november", "11"), ("december", "12"),
        ("jan", "01"), ("feb", "02"), ("mar", "03"), ("apr", "04"),
        ("jun", "06"), ("jul", "07"), ("aug", "08"),
        ("sep", "09"), ("oct", "10"), ("nov", "11"), ("dec", "12"),
    ];
    
    // Try to find month name followed by day
    for (month_name, month_num) in &months {
        if let Some(idx) = title_lower.find(month_name) {
            // Look for a day number after the month
            // Slice the SAME string we searched in so indexes are valid even with Unicode.
            let after_month = &title_lower[idx + month_name.len()..];
            let after_month_trimmed = after_month.trim_start();
            
            // Extract day number
            let day_str: String = after_month_trimmed
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            
            if !day_str.is_empty() {
                if let Ok(day) = day_str.parse::<u32>() {
                    if day >= 1 && day <= 31 {
                        // Assume current year or next year
                        let year = 2025; // TODO: derive from context
                        return Some(format!("{}-{}-{:02}", year, month_num, day));
                    }
                }
            }
        }
    }
    
    // Try ISO format (YYYY-MM-DD) manually without panicking on Unicode.
    // Look for pattern like "2025-12-17"
    let bytes = title.as_bytes();
    if bytes.len() >= 10 {
        for i in 0..=(bytes.len() - 10) {
            let b = &bytes[i..i + 10];
            let is_digit = |c: u8| c.is_ascii_digit();
            if is_digit(b[0]) && is_digit(b[1]) && is_digit(b[2]) && is_digit(b[3])
                && b[4] == b'-'
                && is_digit(b[5]) && is_digit(b[6])
                && b[7] == b'-'
                && is_digit(b[8]) && is_digit(b[9])
            {
                if title.is_char_boundary(i) && title.is_char_boundary(i + 10) {
                    return Some(title[i..i + 10].to_string());
                }
            }
        }
    }
    
    None
}


/// Extract date from Kalshi event ticker for mention markets
/// Format: "KXSAY-25DEC17" or similar
pub fn extract_kalshi_mention_date(event_ticker: &str) -> Option<String> {
    let parts: Vec<&str> = event_ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }
    
    // The date part should be like "25DEC17" (YYMMMDD)
    let date_part = parts[1];
    if date_part.len() >= 7 {
        // Try to parse as YYMMMDD format
        let year = format!("20{}", &date_part[..2]);
        let month_str = &date_part[2..5].to_uppercase();
        let month = match month_str.as_str() {
            "JAN" => "01", "FEB" => "02", "MAR" => "03", "APR" => "04",
            "MAY" => "05", "JUN" => "06", "JUL" => "07", "AUG" => "08",
            "SEP" => "09", "OCT" => "10", "NOV" => "11", "DEC" => "12",
            _ => return None,
        };
        let day = &date_part[5..7];
        return Some(format!("{}-{}-{}", year, month, day));
    }
    
    None
}

/// Normalize speaker name for matching
pub fn normalize_speaker(name: &str) -> String {
    name.to_lowercase()
        .trim()
        .replace("president ", "")
        .replace("former ", "")
        .replace("vice ", "")
        .to_string()
}

/// Normalize date string for comparison
pub fn normalize_date(date: &str) -> String {
    // Remove any non-alphanumeric characters and lowercase
    date.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-')
        .collect::<String>()
        .to_lowercase()
}

// === Mention Market Matching ===

/// Parsed Polymarket mention market for matching
#[derive(Debug, Clone)]
pub struct PolyMentionMarket {
    pub slug: String,
    pub question: String,
    pub yes_token: String,
    pub no_token: String,
    pub term: String,
    pub meta: MentionMarketMeta,
}

/// Check if two mention markets are about the same event
/// Returns match score (0.0 = no match, 1.0 = perfect match)
pub fn match_mention_markets(
    poly: &MentionMarketMeta,
    kalshi_event: &KalshiEvent,
    kalshi_meta: &MentionMarketMeta,
) -> f32 {
    let mut score: f32 = 0.0;
    
    // Speaker must match (required)
    let poly_speaker = normalize_speaker(&poly.who);
    let kalshi_speaker = normalize_speaker(&kalshi_meta.who);
    
    if poly_speaker != kalshi_speaker {
        return 0.0; // No match if speakers differ
    }
    score += 0.4; // Speaker match is important
    
    // Date matching (highly important)
    match (&poly.event_date, &kalshi_meta.event_date) {
        (Some(poly_date), Some(kalshi_date)) => {
            if normalize_date(poly_date) == normalize_date(kalshi_date) {
                score += 0.4; // Exact date match
            } else {
                // Check if dates are within 1 day (timezone issues)
                if dates_within_days(poly_date, kalshi_date, 1) {
                    score += 0.3; // Close date match
                } else {
                    return 0.0; // Dates too different
                }
            }
        }
        _ => {
            // If one or both dates missing, try to match by event name
            score += 0.1;
        }
    }
    
    // Event name similarity (bonus)
    let poly_event = poly.event_name.to_lowercase();
    let kalshi_event_title = kalshi_event.title.to_lowercase();
    
    if kalshi_event_title.contains(&poly_event) || poly_event.contains(&kalshi_event.title.to_lowercase()) {
        score += 0.2;
    } else {
        // Check for common event keywords
        let event_keywords = ["address", "speech", "conference", "debate", "rally", "interview"];
        for keyword in &event_keywords {
            if poly_event.contains(keyword) && kalshi_event_title.contains(keyword) {
                score += 0.1;
                break;
            }
        }
    }
    
    score.min(1.0)
}

/// Find common terms between Polymarket and Kalshi markets
/// Returns list of (term, poly_index) pairs
pub fn find_common_terms(
    poly_outcomes: &[String],
    kalshi_markets: &[KalshiMarket],
) -> Vec<(String, usize, String)> {
    let mut matches = Vec::new();
    
    for (poly_idx, poly_outcome) in poly_outcomes.iter().enumerate() {
        let poly_term = poly_outcome.to_lowercase().trim().to_string();
        
        for kalshi_market in kalshi_markets {
            let kalshi_term = kalshi_market.title.to_lowercase().trim().to_string();
            
            // Exact match (case-insensitive)
            if poly_term == kalshi_term {
                matches.push((poly_outcome.clone(), poly_idx, kalshi_market.ticker.clone()));
                continue;
            }
            
            // Check if one contains the other (for partial matches like "Nuclear" vs "Nuclear Weapons")
            if poly_term.contains(&kalshi_term) || kalshi_term.contains(&poly_term) {
                // Only match if one is a substring and the difference is small
                let len_diff = (poly_term.len() as i32 - kalshi_term.len() as i32).abs();
                if len_diff <= 5 {
                    matches.push((poly_outcome.clone(), poly_idx, kalshi_market.ticker.clone()));
                }
            }
        }
    }
    
    matches
}

/// Check if two dates are within N days of each other
fn dates_within_days(date1: &str, date2: &str, max_days: i32) -> bool {
    // Parse ISO dates (YYYY-MM-DD)
    let parse_date = |s: &str| -> Option<(i32, u32, u32)> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        let year: i32 = parts[0].parse().ok()?;
        let month: u32 = parts[1].parse().ok()?;
        let day: u32 = parts[2].parse().ok()?;
        Some((year, month, day))
    };
    
    let d1 = match parse_date(date1) {
        Some(d) => d,
        None => return false,
    };
    let d2 = match parse_date(date2) {
        Some(d) => d,
        None => return false,
    };
    
    // Simple day-of-year comparison (ignoring year boundary edge cases for now)
    let days1 = d1.0 * 365 + d1.1 as i32 * 31 + d1.2 as i32;
    let days2 = d2.0 * 365 + d2.1 as i32 * 31 + d2.2 as i32;
    
    (days1 - days2).abs() <= max_days
}

/// Get Polymarket token IDs for a specific outcome index
/// Returns (yes_token, no_token) for binary markets
#[allow(dead_code)]
pub fn get_poly_tokens_for_outcome(
    clob_token_ids: &[String],
    outcome_index: usize,
) -> Option<(String, String)> {
    // Polymarket token layout for multi-outcome markets:
    // Each outcome has 2 tokens: YES and NO
    // So outcome 0 = tokens[0] (YES), tokens[1] (NO)
    // outcome 1 = tokens[2] (YES), tokens[3] (NO)
    // etc.
    let yes_idx = outcome_index * 2;
    let no_idx = yes_idx + 1;
    
    if no_idx < clob_token_ids.len() {
        Some((clob_token_ids[yes_idx].clone(), clob_token_ids[no_idx].clone()))
    } else if clob_token_ids.len() >= 2 {
        // Fallback: maybe it's a simple binary market
        Some((clob_token_ids[0].clone(), clob_token_ids[1].clone()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_kalshi_ticker() {
        let parsed = parse_kalshi_event_ticker("KXEPLGAME-25DEC27CFCAVL").unwrap();
        assert_eq!(parsed.date, "25DEC27");
        assert_eq!(parsed.team1, "CFC");
        assert_eq!(parsed.team2, "AVL");
    }
    
    #[test]
    fn test_kalshi_date_to_iso() {
        assert_eq!(kalshi_date_to_iso("25DEC27"), "2025-12-27");
        assert_eq!(kalshi_date_to_iso("25JAN01"), "2025-01-01");
    }
    
    // === Mention Market Tests ===
    
    #[test]
    fn test_extract_speaker() {
        assert_eq!(extract_speaker("what will trump say"), Some("Trump".to_string()));
        assert_eq!(extract_speaker("Will Biden say something"), Some("Biden".to_string()));
        assert_eq!(extract_speaker("what will president trump say"), Some("Trump".to_string()));
        // Test actual PM question format
        assert_eq!(
            extract_speaker("will trump say \"nuclear\" while addressing the nation on december 17?"),
            Some("Trump".to_string())
        );
        assert_eq!(
            extract_speaker("will trump say \"isis\" or \"syria\" while addressing the nation on december 17?"),
            Some("Trump".to_string())
        );
    }
    
    #[test]
    fn test_extract_event_name() {
        assert_eq!(extract_event_name("during the address to the nation"), "Address to the Nation");
        assert_eq!(extract_event_name("at the press conference"), "Press Conference");
        assert_eq!(extract_event_name("during his speech"), "Speech");
    }
    
    #[test]
    fn test_extract_mention_date() {
        assert_eq!(extract_mention_date("on December 17"), Some("2025-12-17".to_string()));
        assert_eq!(extract_mention_date("on Dec 5"), Some("2025-12-05".to_string()));
        assert_eq!(extract_mention_date("on January 20"), Some("2025-01-20".to_string()));
        assert_eq!(extract_mention_date("on 2025-12-17"), Some("2025-12-17".to_string()));
    }
    
    #[test]
    fn test_extract_mention_metadata() {
        let meta = extract_mention_metadata(
            "What will Trump say during the address on December 17?",
            Some(r#"["Nuclear", "Tariff", "China"]"#)
        ).unwrap();
        
        assert_eq!(meta.who, "Trump");
        assert_eq!(meta.event_name, "Address");
        assert_eq!(meta.event_date, Some("2025-12-17".to_string()));
        assert_eq!(meta.terms, vec!["Nuclear", "Tariff", "China"]);
    }
    
    #[test]
    fn test_extract_kalshi_mention_date() {
        assert_eq!(extract_kalshi_mention_date("KXSAY-25DEC17"), Some("2025-12-17".to_string()));
        assert_eq!(extract_kalshi_mention_date("KXTRUMPSAY-25JAN20"), Some("2025-01-20".to_string()));
    }
    
    #[test]
    fn test_normalize_speaker() {
        assert_eq!(normalize_speaker("President Trump"), "trump");
        assert_eq!(normalize_speaker("BIDEN"), "biden");
        assert_eq!(normalize_speaker("  Harris  "), "harris");
    }
    
    #[test]
    fn test_dates_within_days() {
        assert!(dates_within_days("2025-12-17", "2025-12-17", 0));
        assert!(dates_within_days("2025-12-17", "2025-12-18", 1));
        assert!(!dates_within_days("2025-12-17", "2025-12-20", 1));
    }
    
    #[test]
    fn test_get_poly_tokens_for_outcome() {
        let tokens = vec![
            "token0-yes".to_string(), "token0-no".to_string(),
            "token1-yes".to_string(), "token1-no".to_string(),
        ];
        
        let (yes, no) = get_poly_tokens_for_outcome(&tokens, 0).unwrap();
        assert_eq!(yes, "token0-yes");
        assert_eq!(no, "token0-no");
        
        let (yes, no) = get_poly_tokens_for_outcome(&tokens, 1).unwrap();
        assert_eq!(yes, "token1-yes");
        assert_eq!(no, "token1-no");
    }
}
