// src/redemption.rs
// Polymarket Auto-Redemption Module
// Uses Polymarket Data API to find redeemable positions

use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, anyhow};
use ethers::abi::Abi;
use ethers::prelude::*;
use serde::Deserialize;
use tracing::{info, warn, error};
use crate::polymarket_clob::SharedAsyncClient;

// Polymarket Mainnet Addresses
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_ADDR: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const POLYMARKET_DATA_API: &str = "https://data-api.polymarket.com";

// ABI for the redeemPositions function
const CTF_ABI: &str = r#"[
    {"inputs":[{"internalType":"contract IERC20","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"indexSets","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
]"#;

/// Position from Polymarket Data API /positions endpoint
#[derive(Debug, Deserialize)]
struct RedeemablePosition {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[allow(dead_code)]
    asset: String, // token ID
    #[allow(dead_code)]
    size: f64,
}

pub struct RedemptionManager {
    provider: Arc<Provider<Http>>,
    wallet: LocalWallet,
    contract: Contract<Provider<Http>>,
    funder: String, // POLY_FUNDER - the wallet that owns positions
    http: reqwest::Client,
}

impl RedemptionManager {
    pub fn new(poly_client: Arc<SharedAsyncClient>, rpc_url: String, funder: String) -> Result<Self> {
        let provider = Provider::<Http>::try_from(&rpc_url)?;
        let provider = Arc::new(provider);
        
        // Use the same wallet as the CLOB client (signer)
        let wallet = poly_client.inner_wallet().clone();
        
        let abi: Abi = serde_json::from_str(CTF_ABI)?;
        let address: Address = CTF_CONTRACT.parse()?;
        let contract = Contract::new(address, abi, provider.clone());

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            provider,
            wallet,
            contract,
            funder,
            http,
        })
    }

    /// Fetch redeemable positions from Polymarket Data API
    /// Only returns positions where user has a winning balance ready to redeem
    async fn fetch_redeemable_positions(&self) -> Result<Vec<RedeemablePosition>> {
        let url = format!(
            "{}/positions?user={}&redeemable=true",
            POLYMARKET_DATA_API, self.funder
        );
        
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to fetch positions: {} - {}", status, body));
        }

        let positions: Vec<RedeemablePosition> = resp.json().await?;
        Ok(positions)
    }

    /// Redeem all redeemable positions
    pub async fn redeem_all(&self) -> Result<()> {
        let positions = self.fetch_redeemable_positions().await?;
        
        if positions.is_empty() {
            info!("[REDEEM] No redeemable positions found.");
            return Ok(());
        }

        info!("[REDEEM] Found {} redeemable positions!", positions.len());

        let collateral_token: Address = USDC_ADDR.parse()?;
        let parent_collection_id = H256::zero();
        let index_sets = vec![U256::from(1), U256::from(2)]; // YES (0b01) and NO (0b10)

        // Create a signer middleware
        let client = SignerMiddleware::new(self.provider.clone(), self.wallet.clone());
        let client = Arc::new(client);

        for position in positions {
            let condition_id: H256 = match position.condition_id.parse() {
                Ok(h) => h,
                Err(_) => {
                    warn!("[REDEEM] Invalid conditionId: {}", position.condition_id);
                    continue;
                }
            };

            match self.try_redeem(&client, collateral_token, parent_collection_id, condition_id, &index_sets).await {
                Ok(true) => info!("[REDEEM] ✅ Redeemed: {}", &position.condition_id[..20]),
                Ok(false) => warn!("[REDEEM] ⚠️ Redemption returned false for {}", &position.condition_id[..20]),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("insufficient funds") || err_str.contains("Insufficient MATIC") {
                        error!("[REDEEM] ❌ Insufficient MATIC for gas. Fund: {}", self.wallet.address());
                        break; // No point continuing without gas
                    } else {
                        warn!("[REDEEM] ⚠️ Error for {}: {}", &position.condition_id[..20], e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn try_redeem(
        &self, 
        _client: &SignerMiddleware<Arc<Provider<Http>>, LocalWallet>,
        collateral_token: Address,
        parent_collection_id: H256,
        condition_id: H256,
        index_sets: &[U256]
    ) -> Result<bool> {
        // Build the transaction
        let tx = self.contract
            .method::<_, ()>("redeemPositions", (collateral_token, parent_collection_id, condition_id, index_sets.to_vec()))?
            .from(self.wallet.address());

        // Use call() to check if it would succeed without sending gas
        match tx.call().await {
            Ok(_) => {
                // Not reverted - proceed with redemption
                info!("[REDEEM] 🚀 Sending redemption tx for {}...", condition_id);
                let pending_tx = tx.send().await?;
                let receipt = pending_tx.await?;
                if let Some(r) = receipt {
                    info!("[REDEEM] ✅ Success! Tx: {:?}", r.transaction_hash);
                    return Ok(true);
                }
                Ok(false)
            }
            Err(e) => {
                // Reverted - this shouldn't happen if API said it's redeemable
                warn!("[REDEEM] ⚠️ Call reverted (unexpected): {}", e);
                Ok(false)
            }
        }
    }
}

/// Main loop for auto-redemption (runs every 15 minutes)
pub async fn run_hourly_loop(
    poly_client: Arc<SharedAsyncClient>,
    rpc_url: String,
    funder: String,
    engine: Option<Arc<crate::execution::ExecutionEngine>>,
) {
    info!("[REDEEM] Auto-redemption started (checks every 15 minutes)");

    let manager = match RedemptionManager::new(poly_client, rpc_url, funder) {
        Ok(m) => m,
        Err(e) => {
            error!("[REDEEM] ❌ Failed to initialize: {}", e);
            return;
        }
    };

    // Wait 15 minutes before first check (assume everything redeemed at startup)
    info!("[REDEEM] First check in 15 minutes...");
    tokio::time::sleep(Duration::from_secs(900)).await;

    loop {
        if let Err(e) = manager.redeem_all().await {
            error!("[REDEEM] Loop error: {}", e);
        }

        // Reset exposure counters after redemption (positions are now settled)
        if let Some(ref eng) = engine {
            eng.reset_exposure();
        }

        info!("[REDEEM] Next check in 15 minutes...");
        tokio::time::sleep(Duration::from_secs(900)).await;
    }
}
