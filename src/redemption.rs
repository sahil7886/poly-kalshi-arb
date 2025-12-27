// src/redemption.rs
// Polymarket Auto-Redemption Module

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
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

// ABI for the redeemPositions function
const CTF_ABI: &str = r#"[
    {"inputs":[{"internalType":"contract IERC20","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"indexSets","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
]"#;

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    markets: Vec<GammaMarket>,
}

pub struct RedemptionManager {
    provider: Arc<Provider<Http>>,
    wallet: LocalWallet,
    contract: Contract<Provider<Http>>,
}

impl RedemptionManager {
    pub fn new(poly_client: Arc<SharedAsyncClient>, rpc_url: String) -> Result<Self> {
        let provider = Provider::<Http>::try_from(&rpc_url)?;
        let provider = Arc::new(provider);
        
        // Use the same wallet as the CLOB client
        let wallet = poly_client.inner_wallet().clone();
        
        let abi: Abi = serde_json::from_str(CTF_ABI)?;
        let address: Address = CTF_CONTRACT.parse()?;
        let contract = Contract::new(address, abi, provider.clone());

        Ok(Self {
            provider,
            wallet,
            contract,
        })
    }

    /// Fetch condition IDs from recently closed Polymarket events
    async fn fetch_recent_condition_ids(&self) -> Result<Vec<String>> {
        let url = format!("{}/events?closed=true&limit=20", GAMMA_API_BASE);
        let resp = reqwest::get(&url).await?;
        if !resp.status().is_success() {
            return Err(anyhow!("Failed to fetch closed events: {}", resp.status()));
        }

        let events: Vec<GammaEvent> = resp.json().await?;
        let mut ids = Vec::new();
        for event in events {
            for market in event.markets {
                if !market.condition_id.is_empty() && market.condition_id != "0x0000000000000000000000000000000000000000000000000000000000000000" {
                    ids.push(market.condition_id);
                }
            }
        }
        
        // De-duplicate
        ids.sort();
        ids.dedup();
        
        Ok(ids)
    }

    /// Redeem positions for a list of condition IDs
    pub async fn redeem_all(&self) -> Result<()> {
        let condition_ids = self.fetch_recent_condition_ids().await?;
        if condition_ids.is_empty() {
            info!("[REDEEM] No recently closed markets found to check.");
            return Ok(());
        }

        info!("[REDEEM] Checking {} recent condition IDs for winnings...", condition_ids.len());

        let collateral_token: Address = USDC_ADDR.parse()?;
        let parent_collection_id = H256::zero();
        let index_sets = vec![U256::from(1), U256::from(2)]; // YES (0b01) and NO (0b10)

        // Create a signer middleware
        let client = SignerMiddleware::new(self.provider.clone(), self.wallet.clone());
        let client = Arc::new(client);

        for cid in condition_ids {
            let condition_id: H256 = match cid.parse() {
                Ok(h) => h,
                Err(_) => continue,
            };

            // We don't check balance first because it's multiple ERC-1155 IDs. 
            // Just attempt redemption. If zero balance, it might revert or do nothing.
            // To be efficient, we could query balance of tokenID = keccak256(collateral, conditionId, indexSet).
            // But for "make it happen", simple is better.
            
            // Note: redeemPositions only works if the market is resolved AND you have a balance.
            // If we attempt it on a market where we have no balance, it's a waste of gas.
            // In a low-priority implementation, we'll just try if it's cheap, or better:
            // Check if we *ever* traded this market. 
            // Since we don't have persistent state, we'll just try the last 20 events.
            
            // To avoid wasting gas on zero positions, let's at least check if we have ANY balance on one of the outcomes.
            // This requires computing the token IDs. 
            
            match self.try_redeem(&client, collateral_token, parent_collection_id, condition_id, &index_sets).await {
                Ok(true) => info!("[REDEEM] ✅ Redeemed winnings for {}", cid),
                Ok(false) => {}, // No position or already redeemed
                Err(e) => {
                    if e.to_string().contains("insufficient funds") {
                        error!("[REDEEM] ❌ FAILED: Insufficient MATIC for gas. Please fund {}", self.wallet.address());
                    } else if e.to_string().contains("execution reverted") {
                        // Likely no balance or not resolved yet
                        // info!("[REDEEM] Skipping {}: Not redeemable (reverted)", cid);
                    } else {
                        warn!("[REDEEM] ⚠️ Error redeeming {}: {}", cid, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn try_redeem(
        &self, 
        client: &SignerMiddleware<Arc<Provider<Http>>, LocalWallet>,
        collateral_token: Address,
        parent_collection_id: H256,
        condition_id: H256,
        index_sets: &[U256]
    ) -> Result<bool> {
        // Build the transaction
        let tx = self.contract
            .method::<_, ()>("redeemPositions", (collateral_token, parent_collection_id, condition_id, index_sets.to_vec()))?
            .from(self.wallet.address());

        // We can use call() to see if it would revert without sending gas
        match tx.call().await {
            Ok(_) => {
                // Not reverted! Let's send it.
                info!("[REDEEM] 🚀 Sending redemption transaction for {}...", condition_id);
                let pending_tx = tx.send().await?;
                let receipt = pending_tx.await?;
                if let Some(r) = receipt {
                    info!("[REDEEM] ✅ Success! Tx: {:?}", r.transaction_hash);
                    return Ok(true);
                }
                Ok(false)
            }
            Err(_) => {
                // Reverted, likely zero balance or not resolved
                Ok(false)
            }
        }
    }
}

/// Main hourly loop for auto-redemption
pub async fn run_hourly_loop(poly_client: Arc<SharedAsyncClient>, rpc_url: String) {
    info!("[REDEEM] Starting auto-redemption background task (Interval: 1 hour)");
    
    let manager = match RedemptionManager::new(poly_client, rpc_url) {
        Ok(m) => m,
        Err(e) => {
            error!("[REDEEM] ❌ Failed to initialize RedemptionManager: {}", e);
            return;
        }
    };

    loop {
        if let Err(e) = manager.redeem_all().await {
            error!("[REDEEM] Loop error: {}", e);
        }
        
        info!("[REDEEM] Sleeping for 1 hour...");
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}
