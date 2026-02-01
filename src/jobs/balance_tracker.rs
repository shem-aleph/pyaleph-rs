//! Balance Tracker Job
//!
//! Tracks ALEPH token balances across chains for cost calculation
//! and resource cleanup when balance drops.
//!
//! Reference: aleph/jobs/balance_tracker.py

use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn, error};
use sqlx::PgPool;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    types::Address as EthAddress,
};

use crate::config::Config;
use crate::types::Chain;
use crate::services::Metrics;

/// ALEPH token decimals
const ALEPH_DECIMALS: u32 = 18;

/// ERC20 balanceOf function selector
const BALANCE_OF_SELECTOR: &str = "70a08231";

/// Balance update record
#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub address: String,
    pub chain: Chain,
    pub balance: Decimal,
    pub previous_balance: Option<Decimal>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Balance change event
#[derive(Debug, Clone)]
pub enum BalanceChange {
    Increased { address: String, chain: Chain, amount: Decimal },
    Decreased { address: String, chain: Chain, amount: Decimal },
    BelowThreshold { address: String, chain: Chain, balance: Decimal },
}

/// Run the balance tracker job
pub async fn run(
    db: PgPool,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) {
    let update_interval = config.aleph.balances.update_interval;
    let mut ticker = interval(Duration::from_secs(update_interval));
    
    info!("Balance tracker started (interval: {}s)", update_interval);
    
    loop {
        ticker.tick().await;
        
        match update_balances(&db, &config, &metrics).await {
            Ok(count) => {
                if count > 0 {
                    debug!("Updated {} balances", count);
                }
            }
            Err(e) => {
                error!("Balance tracking error: {}", e);
            }
        }
    }
}

/// Update all tracked balances
async fn update_balances(
    db: &PgPool,
    config: &Config,
    metrics: &Metrics,
) -> Result<u32, BalanceError> {
    // Get addresses that need balance tracking
    // (addresses with active programs, instances, or storage)
    let addresses: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT address FROM (
            SELECT owner as address FROM programs
            UNION
            SELECT owner as address FROM instances
            UNION
            SELECT owner as address FROM file_pins
        ) active_addresses
        LIMIT 1000
        "#
    )
    .fetch_all(db)
    .await
    .map_err(|e| BalanceError::Database(e.to_string()))?;
    
    if addresses.is_empty() {
        return Ok(0);
    }
    
    let mut update_count = 0u32;
    let mut changes: Vec<BalanceChange> = Vec::new();
    
    // Update balances for each configured chain
    if let Some(eth_config) = &config.chains.ethereum {
        if eth_config.enabled {
            let provider = Provider::<Http>::try_from(&eth_config.rpc_url)
                .map_err(|e| BalanceError::Rpc(e.to_string()))?;
            
            // ALEPH token contract on Ethereum mainnet
            let token_address = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
                .parse::<EthAddress>()
                .map_err(|e| BalanceError::Parse(e.to_string()))?;
            
            for address in &addresses {
                match get_erc20_balance(&provider, &token_address, address).await {
                    Ok(balance) => {
                        let change = update_balance(db, address, Chain::ETH, balance).await?;
                        if let Some(c) = change {
                            changes.push(c);
                        }
                        update_count += 1;
                        metrics.inc_balance_update();
                    }
                    Err(e) => {
                        debug!("Failed to get ETH balance for {}: {}", address, e);
                    }
                }
            }
        }
    }
    
    // Update Avalanche balances
    if let Some(avax_config) = &config.chains.avalanche {
        if avax_config.enabled {
            let provider = Provider::<Http>::try_from(&avax_config.rpc_url)
                .map_err(|e| BalanceError::Rpc(e.to_string()))?;
            
            // ALEPH token on Avalanche C-Chain
            let token_address = "0xc0Fbc4967259786C743361a5885ef49380473dCF"
                .parse::<EthAddress>()
                .map_err(|e| BalanceError::Parse(e.to_string()))?;
            
            for address in &addresses {
                match get_erc20_balance(&provider, &token_address, address).await {
                    Ok(balance) => {
                        let change = update_balance(db, address, Chain::AVAX, balance).await?;
                        if let Some(c) = change {
                            changes.push(c);
                        }
                        update_count += 1;
                    }
                    Err(e) => {
                        debug!("Failed to get AVAX balance for {}: {}", address, e);
                    }
                }
            }
        }
    }
    
    // Process balance changes (e.g., cleanup resources for low balances)
    for change in changes {
        if let BalanceChange::BelowThreshold { address, chain, balance } = change {
            warn!(
                "Account {} on {} balance dropped below threshold: {}",
                address, chain, balance
            );
            // Could trigger resource cleanup here
        }
    }
    
    Ok(update_count)
}

/// Get ERC20 token balance
async fn get_erc20_balance(
    provider: &Provider<Http>,
    token_address: &EthAddress,
    holder_address: &str,
) -> Result<Decimal, BalanceError> {
    // Parse holder address
    let holder: EthAddress = holder_address.parse()
        .map_err(|e| BalanceError::Parse(format!("Invalid address: {}", e)))?;
    
    // Build balanceOf call
    let call_data = build_balance_of_call(&holder);
    
    let tx = TransactionRequest::new()
        .to(*token_address)
        .data(call_data);
    
    let result = provider.call(&tx.into(), None).await
        .map_err(|e| BalanceError::Rpc(e.to_string()))?;
    
    // Parse uint256 result
    let balance_wei = U256::from_big_endian(&result);
    
    // Convert to decimal with 18 decimals
    let balance = wei_to_decimal(balance_wei);
    
    Ok(balance)
}

/// Build balanceOf(address) call data
fn build_balance_of_call(address: &EthAddress) -> Vec<u8> {
    // balanceOf selector is 0x70a08231 - known constant, safe to use expect
    let selector = hex::decode(BALANCE_OF_SELECTOR)
        .expect("BALANCE_OF_SELECTOR is a valid hex constant");
    let mut call_data = selector;
    
    // Pad address to 32 bytes
    let mut padded_address = vec![0u8; 12];
    padded_address.extend_from_slice(address.as_bytes());
    
    call_data.extend(padded_address);
    call_data
}

/// Convert wei to Decimal
fn wei_to_decimal(wei: U256) -> Decimal {
    use rust_decimal_macros::dec;
    
    // Convert U256 to string and then to Decimal
    let wei_str = wei.to_string();
    let wei_decimal = Decimal::from_str(&wei_str).unwrap_or(Decimal::ZERO);
    
    // Divide by 10^18 (use const for efficiency)
    const DIVISOR: Decimal = dec!(1_000_000_000_000_000_000);
    wei_decimal / DIVISOR
}

/// Update balance in database and detect changes
async fn update_balance(
    db: &PgPool,
    address: &str,
    chain: Chain,
    balance: Decimal,
) -> Result<Option<BalanceChange>, BalanceError> {
    // Get previous balance
    let previous: Option<(Decimal,)> = sqlx::query_as(
        "SELECT balance FROM balances WHERE address = $1 AND chain = $2"
    )
    .bind(address)
    .bind(chain.to_string())
    .fetch_optional(db)
    .await
    .map_err(|e| BalanceError::Database(e.to_string()))?;
    
    // Update or insert
    sqlx::query(
        r#"
        INSERT INTO balances (address, chain, balance, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (address, chain) DO UPDATE SET
            balance = EXCLUDED.balance,
            updated_at = EXCLUDED.updated_at
        "#
    )
    .bind(address)
    .bind(chain.to_string())
    .bind(balance)
    .execute(db)
    .await
    .map_err(|e| BalanceError::Database(e.to_string()))?;
    
    // Detect significant changes
    use rust_decimal_macros::dec;
    const CHANGE_THRESHOLD: Decimal = dec!(0.1); // 0.1 ALEPH
    const MIN_BALANCE_THRESHOLD: Decimal = dec!(1.0); // 1 ALEPH minimum
    
    let change = match previous {
        Some((prev_balance,)) => {
            let diff = balance - prev_balance;
            
            if diff > CHANGE_THRESHOLD {
                Some(BalanceChange::Increased {
                    address: address.to_string(),
                    chain,
                    amount: diff,
                })
            } else if diff < -CHANGE_THRESHOLD {
                Some(BalanceChange::Decreased {
                    address: address.to_string(),
                    chain,
                    amount: diff.abs(),
                })
            } else {
                None
            }
        }
        None => None,
    };
    
    // Check for low balance threshold
    if balance < MIN_BALANCE_THRESHOLD && previous.map(|p| p.0 >= MIN_BALANCE_THRESHOLD).unwrap_or(true) {
        return Ok(Some(BalanceChange::BelowThreshold {
            address: address.to_string(),
            chain,
            balance,
        }));
    }
    
    Ok(change)
}

/// Get total balance across all chains for an address
pub async fn get_total_balance(db: &PgPool, address: &str) -> Result<Decimal, BalanceError> {
    let result: (Decimal,) = sqlx::query_as(
        "SELECT COALESCE(SUM(balance), 0) FROM balances WHERE address = $1"
    )
    .bind(address)
    .fetch_one(db)
    .await
    .map_err(|e| BalanceError::Database(e.to_string()))?;
    
    Ok(result.0)
}

/// Check if address has sufficient balance
pub async fn check_balance(
    db: &PgPool,
    address: &str,
    required: Decimal,
) -> Result<bool, BalanceError> {
    let total = get_total_balance(db, address).await?;
    Ok(total >= required)
}

/// Balance tracker errors
#[derive(Debug, thiserror::Error)]
pub enum BalanceError {
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("RPC error: {0}")]
    Rpc(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_wei_to_decimal() {
        // 1 ALEPH = 10^18 wei
        let one_aleph = U256::from(10u64.pow(18));
        let result = wei_to_decimal(one_aleph);
        assert_eq!(result, Decimal::ONE);
        
        // 0.5 ALEPH
        let half_aleph = U256::from(5u64 * 10u64.pow(17));
        let result = wei_to_decimal(half_aleph);
        assert_eq!(result, Decimal::from_str("0.5").unwrap());
    }
    
    #[test]
    fn test_build_balance_of_call() {
        let address: EthAddress = "0x1234567890123456789012345678901234567890".parse().unwrap();
        let call_data = build_balance_of_call(&address);
        
        // 4 bytes selector + 32 bytes address
        assert_eq!(call_data.len(), 36);
        
        // Check selector
        assert_eq!(&call_data[0..4], &hex::decode(BALANCE_OF_SELECTOR).unwrap()[..]);
    }
}
