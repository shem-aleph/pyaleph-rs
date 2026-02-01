//! Pricing types for compute and storage

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Product type for pricing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductPriceType {
    Storage,
    Web3Hosting,
    Program,
    ProgramPersistent,
    Instance,
    InstanceGpuPremium,
    InstanceConfidential,
    InstanceGpuStandard,
}

/// Pricing options for a product
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductPriceOptions {
    /// Price for holding tokens (staking)
    pub holding: Decimal,
    /// Pay-as-you-go price
    pub payg: Decimal,
    /// Credit-based price
    pub credit: Decimal,
}

impl ProductPriceOptions {
    pub fn new(holding: Decimal, payg: Decimal, credit: Decimal) -> Self {
        Self { holding, payg, credit }
    }
    
    pub fn zero() -> Self {
        Self {
            holding: Decimal::ZERO,
            payg: Decimal::ZERO,
            credit: Decimal::ZERO,
        }
    }
}

/// Compute unit specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductComputeUnit {
    pub vcpus: u32,
    pub disk_mib: u32,
    pub memory_mib: u32,
}

impl ProductComputeUnit {
    pub fn new(vcpus: u32, disk_mib: u32, memory_mib: u32) -> Self {
        Self { vcpus, disk_mib, memory_mib }
    }
}

/// Price structure for a product
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductPrice {
    pub storage: ProductPriceOptions,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_unit: Option<ProductPriceOptions>,
}

/// Product tier (for GPU instances)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductTier {
    pub id: String,
    pub compute_units: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vram: Option<u32>,
}

/// Full pricing information for a product
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductPricing {
    #[serde(rename = "type")]
    pub product_type: ProductPriceType,
    pub price: ProductPrice,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<ProductTier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_unit: Option<ProductComputeUnit>,
}

/// Balance information for an address
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub address: String,
    pub balance: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked: Option<Decimal>,
}

/// Credit balance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditBalance {
    pub address: String,
    pub balance: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<chrono::DateTime<chrono::Utc>>,
}
