//! Cost calculation service
//!
//! Calculates costs for storage and compute resources.

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;

use crate::types::{ProductPriceType, ProductPrice, ProductPriceOptions};

/// Cost service for calculating resource costs
pub struct CostService {
    prices: HashMap<ProductPriceType, ProductPrice>,
}

impl Default for CostService {
    fn default() -> Self {
        Self::new()
    }
}

impl CostService {
    /// Create a new cost service with default prices
    pub fn new() -> Self {
        let mut prices = HashMap::new();
        
        // Default prices (per MiB per hour for storage, per compute unit per hour for compute)
        prices.insert(ProductPriceType::Storage, ProductPrice {
            storage: ProductPriceOptions::new(
                Decimal::from_str("0.000000016").unwrap(),
                Decimal::ZERO,
                Decimal::from_str("0.0033").unwrap(),
            ),
            compute_unit: None,
        });
        
        prices.insert(ProductPriceType::Program, ProductPrice {
            storage: ProductPriceOptions::new(
                Decimal::from_str("0.000000016").unwrap(),
                Decimal::ZERO,
                Decimal::from_str("0.0033").unwrap(),
            ),
            compute_unit: Some(ProductPriceOptions::new(
                Decimal::from_str("0.0011").unwrap(),
                Decimal::ZERO,
                Decimal::from_str("0.011").unwrap(),
            )),
        });
        
        prices.insert(ProductPriceType::Instance, ProductPrice {
            storage: ProductPriceOptions::new(
                Decimal::from_str("0.000000016").unwrap(),
                Decimal::ZERO,
                Decimal::from_str("0.0033").unwrap(),
            ),
            compute_unit: Some(ProductPriceOptions::new(
                Decimal::from_str("0.0011").unwrap(),
                Decimal::ZERO,
                Decimal::from_str("0.011").unwrap(),
            )),
        });
        
        prices.insert(ProductPriceType::InstanceConfidential, ProductPrice {
            storage: ProductPriceOptions::new(
                Decimal::from_str("0.000000016").unwrap(),
                Decimal::ZERO,
                Decimal::from_str("0.0033").unwrap(),
            ),
            compute_unit: Some(ProductPriceOptions::new(
                Decimal::from_str("0.0022").unwrap(), // 2x for confidential
                Decimal::ZERO,
                Decimal::from_str("0.022").unwrap(),
            )),
        });
        
        Self { prices }
    }
    
    /// Calculate storage cost
    pub fn calculate_storage_cost(
        &self,
        size_mib: u64,
        hours: u64,
        product_type: ProductPriceType,
    ) -> Option<CostResult> {
        let price = self.prices.get(&product_type)?;
        
        let size = Decimal::from(size_mib);
        let duration = Decimal::from(hours);
        
        Some(CostResult {
            holding: price.storage.holding * size * duration,
            payg: price.storage.payg * size * duration,
            credit: price.storage.credit * size * duration,
        })
    }
    
    /// Calculate compute cost
    pub fn calculate_compute_cost(
        &self,
        compute_units: u32,
        hours: u64,
        product_type: ProductPriceType,
    ) -> Option<CostResult> {
        let price = self.prices.get(&product_type)?;
        let compute_price = price.compute_unit.as_ref()?;
        
        let units = Decimal::from(compute_units);
        let duration = Decimal::from(hours);
        
        Some(CostResult {
            holding: compute_price.holding * units * duration,
            payg: compute_price.payg * units * duration,
            credit: compute_price.credit * units * duration,
        })
    }
    
    /// Calculate total cost for an instance
    pub fn calculate_instance_cost(
        &self,
        memory_mib: u32,
        vcpus: u32,
        storage_mib: u64,
        hours: u64,
        product_type: ProductPriceType,
    ) -> Option<CostResult> {
        // Calculate compute units (simplified: 1 CU = 2GB RAM + 1 vCPU)
        let compute_units = self.calculate_compute_units(memory_mib, vcpus);
        
        let storage_cost = self.calculate_storage_cost(storage_mib, hours, product_type)?;
        let compute_cost = self.calculate_compute_cost(compute_units, hours, product_type)?;
        
        Some(CostResult {
            holding: storage_cost.holding + compute_cost.holding,
            payg: storage_cost.payg + compute_cost.payg,
            credit: storage_cost.credit + compute_cost.credit,
        })
    }
    
    /// Calculate compute units from memory and vCPUs
    pub fn calculate_compute_units(&self, memory_mib: u32, vcpus: u32) -> u32 {
        // 1 compute unit = 2048 MiB RAM and 1 vCPU
        let memory_units = (memory_mib + 2047) / 2048;
        let vcpu_units = vcpus;
        std::cmp::max(memory_units, vcpu_units)
    }
    
    /// Get price for a product type
    pub fn get_price(&self, product_type: &ProductPriceType) -> Option<&ProductPrice> {
        self.prices.get(product_type)
    }
    
    /// Update price for a product type
    pub fn set_price(&mut self, product_type: ProductPriceType, price: ProductPrice) {
        self.prices.insert(product_type, price);
    }
}

/// Result of a cost calculation
#[derive(Debug, Clone)]
pub struct CostResult {
    pub holding: Decimal,
    pub payg: Decimal,
    pub credit: Decimal,
}

impl CostResult {
    /// Get the minimum required holding for a given duration
    pub fn holding_required(&self) -> Decimal {
        self.holding
    }
    
    /// Get the PAYG cost
    pub fn payg_cost(&self) -> Decimal {
        self.payg
    }
    
    /// Get the credit cost
    pub fn credit_cost(&self) -> Decimal {
        self.credit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_compute_units() {
        let cost = CostService::new();
        
        // 2GB RAM, 1 vCPU = 1 CU
        assert_eq!(cost.calculate_compute_units(2048, 1), 1);
        
        // 4GB RAM, 2 vCPU = 2 CU
        assert_eq!(cost.calculate_compute_units(4096, 2), 2);
        
        // 8GB RAM, 2 vCPU = 4 CU (RAM dominates)
        assert_eq!(cost.calculate_compute_units(8192, 2), 4);
        
        // 2GB RAM, 4 vCPU = 4 CU (vCPU dominates)
        assert_eq!(cost.calculate_compute_units(2048, 4), 4);
    }
    
    #[test]
    fn test_storage_cost() {
        let cost = CostService::new();
        
        // 100 MiB for 24 hours
        let result = cost.calculate_storage_cost(100, 24, ProductPriceType::Storage).unwrap();
        
        // Should be positive
        assert!(result.holding > Decimal::ZERO);
        assert!(result.credit > Decimal::ZERO);
    }
}
