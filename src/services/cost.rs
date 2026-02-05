//! Cost calculation service
//!
//! Calculates costs for storage and compute resources.
//! 
//! Key features:
//! - Dynamic pricing from aggregates (not hardcoded)
//! - Volume discount calculation
//! - GPU tier pricing with device ID matching
//! - Internet-enabled execution multiplier
//! - Support for hold/payg/credit payment types
//!
//! Reference: aleph/services/cost.py

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::types::{ProductPriceType, ProductPrice, ProductPriceOptions, InstanceContent, ProgramContent, VolumeSource, PaymentType};
use crate::handlers::AccountCostRecord;

/// Address that holds the pricing aggregate
pub const PRICING_AGGREGATE_ADDRESS: &str = "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4";
/// Key for the pricing aggregate
pub const PRICING_AGGREGATE_KEY: &str = "pricing";

/// Default fallback prices (used if aggregate unavailable)
/// 
/// These use compile-time validated constants to avoid unwrap at runtime.
mod defaults {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    
    /// Storage holding cost per MiB per hour
    pub const fn storage_holding() -> Decimal {
        dec!(0.000000016)
    }
    
    /// Storage credit cost per MiB per hour
    pub const fn storage_credit() -> Decimal {
        dec!(0.0033)
    }
    
    /// Compute holding cost per unit per hour
    pub const fn compute_holding() -> Decimal {
        dec!(0.0011)
    }
    
    /// Compute credit cost per unit per hour
    pub const fn compute_credit() -> Decimal {
        dec!(0.011)
    }
    
    /// Confidential instance multiplier
    pub const fn confidential_multiplier() -> Decimal {
        dec!(2.0)
    }
    
    /// GPU premium tier multiplier
    pub const fn gpu_premium_multiplier() -> Decimal {
        dec!(10.0)
    }
    
    /// GPU standard tier multiplier
    pub const fn gpu_standard_multiplier() -> Decimal {
        dec!(5.0)
    }
    
    /// Internet-enabled execution multiplier
    pub const fn internet_multiplier() -> Decimal {
        dec!(1.2)
    }
}

/// Volume discount configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeDiscount {
    /// Minimum compute units to qualify
    pub min_compute_units: u32,
    /// Discount percentage (0.0 to 1.0)
    pub discount: Decimal,
}

impl Default for VolumeDiscount {
    fn default() -> Self {
        Self {
            min_compute_units: 0,
            discount: Decimal::ZERO,
        }
    }
}

/// GPU tier information with device matching
/// 
/// Reference: aleph/types/vms.py GpuProperties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuTier {
    /// Tier identifier (e.g., "nvidia-a100", "nvidia-rtx4090")
    pub id: String,
    /// Display name
    pub name: String,
    /// Compute units for this GPU
    pub compute_units: u32,
    /// VRAM in gigabytes
    pub vram_gb: u32,
    /// Cost multiplier
    pub multiplier: Decimal,
    /// Device IDs that match this tier (PCI device IDs)
    #[serde(default)]
    pub device_ids: Vec<String>,
    /// Device name patterns (for matching by name)
    #[serde(default)]
    pub name_patterns: Vec<String>,
    /// Tier category: "premium" or "standard"
    #[serde(default = "default_tier_category")]
    pub category: String,
}

fn default_tier_category() -> String {
    "standard".to_string()
}

impl GpuTier {
    /// Check if a GPU device matches this tier
    /// 
    /// Matches by:
    /// 1. Exact device ID match (highest priority)
    /// 2. Device ID prefix match
    /// 3. Name pattern match
    pub fn matches(&self, device_id: Option<&str>, device_name: Option<&str>) -> bool {
        // Check device ID match (highest priority)
        if let Some(id) = device_id {
            let id_lower = id.to_lowercase();
            for tier_id in &self.device_ids {
                let tier_id_lower = tier_id.to_lowercase();
                if id_lower == tier_id_lower || id_lower.starts_with(&tier_id_lower) {
                    return true;
                }
            }
        }
        
        // Check name pattern match
        if let Some(name) = device_name {
            let name_lower = name.to_lowercase();
            for pattern in &self.name_patterns {
                let pattern_lower = pattern.to_lowercase();
                if name_lower.contains(&pattern_lower) {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Check if this is a premium tier
    pub fn is_premium(&self) -> bool {
        self.category == "premium" || 
        self.vram_gb >= 24 ||
        self.name.to_lowercase().contains("a100") ||
        self.name.to_lowercase().contains("h100") ||
        self.name.to_lowercase().contains("a40")
    }
}

/// Cost service for calculating resource costs
#[derive(Debug)]
pub struct CostService {
    /// Cached prices from aggregate
    prices: Arc<RwLock<HashMap<ProductPriceType, ProductPrice>>>,
    /// Volume discounts
    volume_discounts: Arc<RwLock<Vec<VolumeDiscount>>>,
    /// GPU tiers with device matching
    gpu_tiers: Arc<RwLock<HashMap<String, GpuTier>>>,
    /// Internet access multiplier
    internet_multiplier: Decimal,
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
        
        // Initialize with default prices (will be overwritten by aggregate)
        let default_storage = ProductPriceOptions::new(
            defaults::storage_holding(),
            Decimal::ZERO,
            defaults::storage_credit(),
        );
        
        let default_compute = ProductPriceOptions::new(
            defaults::compute_holding(),
            Decimal::ZERO,
            defaults::compute_credit(),
        );
        
        prices.insert(ProductPriceType::Storage, ProductPrice {
            storage: default_storage.clone(),
            compute_unit: None,
        });
        
        prices.insert(ProductPriceType::Program, ProductPrice {
            storage: default_storage.clone(),
            compute_unit: Some(default_compute.clone()),
        });
        
        prices.insert(ProductPriceType::Instance, ProductPrice {
            storage: default_storage.clone(),
            compute_unit: Some(default_compute.clone()),
        });
        
        // Confidential instances cost 2x
        prices.insert(ProductPriceType::InstanceConfidential, ProductPrice {
            storage: default_storage.clone(),
            compute_unit: Some(ProductPriceOptions::new(
                defaults::compute_holding() * defaults::confidential_multiplier(),
                Decimal::ZERO,
                defaults::compute_credit() * defaults::confidential_multiplier(),
            )),
        });
        
        // GPU instances
        prices.insert(ProductPriceType::InstanceGpuPremium, ProductPrice {
            storage: default_storage.clone(),
            compute_unit: Some(ProductPriceOptions::new(
                defaults::compute_holding() * defaults::gpu_premium_multiplier(),
                Decimal::ZERO,
                defaults::compute_credit() * defaults::gpu_premium_multiplier(),
            )),
        });
        
        prices.insert(ProductPriceType::InstanceGpuStandard, ProductPrice {
            storage: default_storage.clone(),
            compute_unit: Some(ProductPriceOptions::new(
                defaults::compute_holding() * defaults::gpu_standard_multiplier(),
                Decimal::ZERO,
                defaults::compute_credit() * defaults::gpu_standard_multiplier(),
            )),
        });
        
        // Default volume discounts using compile-time validated decimals
        use rust_decimal_macros::dec;
        let volume_discounts = vec![
            VolumeDiscount { min_compute_units: 10, discount: dec!(0.05) },
            VolumeDiscount { min_compute_units: 50, discount: dec!(0.10) },
            VolumeDiscount { min_compute_units: 100, discount: dec!(0.15) },
            VolumeDiscount { min_compute_units: 500, discount: dec!(0.20) },
        ];
        
        // Initialize default GPU tiers with device IDs
        // Reference: aleph/types/vms.py GPU device mapping
        let mut gpu_tiers = HashMap::new();
        gpu_tiers.insert("nvidia-a100".to_string(), GpuTier {
            id: "nvidia-a100".to_string(),
            name: "NVIDIA A100".to_string(),
            compute_units: 100,
            vram_gb: 40,
            multiplier: dec!(10.0),
            device_ids: vec![
                "20b0".to_string(), // A100 PCIe 40GB
                "20b2".to_string(), // A100 SXM4 40GB
                "20bf".to_string(), // A100 PCIe 80GB
                "20b5".to_string(), // A100 SXM4 80GB
            ],
            name_patterns: vec!["A100".to_string()],
            category: "premium".to_string(),
        });
        
        gpu_tiers.insert("nvidia-h100".to_string(), GpuTier {
            id: "nvidia-h100".to_string(),
            name: "NVIDIA H100".to_string(),
            compute_units: 150,
            vram_gb: 80,
            multiplier: dec!(15.0),
            device_ids: vec![
                "2330".to_string(), // H100 PCIe
                "2331".to_string(), // H100 SXM
            ],
            name_patterns: vec!["H100".to_string()],
            category: "premium".to_string(),
        });
        
        gpu_tiers.insert("nvidia-rtx4090".to_string(), GpuTier {
            id: "nvidia-rtx4090".to_string(),
            name: "NVIDIA RTX 4090".to_string(),
            compute_units: 60,
            vram_gb: 24,
            multiplier: dec!(8.0),
            device_ids: vec![
                "2684".to_string(), // RTX 4090
            ],
            name_patterns: vec!["RTX 4090".to_string(), "4090".to_string()],
            category: "premium".to_string(),
        });
        
        gpu_tiers.insert("nvidia-rtx3090".to_string(), GpuTier {
            id: "nvidia-rtx3090".to_string(),
            name: "NVIDIA RTX 3090".to_string(),
            compute_units: 40,
            vram_gb: 24,
            multiplier: dec!(6.0),
            device_ids: vec![
                "2204".to_string(), // RTX 3090
                "2208".to_string(), // RTX 3090 Ti
            ],
            name_patterns: vec!["RTX 3090".to_string(), "3090".to_string()],
            category: "premium".to_string(),
        });
        
        gpu_tiers.insert("nvidia-a40".to_string(), GpuTier {
            id: "nvidia-a40".to_string(),
            name: "NVIDIA A40".to_string(),
            compute_units: 50,
            vram_gb: 48,
            multiplier: dec!(8.0),
            device_ids: vec![
                "2235".to_string(), // A40
            ],
            name_patterns: vec!["A40".to_string()],
            category: "premium".to_string(),
        });
        
        gpu_tiers.insert("nvidia-t4".to_string(), GpuTier {
            id: "nvidia-t4".to_string(),
            name: "NVIDIA T4".to_string(),
            compute_units: 20,
            vram_gb: 16,
            multiplier: dec!(4.0),
            device_ids: vec![
                "1eb8".to_string(), // T4
            ],
            name_patterns: vec!["T4".to_string()],
            category: "standard".to_string(),
        });
        
        gpu_tiers.insert("nvidia-rtx3080".to_string(), GpuTier {
            id: "nvidia-rtx3080".to_string(),
            name: "NVIDIA RTX 3080".to_string(),
            compute_units: 25,
            vram_gb: 10,
            multiplier: dec!(5.0),
            device_ids: vec![
                "2206".to_string(), // RTX 3080
                "2216".to_string(), // RTX 3080 Ti
            ],
            name_patterns: vec!["RTX 3080".to_string(), "3080".to_string()],
            category: "standard".to_string(),
        });
        
        Self {
            prices: Arc::new(RwLock::new(prices)),
            volume_discounts: Arc::new(RwLock::new(volume_discounts)),
            gpu_tiers: Arc::new(RwLock::new(gpu_tiers)),
            internet_multiplier: defaults::internet_multiplier(),
        }
    }
    
    /// Update prices from an aggregate value
    /// 
    /// This should be called when the pricing aggregate is updated.
    pub async fn update_from_aggregate(&self, aggregate: &serde_json::Value) -> Result<(), String> {
        let mut prices = self.prices.write().await;
        
        // Parse prices from aggregate
        if let Some(pricing) = aggregate.get("pricing").and_then(|v| v.as_object()) {
            for (key, value) in pricing {
                if let Ok(product_type) = serde_json::from_str::<ProductPriceType>(&format!("\"{}\"", key)) {
                    if let Ok(price) = serde_json::from_value::<ProductPrice>(value.clone()) {
                        prices.insert(product_type, price);
                        tracing::info!("Updated price for {:?} from aggregate", product_type);
                    }
                }
            }
        }
        
        // Parse volume discounts
        if let Some(discounts) = aggregate.get("volume_discounts").and_then(|v| v.as_array()) {
            let mut vd = self.volume_discounts.write().await;
            vd.clear();
            
            for discount in discounts {
                if let Ok(d) = serde_json::from_value::<VolumeDiscount>(discount.clone()) {
                    vd.push(d);
                }
            }
            vd.sort_by(|a, b| a.min_compute_units.cmp(&b.min_compute_units));
        }
        
        // Parse GPU tiers with device ID support
        if let Some(tiers) = aggregate.get("gpu_tiers").and_then(|v| v.as_object()) {
            let mut gt = self.gpu_tiers.write().await;
            
            for (tier_id, tier_data) in tiers {
                if let Ok(tier) = serde_json::from_value::<GpuTier>(tier_data.clone()) {
                    gt.insert(tier_id.clone(), tier);
                }
            }
        }
        
        Ok(())
    }
    
    /// Calculate storage cost
    pub async fn calculate_storage_cost(
        &self,
        size_mib: u64,
        hours: u64,
        product_type: ProductPriceType,
    ) -> Option<CostResult> {
        let prices = self.prices.read().await;
        let price = prices.get(&product_type)?;
        
        let size = Decimal::from(size_mib);
        let duration = Decimal::from(hours);
        
        Some(CostResult {
            holding: price.storage.holding * size * duration,
            payg: price.storage.payg * size * duration,
            credit: price.storage.credit * size * duration,
        })
    }
    
    /// Calculate compute cost with optional internet multiplier
    pub async fn calculate_compute_cost(
        &self,
        compute_units: u32,
        hours: u64,
        product_type: ProductPriceType,
        internet_enabled: bool,
    ) -> Option<CostResult> {
        let prices = self.prices.read().await;
        let price = prices.get(&product_type)?;
        let compute_price = price.compute_unit.as_ref()?;
        
        let units = Decimal::from(compute_units);
        let duration = Decimal::from(hours);
        
        // Apply internet multiplier if enabled
        let multiplier = if internet_enabled {
            self.internet_multiplier
        } else {
            Decimal::ONE
        };
        
        Some(CostResult {
            holding: compute_price.holding * units * duration * multiplier,
            payg: compute_price.payg * units * duration * multiplier,
            credit: compute_price.credit * units * duration * multiplier,
        })
    }
    
    /// Get volume discount for a given number of compute units
    pub async fn get_volume_discount(&self, compute_units: u32) -> Decimal {
        let discounts = self.volume_discounts.read().await;
        
        let mut applicable_discount = Decimal::ZERO;
        for discount in discounts.iter() {
            if compute_units >= discount.min_compute_units {
                applicable_discount = discount.discount;
            } else {
                break;
            }
        }
        
        applicable_discount
    }
    
    /// Calculate total cost for an instance with volume discount
    pub async fn calculate_instance_cost(
        &self,
        memory_mib: u32,
        vcpus: u32,
        storage_mib: u64,
        hours: u64,
        product_type: ProductPriceType,
        internet_enabled: bool,
    ) -> Option<CostResult> {
        // Calculate compute units (simplified: 1 CU = 2GB RAM + 1 vCPU)
        let compute_units = self.calculate_compute_units(memory_mib, vcpus);
        
        let storage_cost = self.calculate_storage_cost(storage_mib, hours, product_type).await?;
        let compute_cost = self.calculate_compute_cost(compute_units, hours, product_type, internet_enabled).await?;
        
        // Apply volume discount
        let discount = self.get_volume_discount(compute_units).await;
        let discount_multiplier = Decimal::ONE - discount;
        
        Some(CostResult {
            holding: (storage_cost.holding + compute_cost.holding) * discount_multiplier,
            payg: (storage_cost.payg + compute_cost.payg) * discount_multiplier,
            credit: (storage_cost.credit + compute_cost.credit) * discount_multiplier,
        })
    }
    
    /// Calculate compute units from memory and vCPUs
    /// 
    /// # Formula
    /// 
    /// 1 compute unit = 2048 MiB RAM and 1 vCPU.
    /// The result is the maximum of memory-based units and vCPU count,
    /// ensuring resources are always covered.
    /// 
    /// # Examples
    /// 
    /// ```rust
    /// let cost = CostService::new();
    /// assert_eq!(cost.calculate_compute_units(2048, 1), 1);  // 1 CU
    /// assert_eq!(cost.calculate_compute_units(4096, 2), 2);  // 2 CU
    /// assert_eq!(cost.calculate_compute_units(8192, 2), 4);  // Memory dominates
    /// ```
    #[inline]
    pub fn calculate_compute_units(&self, memory_mib: u32, vcpus: u32) -> u32 {
        // Round up memory to nearest compute unit
        let memory_units = memory_mib.saturating_add(2047) / 2048;
        std::cmp::max(memory_units, vcpus)
    }
    
    /// Get price for a product type
    pub async fn get_price(&self, product_type: &ProductPriceType) -> Option<ProductPrice> {
        let prices = self.prices.read().await;
        prices.get(product_type).cloned()
    }
    
    /// Update price for a product type manually
    pub async fn set_price(&self, product_type: ProductPriceType, price: ProductPrice) {
        let mut prices = self.prices.write().await;
        prices.insert(product_type, price);
    }
    
    /// Get GPU tier information by tier ID
    pub async fn get_gpu_tier(&self, tier_id: &str) -> Option<GpuTier> {
        let tiers = self.gpu_tiers.read().await;
        tiers.get(tier_id).cloned()
    }
    
    /// Find GPU tier by device ID or name
    /// 
    /// This matches GPU devices to tiers using:
    /// 1. Exact device ID match (PCI device ID)
    /// 2. Device ID prefix match
    /// 3. Device name pattern match
    pub async fn find_gpu_tier_by_device(
        &self,
        device_id: Option<&str>,
        device_name: Option<&str>,
    ) -> Option<GpuTier> {
        let tiers = self.gpu_tiers.read().await;
        
        // First try exact device ID match
        if let Some(id) = device_id {
            for tier in tiers.values() {
                if tier.matches(Some(id), None) {
                    return Some(tier.clone());
                }
            }
        }
        
        // Then try name match
        if let Some(name) = device_name {
            for tier in tiers.values() {
                if tier.matches(None, Some(name)) {
                    return Some(tier.clone());
                }
            }
        }
        
        None
    }
    
    /// Determine product type based on GPU device
    /// 
    /// Returns the appropriate price type (premium/standard) based on GPU.
    pub async fn get_gpu_product_type(
        &self,
        device_id: Option<&str>,
        device_name: Option<&str>,
    ) -> ProductPriceType {
        if let Some(tier) = self.find_gpu_tier_by_device(device_id, device_name).await {
            if tier.is_premium() {
                ProductPriceType::InstanceGpuPremium
            } else {
                ProductPriceType::InstanceGpuStandard
            }
        } else {
            // Default to standard if no match
            ProductPriceType::InstanceGpuStandard
        }
    }
    
    /// Calculate cost for a GPU instance
    pub async fn calculate_gpu_instance_cost(
        &self,
        memory_mib: u32,
        vcpus: u32,
        storage_mib: u64,
        hours: u64,
        gpu_tier_id: &str,
        internet_enabled: bool,
    ) -> Option<CostResult> {
        let tier = self.get_gpu_tier(gpu_tier_id).await?;
        
        // Determine product type based on tier
        let product_type = if tier.is_premium() {
            ProductPriceType::InstanceGpuPremium
        } else {
            ProductPriceType::InstanceGpuStandard
        };
        
        // Use tier compute units if specified
        let compute_units = if tier.compute_units > 0 {
            tier.compute_units
        } else {
            self.calculate_compute_units(memory_mib, vcpus)
        };
        
        let storage_cost = self.calculate_storage_cost(storage_mib, hours, product_type).await?;
        let compute_cost = self.calculate_compute_cost(compute_units, hours, product_type, internet_enabled).await?;
        
        // Apply GPU tier multiplier
        let multiplied_cost = CostResult {
            holding: storage_cost.holding + (compute_cost.holding * tier.multiplier),
            payg: storage_cost.payg + (compute_cost.payg * tier.multiplier),
            credit: storage_cost.credit + (compute_cost.credit * tier.multiplier),
        };
        
        // Apply volume discount
        let discount = self.get_volume_discount(compute_units).await;
        let discount_multiplier = Decimal::ONE - discount;
        
        Some(CostResult {
            holding: multiplied_cost.holding * discount_multiplier,
            payg: multiplied_cost.payg * discount_multiplier,
            credit: multiplied_cost.credit * discount_multiplier,
        })
    }
    
    /// Calculate cost for GPU instance by device ID/name
    /// 
    /// This is the preferred method as it automatically finds the right tier.
    pub async fn calculate_gpu_instance_cost_by_device(
        &self,
        memory_mib: u32,
        vcpus: u32,
        storage_mib: u64,
        hours: u64,
        device_id: Option<&str>,
        device_name: Option<&str>,
        internet_enabled: bool,
    ) -> Option<CostResult> {
        let tier = self.find_gpu_tier_by_device(device_id, device_name).await;
        
        let product_type = if let Some(ref t) = tier {
            if t.is_premium() {
                ProductPriceType::InstanceGpuPremium
            } else {
                ProductPriceType::InstanceGpuStandard
            }
        } else {
            ProductPriceType::InstanceGpuStandard
        };
        
        let compute_units = if let Some(ref t) = tier {
            if t.compute_units > 0 {
                t.compute_units
            } else {
                self.calculate_compute_units(memory_mib, vcpus)
            }
        } else {
            self.calculate_compute_units(memory_mib, vcpus)
        };
        
        let multiplier = tier.map(|t| t.multiplier).unwrap_or_else(|| {
            use rust_decimal_macros::dec;
            dec!(5.0) // Default GPU multiplier
        });
        
        let storage_cost = self.calculate_storage_cost(storage_mib, hours, product_type).await?;
        let compute_cost = self.calculate_compute_cost(compute_units, hours, product_type, internet_enabled).await?;
        
        let multiplied_cost = CostResult {
            holding: storage_cost.holding + (compute_cost.holding * multiplier),
            payg: storage_cost.payg + (compute_cost.payg * multiplier),
            credit: storage_cost.credit + (compute_cost.credit * multiplier),
        };
        
        let discount = self.get_volume_discount(compute_units).await;
        let discount_multiplier = Decimal::ONE - discount;
        
        Some(CostResult {
            holding: multiplied_cost.holding * discount_multiplier,
            payg: multiplied_cost.payg * discount_multiplier,
            credit: multiplied_cost.credit * discount_multiplier,
        })
    }

    /// Calculate costs for an instance message.
    /// Returns a list of AccountCostRecord entries (execution + rootfs + volume costs).
    /// Reference: aleph/services/cost.py _calculate_executable_costs()
    pub fn calculate_instance_costs(
        &self,
        item_hash: &str,
        content: &InstanceContent,
    ) -> Vec<AccountCostRecord> {
        let owner = &content.address;
        let payment_type = content.payment.as_ref()
            .map(|p| match p.payment_type {
                PaymentType::Hold => "hold",
                PaymentType::Credit => "credit",
                PaymentType::Superfluid => "superfluid",
            })
            .unwrap_or("hold")
            .to_string();

        let mut costs = Vec::new();
        let compute_units = self.calculate_compute_units(content.resources.memory, content.resources.vcpus);

        // 1. Execution cost
        let cu_dec = Decimal::from(compute_units);
        let exec_hold = defaults::compute_holding() * cu_dec;
        let exec_credit = defaults::compute_credit() * cu_dec;
        costs.push(AccountCostRecord {
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            cost_type: "EXECUTION".to_string(),
            name: "execution".to_string(),
            ref_hash: None,
            payment_type: payment_type.clone(),
            cost_hold: exec_hold,
            cost_stream: Decimal::ZERO,
            cost_credit: exec_credit,
        });

        // 2. Rootfs volume cost
        let rootfs_mib = Decimal::from(content.rootfs.size_mib);
        costs.push(AccountCostRecord {
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            cost_type: "EXECUTION_INSTANCE_VOLUME_ROOTFS".to_string(),
            name: "rootfs".to_string(),
            ref_hash: Some(content.rootfs.parent.ref_.clone()),
            payment_type: payment_type.clone(),
            cost_hold: defaults::storage_holding() * rootfs_mib,
            cost_stream: Decimal::ZERO,
            cost_credit: defaults::storage_credit() * rootfs_mib,
        });

        // 3. Machine volume costs
        for (i, vol) in content.volumes.iter().enumerate() {
            let (cost_type, size, ref_hash) = match &vol.source {
                VolumeSource::Immutable { ref_, .. } => (
                    "EXECUTION_VOLUME_IMMUTABLE", 0u32, Some(ref_.clone()),
                ),
                VolumeSource::Persistent { size_mib, .. } => (
                    "EXECUTION_VOLUME_PERSISTENT", *size_mib, None,
                ),
                VolumeSource::Ephemeral { size_mib, .. } => (
                    "EXECUTION_VOLUME_EPHEMERAL", *size_mib, None,
                ),
            };
            let size_dec = Decimal::from(size);
            costs.push(AccountCostRecord {
                owner: owner.to_string(),
                item_hash: item_hash.to_string(),
                cost_type: cost_type.to_string(),
                name: format!("volume_{}", i),
                ref_hash,
                payment_type: payment_type.clone(),
                cost_hold: defaults::storage_holding() * size_dec,
                cost_stream: Decimal::ZERO,
                cost_credit: defaults::storage_credit() * size_dec,
            });
        }

        costs
    }

    /// Calculate costs for a program message.
    /// Same structure as instance costs but with program-specific cost type names.
    pub fn calculate_program_costs(
        &self,
        item_hash: &str,
        content: &ProgramContent,
    ) -> Vec<AccountCostRecord> {
        let owner = &content.address;
        let payment_type = content.payment.as_ref()
            .map(|p| match p.payment_type {
                PaymentType::Hold => "hold",
                PaymentType::Credit => "credit",
                PaymentType::Superfluid => "superfluid",
            })
            .unwrap_or("hold")
            .to_string();

        let mut costs = Vec::new();
        let compute_units = self.calculate_compute_units(content.resources.memory, content.resources.vcpus);

        // 1. Execution cost
        let cu_dec = Decimal::from(compute_units);
        costs.push(AccountCostRecord {
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            cost_type: "EXECUTION".to_string(),
            name: "execution".to_string(),
            ref_hash: None,
            payment_type: payment_type.clone(),
            cost_hold: defaults::compute_holding() * cu_dec,
            cost_stream: Decimal::ZERO,
            cost_credit: defaults::compute_credit() * cu_dec,
        });

        // 2. Machine volume costs
        for (i, vol) in content.volumes.iter().enumerate() {
            let (cost_type, size, ref_hash) = match &vol.source {
                VolumeSource::Immutable { ref_, .. } => (
                    "EXECUTION_VOLUME_IMMUTABLE", 0u32, Some(ref_.clone()),
                ),
                VolumeSource::Persistent { size_mib, .. } => (
                    "EXECUTION_VOLUME_PERSISTENT", *size_mib, None,
                ),
                VolumeSource::Ephemeral { size_mib, .. } => (
                    "EXECUTION_VOLUME_EPHEMERAL", *size_mib, None,
                ),
            };
            let size_dec = Decimal::from(size);
            costs.push(AccountCostRecord {
                owner: owner.to_string(),
                item_hash: item_hash.to_string(),
                cost_type: cost_type.to_string(),
                name: format!("volume_{}", i),
                ref_hash,
                payment_type: payment_type.clone(),
                cost_hold: defaults::storage_holding() * size_dec,
                cost_stream: Decimal::ZERO,
                cost_credit: defaults::storage_credit() * size_dec,
            });
        }

        costs
    }
}

/// Result of a cost calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    
    /// Get cost for a specific payment type
    pub fn for_payment_type(&self, payment_type: &str) -> Decimal {
        match payment_type.to_lowercase().as_str() {
            "hold" | "holding" => self.holding,
            "payg" | "pay-as-you-go" => self.payg,
            "credit" => self.credit,
            _ => self.holding, // Default to holding
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_compute_units() {
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
    
    use std::str::FromStr;
    
    #[tokio::test]
    async fn test_storage_cost() {
        let cost = CostService::new();
        
        // 100 MiB for 24 hours
        let result = cost.calculate_storage_cost(100, 24, ProductPriceType::Storage).await.unwrap();
        
        // Should be positive
        assert!(result.holding > Decimal::ZERO);
        assert!(result.credit > Decimal::ZERO);
    }
    
    #[tokio::test]
    async fn test_volume_discount() {
        let cost = CostService::new();
        
        // No discount for small usage
        assert_eq!(cost.get_volume_discount(5).await, Decimal::ZERO);
        
        // 5% discount for 10+ CU
        assert_eq!(cost.get_volume_discount(10).await, Decimal::from_str("0.05").unwrap());
        
        // 20% discount for 500+ CU
        assert_eq!(cost.get_volume_discount(500).await, Decimal::from_str("0.20").unwrap());
    }
    
    #[tokio::test]
    async fn test_gpu_tier_matching() {
        let cost = CostService::new();
        
        // Match by device ID
        let tier = cost.find_gpu_tier_by_device(Some("20b0"), None).await;
        assert!(tier.is_some());
        assert_eq!(tier.unwrap().name, "NVIDIA A100");
        
        // Match by name
        let tier = cost.find_gpu_tier_by_device(None, Some("RTX 4090")).await;
        assert!(tier.is_some());
        assert_eq!(tier.unwrap().id, "nvidia-rtx4090");
        
        // Match by partial name
        let tier = cost.find_gpu_tier_by_device(None, Some("NVIDIA GeForce RTX 3090 Ti")).await;
        assert!(tier.is_some());
        assert!(tier.unwrap().name.contains("3090"));
        
        // No match
        let tier = cost.find_gpu_tier_by_device(Some("unknown"), Some("Unknown GPU")).await;
        assert!(tier.is_none());
    }
    
    #[tokio::test]
    async fn test_gpu_tier_is_premium() {
        let cost = CostService::new();
        
        // A100 should be premium
        let tier = cost.get_gpu_tier("nvidia-a100").await.unwrap();
        assert!(tier.is_premium());
        
        // T4 should be standard
        let tier = cost.get_gpu_tier("nvidia-t4").await.unwrap();
        assert!(!tier.is_premium());
    }
    
    #[tokio::test]
    async fn test_update_from_aggregate() {
        let cost = CostService::new();
        
        let aggregate = serde_json::json!({
            "pricing": {
                "storage": {
                    "storage": {
                        "holding": "0.000000020",
                        "payg": "0.0",
                        "credit": "0.0040"
                    }
                }
            },
            "gpu_tiers": {
                "custom-gpu": {
                    "id": "custom-gpu",
                    "name": "Custom GPU",
                    "compute_units": 50,
                    "vram_gb": 16,
                    "multiplier": "6.0",
                    "device_ids": ["9999"],
                    "name_patterns": ["Custom"],
                    "category": "standard"
                }
            }
        });
        
        cost.update_from_aggregate(&aggregate).await.unwrap();
        
        let price = cost.get_price(&ProductPriceType::Storage).await.unwrap();
        assert_eq!(price.storage.credit, Decimal::from_str("0.0040").unwrap());
        
        let tier = cost.get_gpu_tier("custom-gpu").await;
        assert!(tier.is_some());
        assert_eq!(tier.unwrap().vram_gb, 16);
    }

    #[tokio::test]
    async fn test_instance_cost_calculation() {
        let cost = CostService::new();
        let content = crate::types::InstanceContent {
            address: "0xTest".to_string(),
            allow_amend: true,
            rootfs: crate::types::RootfsInfo {
                parent: crate::types::RootfsParent { ref_: "rootfs_hash".to_string(), use_latest: true },
                persistence: "host".to_string(),
                size_mib: 20480,
            },
            resources: crate::types::VmResources { memory: 2048, vcpus: 1, seconds: 30 },
            environment: None,
            metadata: None,
            variables: None,
            volumes: vec![],
            authorized_keys: vec![],
            payment: Some(crate::types::PaymentInfo {
                chain: crate::types::Chain::ETH,
                payment_type: crate::types::PaymentType::Hold,
                receiver: None,
            }),
            requirements: None,
            replaces: None,
            time: 1234567890.0,
        };
        let costs = cost.calculate_instance_costs("test_hash", &content);
        assert_eq!(costs.len(), 2); // execution + rootfs
        assert_eq!(costs[0].cost_type, "EXECUTION");
        assert!(costs[0].cost_hold > Decimal::ZERO);
        assert_eq!(costs[1].cost_type, "EXECUTION_INSTANCE_VOLUME_ROOTFS");
    }
}
