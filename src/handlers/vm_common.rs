//! Shared validation and processing logic for INSTANCE and PROGRAM messages.
//! Reference: aleph/handlers/content/vm.py

use crate::types::{VolumeInfo, VolumeSource, PaymentType};
use rust_decimal::Decimal;
use super::{HandlerContext, HandlerError};

/// Collect all volume refs that need to be checked for existence.
/// Returns (pin_refs, tag_refs) -- pin_refs for use_latest=false, tag_refs for use_latest=true.
pub fn collect_volume_refs(volumes: &[VolumeInfo]) -> (Vec<String>, Vec<String>) {
    let mut pin_refs = Vec::new();
    let mut tag_refs = Vec::new();
    for vol in volumes {
        match &vol.source {
            VolumeSource::Immutable { ref_, use_latest } => {
                if *use_latest {
                    tag_refs.push(ref_.clone());
                } else {
                    pin_refs.push(ref_.clone());
                }
            }
            VolumeSource::Persistent { .. } | VolumeSource::Ephemeral { .. } => {}
        }
    }
    (pin_refs, tag_refs)
}

/// Validate volume references exist in file_pins / file_tags.
/// Reference: aleph/handlers/content/vm.py find_missing_volumes()
pub async fn validate_volume_refs(
    pin_refs: &[String],
    tag_refs: &[String],
    ctx: &HandlerContext,
) -> Result<(), HandlerError> {
    if pin_refs.is_empty() && tag_refs.is_empty() {
        return Ok(());
    }
    if let Some(ref db) = ctx.db {
        let (missing_pins, missing_tags) = db.check_volume_refs_exist(pin_refs, tag_refs).await
            .map_err(HandlerError::Database)?;
        if !missing_pins.is_empty() || !missing_tags.is_empty() {
            let all_missing: Vec<_> = missing_pins.into_iter().chain(missing_tags).collect();
            return Err(HandlerError::NotAllowed(format!(
                "Volume references not found: {}", all_missing.join(", ")
            )));
        }
    }
    Ok(())
}

/// Validate amendment (replaces) constraints.
/// Reference: aleph/handlers/content/vm.py check_dependencies()
pub async fn validate_amendment(
    replaces: &str,
    ctx: &HandlerContext,
    is_instance: bool,
) -> Result<(), HandlerError> {
    if let Some(ref db) = ctx.db {
        let original = if is_instance {
            db.get_instance(replaces).await.map_err(HandlerError::Database)?
        } else {
            db.get_program(replaces).await.map_err(HandlerError::Database)?
        };

        let original = original.ok_or_else(|| HandlerError::NotAllowed(
            format!("Referenced VM not found: {}", replaces)
        ))?;

        // Cannot amend an amendment (no chain: A->B->C)
        if original.replaces.is_some() {
            return Err(HandlerError::NotAllowed(
                "Cannot amend an amendment (only direct updates allowed)".to_string()
            ));
        }

        // Check allow_amend on the current version
        let amend_allowed = db.is_vm_amend_allowed(replaces).await
            .map_err(HandlerError::Database)?;
        match amend_allowed {
            Some(false) => return Err(HandlerError::NotAllowed(
                format!("VM {} does not allow amendments", replaces)
            )),
            None => return Err(HandlerError::NotAllowed(
                format!("Could not determine amend status for VM {}", replaces)
            )),
            Some(true) => {} // OK
        }
    }
    Ok(())
}

/// Validate that address has sufficient balance for the VM costs.
/// - HOLD: balance >= existing_cost + new_cost
/// - CREDIT: balance >= (existing_per_sec + new_per_sec) * 86400 (1-day minimum)
/// - SUPERFLUID: pass (validated on-chain)
/// Reference: aleph/services/cost_validation.py validate_balance_for_payment()
pub async fn validate_balance(
    address: &str,
    payment_type: PaymentType,
    message_cost: Decimal,
    ctx: &HandlerContext,
) -> Result<(), HandlerError> {
    match payment_type {
        PaymentType::Superfluid => Ok(()), // Validated on-chain via streams
        PaymentType::Hold => {
            if let Some(ref db) = ctx.db {
                let balance = db.get_balance(address, "ETH").await
                    .map_err(HandlerError::Database)?
                    .unwrap_or(Decimal::ZERO);
                let existing_cost = db.get_total_cost_for_address(address, "hold").await
                    .map_err(HandlerError::Database)?;
                let required = existing_cost + message_cost;
                if balance < required {
                    return Err(HandlerError::InsufficientBalance);
                }
            }
            Ok(())
        }
        PaymentType::Credit => {
            if let Some(ref db) = ctx.db {
                let balance = db.get_credit_balance(address).await
                    .map_err(HandlerError::Database)?
                    .unwrap_or(Decimal::ZERO);
                let existing_per_sec = db.get_total_cost_for_address(address, "credit").await
                    .map_err(HandlerError::Database)?;
                let total_per_sec = existing_per_sec + message_cost;
                let day_seconds = Decimal::from(86400);
                let required = total_per_sec * day_seconds;
                if balance < required {
                    return Err(HandlerError::InsufficientCredit);
                }
            }
            Ok(())
        }
    }
}
