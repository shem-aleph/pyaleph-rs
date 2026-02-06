//! Program message handler
//!
//! Programs are serverless functions that run on Aleph compute nodes.
//! Reference: aleph/handlers/content/vm.py VmMessageHandler

use async_trait::async_trait;
use crate::types::{Message, MessageType, ProgramContent};
use super::{HandlerContext, HandlerError, MessageHandler, vm_common};

/// Handler for program messages
pub struct ProgramHandler;

/// Parse and return the ProgramContent from a message
fn parse_content(message: &Message) -> Result<ProgramContent, HandlerError> {
    let content_str = message.item_content.as_ref()
        .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
    serde_json::from_str(content_str)
        .map_err(|e| HandlerError::InvalidContent(format!("Failed to parse program content: {}", e)))
}

#[async_trait]
impl MessageHandler for ProgramHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Program
    }

    async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content = parse_content(message)?;

        // Validate resources
        if content.resources.memory == 0 {
            return Err(HandlerError::InvalidContent("Memory must be > 0".to_string()));
        }
        if content.resources.vcpus == 0 {
            return Err(HandlerError::InvalidContent("vCPUs must be > 0".to_string()));
        }

        // Validate code and runtime refs
        let (mut pin_refs, mut tag_refs) = vm_common::collect_volume_refs(&content.volumes);
        if content.code.use_latest {
            tag_refs.push(content.code.ref_.clone());
        } else {
            pin_refs.push(content.code.ref_.clone());
        }
        if content.runtime.use_latest {
            tag_refs.push(content.runtime.ref_.clone());
        } else {
            pin_refs.push(content.runtime.ref_.clone());
        }
        vm_common::validate_volume_refs(&pin_refs, &tag_refs, ctx).await?;

        // Validate amendment if replaces is set
        if let Some(ref replaces) = content.replaces {
            vm_common::validate_amendment(replaces, ctx, false).await?;
        }

        Ok(())
    }

    async fn check_balance(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content = parse_content(message)?;

        let payment = match content.payment {
            Some(ref p) => p,
            None => return Ok(()), // No payment info = no balance check
        };

        let cost_service = match ctx.cost.as_ref() {
            Some(c) => c,
            None => return Ok(()), // No cost service configured
        };

        let costs = cost_service.calculate_program_costs(&message.item_hash, &content);
        let total_cost: rust_decimal::Decimal = costs.iter().map(|c| c.cost_hold).sum();

        vm_common::validate_balance(
            &content.address,
            payment.payment_type.clone(),
            total_cost,
            ctx,
        ).await
    }

    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content = parse_content(message)?;

        if let Some(ref db) = ctx.db {
            // Store program in database
            db.store_program(&message.item_hash, &content, &message.sender).await
                .map_err(HandlerError::Database)?;

            // Store volumes
            if !content.volumes.is_empty() {
                db.store_vm_volumes(&message.item_hash, &content.volumes).await
                    .map_err(HandlerError::Database)?;
            }

            // Upsert vm_versions
            let vm_hash = content.replaces.as_deref().unwrap_or(&message.item_hash);
            db.upsert_vm_version(
                vm_hash,
                &content.address,
                &message.item_hash,
                content.time,
            ).await.map_err(HandlerError::Database)?;

            // Store cost records
            if let Some(ref cost_service) = ctx.cost {
                let costs = cost_service.calculate_program_costs(&message.item_hash, &content);
                if !costs.is_empty() {
                    db.store_account_costs(&costs).await
                        .map_err(HandlerError::Database)?;
                }
            }
        }

        tracing::info!(
            "Processed program: hash={} address={} memory={}MB vcpus={} runtime={}",
            &message.item_hash[..std::cmp::min(16, message.item_hash.len())],
            content.address,
            content.resources.memory,
            content.resources.vcpus,
            &content.runtime.ref_[..std::cmp::min(16, content.runtime.ref_.len())],
        );

        Ok(())
    }
}
