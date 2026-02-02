/// Query parameters for aggregate
#[derive(Debug, Deserialize)]
pub struct AggregateQuery {
    pub keys: Option<String>,
    pub limit: Option<u32>,         // NEW: limit number of aggregates
    pub with_info: Option<bool>,    // NEW: include metadata (created, last_updated, item_hashes)
    pub value_only: Option<bool>,   // NEW: return only the value, not the wrapper
}

/// Aggregate info response for with_info=true
#[derive(Debug, Serialize)]
pub struct AggregateInfo {
    pub created: String,
    pub last_updated: String,
    pub original_item_hash: String,
    pub last_update_item_hash: String,
}

/// Get aggregates for an address - matches pyaleph format
/// 
/// Uses parameterized queries to prevent SQL injection.
/// Supports:
/// - keys: comma-separated list of keys to filter
/// - limit: limit number of aggregates returned
/// - with_info: include metadata (created, last_updated, item_hashes)
/// - value_only: return just the aggregate values (only if single key requested)
pub async fn get_aggregates(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<AggregateQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "data": {},
            "error": "Database not available"
        }));
    }
    
    // Parse keys filter safely
    let key_list: Option<Vec<String>> = params.keys.as_ref().map(|keys| {
        keys.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    
    let with_info = params.with_info.unwrap_or(false);
    let value_only = params.value_only.unwrap_or(false);
    let limit = params.limit;
    
    // Build base query - different for with_info vs regular
    if with_info {
        // Query with join to get metadata
        let aggregates: Vec<(String, serde_json::Value, f64, f64, Option<String>, Option<String>)> = match &key_list {
            Some(keys) if !keys.is_empty() => {
                let mut query_str = String::from(
                    "SELECT a.key, a.content, a.time as created, \
                     COALESCE(ae.time, a.time) as last_updated, \
                     a.last_revision_hash as last_update_item_hash, \
                     ae.item_hash as original_item_hash \
                     FROM aggregates a \
                     LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                     WHERE a.address = $1 AND a.key = ANY($2)"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .bind(keys)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
            _ => {
                let mut query_str = String::from(
                    "SELECT a.key, a.content, a.time as created, \
                     COALESCE(ae.time, a.time) as last_updated, \
                     a.last_revision_hash as last_update_item_hash, \
                     ae.item_hash as original_item_hash \
                     FROM aggregates a \
                     LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                     WHERE a.address = $1"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
        };
        
        if aggregates.is_empty() {
            return Json(json!({
                "error": "No aggregate found for this address"
            }));
        }
        
        // Build data and info maps
        let mut data = serde_json::Map::new();
        let mut info = serde_json::Map::new();
        
        for (key, content, created, last_updated, last_update_hash, original_hash) in aggregates {
            data.insert(key.clone(), content);
            
            // Convert timestamps to ISO format
            let created_dt = chrono::DateTime::from_timestamp(created as i64, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| created.to_string());
            let last_updated_dt = chrono::DateTime::from_timestamp(last_updated as i64, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| last_updated.to_string());
            
            info.insert(key, json!({
                "created": created_dt,
                "last_updated": last_updated_dt,
                "original_item_hash": original_hash.unwrap_or_default(),
                "last_update_item_hash": last_update_hash.unwrap_or_default(),
            }));
        }
        
        Json(json!({
            "address": address,
            "data": data,
            "info": info,
        }))
    } else {
        // Regular query without metadata
        let aggregates: Vec<(String, serde_json::Value)> = match &key_list {
            Some(keys) if !keys.is_empty() => {
                let mut query_str = String::from(
                    "SELECT key, content FROM aggregates WHERE address = $1 AND key = ANY($2)"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .bind(keys)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
            _ => {
                let mut query_str = String::from(
                    "SELECT key, content FROM aggregates WHERE address = $1"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
        };
        
        if aggregates.is_empty() {
            return Json(json!({
                "error": "No aggregate found for this address"
            }));
        }
        
        // Handle value_only - only works for single key
        if value_only {
            if let Some(ref keys) = key_list {
                if keys.len() == 1 {
                    // Find the matching aggregate and return just its value
                    for (key, content) in &aggregates {
                        if key == &keys[0] {
                            return Json(content.clone());
                        }
                    }
                }
            }
        }
        
        // Build data map
        let mut data = serde_json::Map::new();
        for (key, content) in aggregates {
            data.insert(key, content);
        }
        
        Json(json!({
            "address": address,
            "data": data,
        }))
    }
}
