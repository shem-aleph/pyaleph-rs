//! Database pool management

use sqlx::postgres::PgPool;

/// Shared database pool state
#[derive(Clone)]
pub struct DbPool(pub PgPool);

impl DbPool {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
    
    pub fn inner(&self) -> &PgPool {
        &self.0
    }
}
