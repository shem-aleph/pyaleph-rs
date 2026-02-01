//! Database migrations

use sqlx::PgPool;
use tracing::info;

/// Run all pending database migrations
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    info!("Running database migrations...");
    
    // Check if migrations table exists
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _migrations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(256) NOT NULL UNIQUE,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        "#
    )
    .execute(pool)
    .await?;
    
    // List of migrations to apply
    let migrations = vec![
        ("001_initial_schema", include_str!("../../migrations/001_initial_schema.sql")),
    ];
    
    for (name, sql) in migrations {
        // Check if migration already applied
        let applied: Option<(i32,)> = sqlx::query_as(
            "SELECT id FROM _migrations WHERE name = $1"
        )
        .bind(name)
        .fetch_optional(pool)
        .await?;
        
        if applied.is_none() {
            info!("Applying migration: {}", name);
            
            // Execute migration SQL
            sqlx::raw_sql(sql)
                .execute(pool)
                .await?;
            
            // Record migration
            sqlx::query(
                "INSERT INTO _migrations (name) VALUES ($1)"
            )
            .bind(name)
            .execute(pool)
            .await?;
            
            info!("Migration {} applied successfully", name);
        } else {
            info!("Migration {} already applied, skipping", name);
        }
    }
    
    info!("All migrations complete");
    Ok(())
}
