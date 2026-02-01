# Contributing to pyaleph-rs

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Code of Conduct

Be respectful and constructive. We're all here to build something great.

## Getting Started

### Prerequisites

- Rust 1.70 or later
- PostgreSQL 14+
- Git

### Setup

```bash
# Clone the repository
git clone https://github.com/shem-aleph/pyaleph-rs.git
cd pyaleph-rs

# Build
cargo build

# Run tests
cargo test

# Run with test database
export DATABASE_URL="postgres://localhost/aleph_test"
cargo run -- --migrate
```

## Development Workflow

### Branch Naming

- `feat/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation
- `refactor/description` - Code refactoring
- `test/description` - Test improvements

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add Solana chain indexer
fix: handle empty message content
docs: update API reference
refactor: simplify message validation
test: add aggregate handler tests
```

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting:
   ```bash
   cargo test
   cargo clippy -- -D warnings
   cargo fmt --check
   ```
5. Submit a PR with a clear description

## Code Style

### Rust Style

- Use `rustfmt` for formatting
- Follow Clippy suggestions
- Write documentation comments for public APIs

```rust
/// Validates a message signature.
///
/// # Arguments
///
/// * `message` - The message to validate
/// * `signature` - The signature to verify
///
/// # Returns
///
/// `true` if the signature is valid
pub fn verify_signature(message: &Message, signature: &str) -> bool {
    // Implementation
}
```

### Error Handling

- Use `anyhow::Result` for application errors
- Use custom error types for library code
- Provide context with `.context("description")`

```rust
use anyhow::{Context, Result};

fn process_message(msg: &Message) -> Result<()> {
    validate(msg).context("message validation failed")?;
    store(msg).context("failed to store message")?;
    Ok(())
}
```

### Testing

- Write unit tests for all public functions
- Use integration tests for API endpoints
- Mock external services

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_validation() {
        let msg = create_test_message();
        assert!(validate(&msg).is_ok());
    }

    #[tokio::test]
    async fn test_api_endpoint() {
        let app = create_test_app().await;
        let response = app.get("/health").await;
        assert_eq!(response.status(), 200);
    }
}
```

## Project Structure

```
src/
├── types/       # Core data types - START HERE for new types
├── config/      # Configuration - Add new config options here
├── web/         # HTTP API - Add new endpoints here
├── db/          # Database - Add migrations here
├── services/    # Business logic - Add new services here
├── chains/      # Blockchain indexers - Add new chains here
├── handlers/    # Message handlers - Add new message types here
├── jobs/        # Background tasks - Add new jobs here
└── network/     # P2P networking
```

## Adding New Features

### New Chain Support

1. Create `src/chains/newchain.rs`:
   ```rust
   pub struct NewChainIndexer { ... }
   
   impl ChainIndexer for NewChainIndexer {
       async fn index_range(&self, start: u64, end: u64) -> Result<IndexResult>;
       async fn get_latest_height(&self) -> Result<u64>;
   }
   ```

2. Add configuration in `src/config/mod.rs`

3. Register in `src/chains/mod.rs`

### New API Endpoint

1. Add handler in `src/web/handlers.rs`:
   ```rust
   pub async fn my_endpoint(
       State(state): State<Arc<AppState>>,
       Query(params): Query<MyParams>,
   ) -> impl IntoResponse {
       // Implementation
   }
   ```

2. Add route in `src/web/routes.rs`

3. Document in `docs/API.md`

### New Message Type

1. Add to `MessageType` enum in `src/types/message.rs`

2. Create handler in `src/handlers/newtype.rs`

3. Add migration in `migrations/`

## Documentation

- Update README.md for user-facing changes
- Update docs/ for detailed documentation
- Add inline documentation for code

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Join the [Aleph.im Discord](https://discord.gg/aleph)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
