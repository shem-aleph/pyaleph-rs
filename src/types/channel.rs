//! Channel types

use serde::{Deserialize, Serialize};

/// A channel is a namespace for messages
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Channel(pub String);

impl Channel {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Channel {
    fn default() -> Self {
        Self("TEST".to_string())
    }
}

impl std::fmt::Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Channel {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Channel {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
