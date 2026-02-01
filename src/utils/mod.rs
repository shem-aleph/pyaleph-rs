//! Utility functions

use std::time::{SystemTime, UNIX_EPOCH};

/// Get current Unix timestamp as f64
/// 
/// # Panics
/// 
/// Panics if system time is before UNIX epoch (should never happen in practice).
pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before UNIX epoch")
        .as_secs_f64()
}

/// Get current Unix timestamp as u64 (seconds)
/// 
/// # Panics
/// 
/// Panics if system time is before UNIX epoch (should never happen in practice).
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before UNIX epoch")
        .as_secs()
}

/// Get current Unix timestamp as u128 (milliseconds)
/// 
/// # Panics
/// 
/// Panics if system time is before UNIX epoch (should never happen in practice).
pub fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before UNIX epoch")
        .as_millis()
}

/// Get current Unix timestamp, returning None if unavailable
/// 
/// Safe version that doesn't panic.
pub fn now_opt() -> Option<f64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs_f64())
}

/// Format bytes as human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    
    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration in seconds as human-readable string
pub fn format_duration(secs: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = MINUTE * 60;
    const DAY: u64 = HOUR * 24;
    
    if secs >= DAY {
        let days = secs / DAY;
        let hours = (secs % DAY) / HOUR;
        format!("{}d {}h", days, hours)
    } else if secs >= HOUR {
        let hours = secs / HOUR;
        let mins = (secs % HOUR) / MINUTE;
        format!("{}h {}m", hours, mins)
    } else if secs >= MINUTE {
        let mins = secs / MINUTE;
        let s = secs % MINUTE;
        format!("{}m {}s", mins, s)
    } else {
        format!("{}s", secs)
    }
}

/// Truncate a string with ellipsis
pub fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len <= 3 {
        "...".to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

/// Validate hex string
pub fn is_valid_hex(s: &str) -> bool {
    let s = s.strip_prefix("0x").unwrap_or(s);
    !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Validate Ethereum address
pub fn is_valid_eth_address(s: &str) -> bool {
    let s = s.strip_prefix("0x").unwrap_or(s);
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }
    
    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3661), "1h 1m");
        assert_eq!(format_duration(90061), "1d 1h");
    }
    
    #[test]
    fn test_is_valid_eth_address() {
        assert!(is_valid_eth_address("0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8"));
        assert!(is_valid_eth_address("06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8"));
        assert!(!is_valid_eth_address("0x123")); // Too short
        assert!(!is_valid_eth_address("not-an-address"));
    }
    
    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 8), "hello...");
        assert_eq!(truncate("hi", 2), "hi");
    }
}
