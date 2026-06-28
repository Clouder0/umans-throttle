//! Configuration — TOML schema with human-readable durations.
//!
//! Example:
//! ```toml
//! [upstream]
//! base_url = "https://api.code.umans.ai"
//!
//! [server]
//! listen = "127.0.0.1:8080"
//!
//! [throttle]
//! max_in_flight = 4
//! max_wait = "5m"
//! idle_timeout = "2m"
//! ```

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub upstream: UpstreamConfig,
    pub server: ServerConfig,
    pub throttle: ThrottleConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    /// Grace period for in-flight requests after SIGTERM before forced exit.
    #[serde(with = "humantime_serde", default = "default_shutdown_timeout")]
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThrottleConfig {
    /// Max concurrent in-flight requests forwarded to upstream.
    /// Code Max = 4, Code Pro = 3.
    pub max_in_flight: usize,
    /// How long a request may wait in queue before we reject it (503).
    #[serde(with = "humantime_serde", default = "default_max_wait")]
    pub max_wait: Duration,
    /// If no bytes arrive from upstream within this window, abort the stream
    /// and release the permit. Safety net against hung upstreams.
    #[serde(with = "humantime_serde", default = "default_idle_timeout")]
    pub idle_timeout: Duration,
    /// Delay between a local upstream response ending and releasing the permit.
    /// This absorbs small upstream account-counter lag without adding client
    /// response latency.
    #[serde(with = "humantime_serde", default = "default_release_grace")]
    pub release_grace: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    /// When to sample the upstream account usage endpoint.
    #[serde(default)]
    pub usage_sampling: UsageSampling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UsageSampling {
    Off,
    #[default]
    #[serde(rename = "on_429")]
    #[serde(alias = "on429")]
    On429,
}

fn default_max_wait() -> Duration {
    Duration::from_secs(300)
}

fn default_idle_timeout() -> Duration {
    Duration::from_secs(120)
}

fn default_release_grace() -> Duration {
    Duration::from_millis(250)
}

fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(30)
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 4,
            max_wait: default_max_wait(),
            idle_timeout: default_idle_timeout(),
            release_grace: default_release_grace(),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            usage_sampling: UsageSampling::On429,
        }
    }
}

impl Config {
    /// Load config from a TOML file. Path may be "-" for stdin (not yet supported).
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Read(path.display().to_string(), e))?;
        let cfg: Self = toml::from_str(&raw).map_err(ConfigError::Parse)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse from a TOML string (used in tests).
    pub fn parse_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(s).map_err(ConfigError::Parse)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Sanity-check config values that would cause silent misbehavior.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.throttle.max_in_flight == 0 {
            return Err(ConfigError::Invalid(
                "throttle.max_in_flight must be at least 1".into(),
            ));
        }
        if self.upstream.base_url.is_empty() {
            return Err(ConfigError::Invalid(
                "upstream.base_url must not be empty".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {0}: {1}")]
    Read(String, #[source] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[source] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}
