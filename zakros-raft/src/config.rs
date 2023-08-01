use rand::{distributions::Uniform, prelude::Distribution};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone)]
pub struct Config {
    pub(crate) heartbeat_interval: Duration,
    pub(crate) election_timeout_min: Duration,
    pub(crate) election_timeout_max: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(200),
            election_timeout_min: Duration::from_secs(1),
            election_timeout_max: Duration::from_secs(2),
        }
    }
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub(crate) fn random_election_deadline(&self) -> Instant {
        let dist = Uniform::new(self.election_timeout_min, self.election_timeout_max);
        tokio::time::Instant::now() + dist.sample(&mut rand::thread_rng())
    }
}

#[derive(Default)]
pub struct ConfigBuilder(Config);

impl ConfigBuilder {
    pub fn heartbeat_interval(&mut self, interval: Duration) -> &mut Self {
        self.0.heartbeat_interval = interval;
        self
    }

    pub fn election_timeout(&mut self, min: Duration, max: Duration) -> &mut Self {
        self.0.election_timeout_min = min;
        self.0.election_timeout_max = max;
        self
    }

    pub fn build(&self) -> Result<Config, ConfigError> {
        if self.0.election_timeout_min >= self.0.election_timeout_max {
            return Err(ConfigError::InvalidElectionTimeoutRange);
        }
        Ok(self.0.clone())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("election timeout has to meet min < max")]
    InvalidElectionTimeoutRange,
}
