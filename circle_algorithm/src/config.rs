use anyhow::{Result, anyhow};
use serde::Deserialize;
use std::fs::read;

#[derive(Debug, Deserialize)]
pub struct ServerList {
    pub list: Vec<String>
}

pub fn load_config() -> Result<Vec<String>> {
    let config_path = "/app/server-list.json".to_string();
    let read = read(&config_path)?;

    let config = String::from_utf8_lossy(&read)
        .trim()
        .to_string();

    if config.is_empty() {
        return Err(anyhow!("Config file is empty"));
    }

    let ServerList { list } = serde_json::from_str(&config)
        .map_err(|e| anyhow!("Failed to parse config file: {}", e))?;

    Ok(list)
}
