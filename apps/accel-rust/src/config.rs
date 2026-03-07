use std::{
    env,
    net::{AddrParseError, SocketAddr},
};

pub struct ServerBind {
    pub host: String,
    pub port: u16,
}

impl ServerBind {
    pub fn from_env() -> Self {
        let host = env::var("AIWF_ACCEL_RUST_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("AIWF_ACCEL_RUST_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(18082);
        Self { host, port }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

pub fn env_truthy(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            matches!(t.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

pub fn allow_egress_enabled() -> bool {
    env_truthy("AIWF_ALLOW_EGRESS") || env_truthy("AIWF_ALLOW_CLOUD_LLM")
}

pub fn is_local_endpoint(endpoint: &str) -> bool {
    let s = endpoint.trim();
    s.contains("127.0.0.1") || s.contains("localhost") || s.contains("[::1]")
}
