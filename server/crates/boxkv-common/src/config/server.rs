use serde::Deserialize;
use thiserror::Error;
use std::net::IpAddr;

/// Errors that can occur during server configuration validation.
#[derive(Debug, Error)]
pub enum ServerConfigError {
    /// The port number is invalid (e.g., 0).
    #[error("Invalid port: {port}")]
    InvalidPort { port: u16 },

    /// The host address is invalid or cannot be parsed.
    #[error("Invalid host: {host}")]
    InvalidHost { host: String },
}

/// Configuration for the network server.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// The host address to bind the server to (e.g., "127.0.0.1" or "0.0.0.0").
    /// Defaults to "127.0.0.1".
    pub host: String,

    /// The port number to listen on.
    /// Must be greater than 0.
    /// Defaults to 21524.
    pub port: u16,
}

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 21524; // b: 2 o: 15 x: 24

impl ServerConfig {
    /// Validates the server configuration.
    ///
    /// Checks:
    /// 1. `host` is a valid IP address.
    /// 2. `port` is a valid port number (> 0).
    pub(crate) fn validate(&self) -> Result<(), ServerConfigError> {
        self.check_host()?;
        self.check_port()?;

        Ok(())
    }

    fn check_host(&self) -> Result<(), ServerConfigError> {
        self.host.parse::<IpAddr>()
        .map_err(|_| ServerConfigError::InvalidHost { 
            host: self.host.clone() 
        })?;

        Ok(())
    }

    fn check_port(&self) -> Result<(), ServerConfigError> {
        match self.port {
            1..=u16::MAX => Ok(()),
            port => Err(ServerConfigError::InvalidPort { port }),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = ServerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 21524);
    }

    #[test]
    fn test_valid_ipv4_addresses() {
        let valid_ips = vec![
            "127.0.0.1",
            "0.0.0.0",
            "192.168.1.1",
            "255.255.255.255",
            "10.0.0.1",
        ];

        for ip in valid_ips {
            let config = ServerConfig {
                host: ip.to_string(),
                port: 8080,
            };
            assert!(config.validate().is_ok(), "IP {} should be valid", ip);
        }
    }

    #[test]
    fn test_valid_ipv6_addresses() {
        let valid_ips = vec![
            "::1",
            "fe80::1",
            "2001:db8::1",
            "::ffff:192.0.2.1",
            "2001:0db8:0000:0000:0000:0000:0000:0001",
        ];

        for ip in valid_ips {
            let config = ServerConfig {
                host: ip.to_string(),
                port: 8080,
            };
            assert!(config.validate().is_ok(), "IP {} should be valid", ip);
        }
    }

    #[test]
    fn test_invalid_host() {
        let invalid_hosts = vec![
            "localhost",
            "example.com",
            "256.1.1.1",
            "192.168.1",
            "not-an-ip",
            "",
            "192.168.1.1.1",
        ];

        for host in invalid_hosts {
            let config = ServerConfig {
                host: host.to_string(),
                port: 8080,
            };
            let result = config.validate();
            assert!(result.is_err(), "Host {} should be invalid", host);
            match result.unwrap_err() {
                ServerConfigError::InvalidHost { host: h } => {
                    assert_eq!(h, host);
                }
                _ => panic!("Expected InvalidHost error for {}", host),
            }
        }
    }

    #[test]
    fn test_valid_ports() {
        let valid_ports = vec![1, 80, 443, 8080, 21524, 65535];

        for port in valid_ports {
            let config = ServerConfig {
                host: "127.0.0.1".to_string(),
                port,
            };
            assert!(config.validate().is_ok(), "Port {} should be valid", port);
        }
    }

    #[test]
    fn test_invalid_port_zero() {
        let config = ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
        };
        let result = config.validate();
        assert!(result.is_err());
        match result.unwrap_err() {
            ServerConfigError::InvalidPort { port } => {
                assert_eq!(port, 0);
            }
            _ => panic!("Expected InvalidPort error"),
        }
    }

    #[test]
    fn test_combined_validation_success() {
        let config = ServerConfig {
            host: "192.168.1.100".to_string(),
            port: 3000,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_combined_validation_fail_host() {
        let config = ServerConfig {
            host: "invalid-host".to_string(),
            port: 3000,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_combined_validation_fail_port() {
        let config = ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
        };
        assert!(config.validate().is_err());
    }
}
