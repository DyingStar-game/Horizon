//! TLS certificates for the listeners: loaded from PEM files and reloaded when the
//! files change, without a restart. New connections get the new certificate;
//! established ones keep theirs.

use crate::error::ServerError;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio_rustls::{rustls, TlsAcceptor};
use tracing::{info, warn};

/// A certificate chain and its private key, as PEM files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// Certificate chain (PEM).
    pub cert_path: PathBuf,
    /// Private key (PEM, PKCS#8, PKCS#1 or SEC1).
    pub key_path: PathBuf,
}

impl TlsFiles {
    /// When either file last changed.
    pub fn modified(&self) -> Option<SystemTime> {
        let cert = std::fs::metadata(&self.cert_path).and_then(|m| m.modified()).ok()?;
        let key = std::fs::metadata(&self.key_path).and_then(|m| m.modified()).ok()?;
        Some(cert.max(key))
    }

    /// The rustls server configuration for these files (TLS for WebSocket).
    pub fn rustls_config(&self) -> Result<rustls::ServerConfig, ServerError> {
        let certs = read_pem(&self.cert_path, |reader| {
            rustls_pemfile::certs(reader).collect::<Result<Vec<_>, _>>()
        })?;
        let key = read_pem(&self.key_path, |reader| rustls_pemfile::private_key(reader))?
            .ok_or_else(|| ServerError::Internal(format!("No private key in {}", self.key_path.display())))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| ServerError::Internal(format!("TLS setup failed: {e}")))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| ServerError::Internal(format!("Invalid certificate or key: {e}")))
    }
}

fn read_pem<T>(path: &Path, parse: impl FnOnce(&mut dyn std::io::BufRead) -> std::io::Result<T>) -> Result<T, ServerError> {
    let file = std::fs::File::open(path)
        .map_err(|e| ServerError::Internal(format!("Cannot open {}: {e}", path.display())))?;
    let mut reader = std::io::BufReader::new(file);
    parse(&mut reader).map_err(|e| ServerError::Internal(format!("Invalid PEM in {}: {e}", path.display())))
}

/// Calls `reload` whenever the files change, checking every `interval`. Stops when
/// `reload` returns `false`.
pub fn watch(files: TlsFiles, interval: Duration, mut reload: impl FnMut() -> bool + Send + 'static) {
    tokio::spawn(async move {
        let mut last = files.modified();
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let now = files.modified();
            if now.is_some() && now != last {
                last = now;
                if !reload() {
                    return;
                }
            }
        }
    });
}

/// A TLS acceptor that picks up new certificate files (WebSocket over TLS).
#[derive(Clone)]
pub struct ReloadingAcceptor {
    current: Arc<RwLock<TlsAcceptor>>,
}

impl std::fmt::Debug for ReloadingAcceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReloadingAcceptor")
    }
}

impl ReloadingAcceptor {
    /// Loads the files and reloads them when they change.
    pub fn new(files: TlsFiles, interval: Duration) -> Result<Self, ServerError> {
        let acceptor = TlsAcceptor::from(Arc::new(files.rustls_config()?));
        let current = Arc::new(RwLock::new(acceptor));
        let weak = Arc::downgrade(&current);
        let watched = files.clone();
        watch(files, interval, move || {
            let Some(current) = weak.upgrade() else { return false };
            match watched.rustls_config() {
                Ok(config) => {
                    *current.write().unwrap_or_else(|e| e.into_inner()) = TlsAcceptor::from(Arc::new(config));
                    info!("🔐 Reloaded the WebSocket TLS certificate from {}", watched.cert_path.display());
                }
                Err(e) => warn!("Keeping the current WebSocket TLS certificate: {}", e),
            }
            true
        });
        Ok(Self { current })
    }

    /// The acceptor for the next connection.
    pub fn acceptor(&self) -> TlsAcceptor {
        self.current.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}
