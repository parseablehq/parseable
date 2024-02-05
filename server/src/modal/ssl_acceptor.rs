use std::{fs::File, io::BufReader, path::PathBuf};

use itertools::Itertools;
use rustls::{Certificate, PrivateKey, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys};

pub fn get_ssl_acceptor(
    tls_cert: &Option<PathBuf>,
    tls_key: &Option<PathBuf>,
) -> anyhow::Result<Option<ServerConfig>> {
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            let server_config = ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth();

            let cert_file = &mut BufReader::new(File::open(cert)?);
            let key_file = &mut BufReader::new(File::open(key)?);
            let cert_chain = certs(cert_file)?.into_iter().map(Certificate).collect_vec();

            let mut keys = pkcs8_private_keys(key_file)?
                .into_iter()
                .map(PrivateKey)
                .collect_vec();

            if keys.is_empty() {
                anyhow::bail!("Could not locate PKCS 8 private keys.");
            }

            Ok(Some(
                server_config.with_single_cert(cert_chain, keys.remove(0))?,
            ))
        }
        (_, _) => Ok(None),
    }
}
