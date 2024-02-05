/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

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
