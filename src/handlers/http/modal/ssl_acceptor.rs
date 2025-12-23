/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::{
    fs::{self, File},
    io::BufReader,
    path::PathBuf,
};

use rustls::ServerConfig;

pub fn get_ssl_acceptor(
    tls_cert: &Option<PathBuf>,
    tls_key: &Option<PathBuf>,
    other_certs: &Option<PathBuf>,
) -> anyhow::Result<Option<ServerConfig>> {
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            let server_config = ServerConfig::builder().with_no_client_auth();

            let cert_file = &mut BufReader::new(File::open(cert)?);
            let key_file = &mut BufReader::new(File::open(key)?);

            let mut certs = rustls_pemfile::certs(cert_file).collect::<Result<Vec<_>, _>>()?;
            // Load CA certificates from the directory
            if let Some(other_cert_dir) = other_certs
                && other_cert_dir.is_dir()
            {
                for entry in fs::read_dir(other_cert_dir)? {
                    let path = entry.unwrap().path();

                    if path.is_file() {
                        let other_cert_file = &mut BufReader::new(File::open(&path)?);
                        let mut other_certs = rustls_pemfile::certs(other_cert_file)
                            .collect::<Result<Vec<_>, _>>()?;
                        certs.append(&mut other_certs);
                    }
                }
            }
            let private_key = rustls_pemfile::private_key(key_file)?
                .ok_or(anyhow::anyhow!("Could not parse private key."))?;

            Ok(Some(server_config.with_single_cert(certs, private_key)?))
        }
        (_, _) => Ok(None),
    }
}
