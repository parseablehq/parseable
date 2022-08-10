/*
 * Parseable Server (C) 2022 Parseable, Inc.
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

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-env-changed=USE_LOCAL_ASSETS");
    println!("Build File running");
    ui::setup().unwrap()
}

mod ui {

    use std::env;
    use std::fs::{self, create_dir_all, OpenOptions};
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    use cargo_toml::Manifest;
    use sha1_smol::Sha1;
    use static_files::resource_dir;
    use ureq::get as get_from_url;

    fn build_resource_from(local_path: impl AsRef<Path>) -> io::Result<()> {
        let local_path = local_path.as_ref();
        if local_path.exists() {
            resource_dir(local_path).build()
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Local UI directory not found!",
            ))
        }
    }

    pub fn setup() -> io::Result<()> {
        let cargo_manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
        let cargo_toml = cargo_manifest_dir.join("Cargo.toml");
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        let parseable_ui_path = out_dir.join("ui");
        let checksum_path = out_dir.join("parseable_ui.sha1");

        let _use_local_assets =
            env::var("USE_LOCAL_ASSETS").unwrap_or_else(|_| String::from("false"));
        // Maybe throw a warning here
        let use_local_assets = bool::from_str(&_use_local_assets).unwrap_or_default();

        let manifest = Manifest::from_path(cargo_toml).unwrap();

        let manifest = manifest
            .package
            .expect("package not specified in Cargo.toml")
            .metadata
            .expect("no metadata specified in Cargo.toml");

        let metadata = manifest
            .get("parseable_ui")
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "Parseable UI Metadata not defined correctly",
                )
            })
            .unwrap();

        // If local build of ui is to be used
        if use_local_assets {
            if let Some(local_path) = metadata.get("local-assets") {
                println!("cargo:rerun-if-changed={}", local_path.as_str().unwrap());
                build_resource_from(local_path.as_str().unwrap()).unwrap();
                return Ok(());
            }
        }

        // If UI is already downloaded in the target directory then verify and return
        if checksum_path.exists() && parseable_ui_path.exists() {
            let checksum = fs::read_to_string(&checksum_path)?;
            if checksum == metadata["assets-sha1"].as_str().unwrap() {
                // Nothing to do.
                return Ok(());
            }
        }

        // If there is no UI in the target directory or checksum check failed
        // then we downlaod the UI from given url in cargo.toml metadata
        let url = metadata["assets-url"].as_str().unwrap();

        // See https://docs.rs/ureq/2.5.0/ureq/struct.Response.html#method.into_reader
        let parseable_ui_bytes = match get_from_url(url).call() {
            Ok(data) => {
                let mut buf: Vec<u8> = Vec::new();
                data.into_reader().read_to_end(&mut buf).unwrap();
                buf
            }
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to download from {url}"),
                ))
            }
        };

        let checksum = Sha1::from(&parseable_ui_bytes).hexdigest();

        assert_eq!(
            metadata["assets-sha1"].as_str().unwrap(),
            checksum,
            "Downloaded parseable UI shasum differs from the one specified in the Cargo.toml"
        );

        create_dir_all(&parseable_ui_path)?;
        let mut zip = zip::read::ZipArchive::new(Cursor::new(&parseable_ui_bytes))?;
        zip.extract(&parseable_ui_path)?;
        resource_dir(&parseable_ui_path).build()?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(checksum_path)?;

        file.write_all(checksum.as_bytes())?;
        file.flush()?;

        Ok(())
    }
}
