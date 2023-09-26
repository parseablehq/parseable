/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::error::Error;
use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    ui::setup().unwrap();

    // Init vergen
    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .git_sha(true)
        .emit()?;

    Ok(())
}

mod ui {

    use std::fs::{self, create_dir_all, OpenOptions};
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::{env, panic};

    use cargo_toml::Manifest;
    use sha1_smol::Sha1;
    use static_files::resource_dir;
    use ureq::get as get_from_url;

    const CARGO_MANIFEST_DIR: &str = "CARGO_MANIFEST_DIR";
    const OUT_DIR: &str = "OUT_DIR";
    const LOCAL_ASSETS_PATH: &str = "LOCAL_ASSETS_PATH";

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
        let cargo_manifest_dir = PathBuf::from(env::var(CARGO_MANIFEST_DIR).unwrap());
        let cargo_toml = cargo_manifest_dir.join("Cargo.toml");
        let out_dir = PathBuf::from(env::var(OUT_DIR).unwrap());
        let parseable_ui_path = out_dir.join("ui");
        let checksum_path = out_dir.join("parseable_ui.sha1");

        let manifest = Manifest::from_path(cargo_toml).unwrap();

        let manifest = manifest
            .package
            .expect("package not specified in Cargo.toml")
            .metadata
            .expect("no metadata specified in Cargo.toml");

        let metadata = manifest
            .get("parseable_ui")
            .expect("Parseable UI Metadata not defined correctly");

        // try fetching frontend path from env var
        let local_assets_path: Option<PathBuf> =
            env::var(LOCAL_ASSETS_PATH).ok().map(PathBuf::from);

        // If local build of ui is to be used
        if let Some(ref path) = local_assets_path {
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.to_str().unwrap());
                build_resource_from(path).unwrap();
                return Ok(());
            } else {
                panic!("Directory specified in LOCAL_ASSETS_PATH is not found")
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
        let parseable_ui_bytes = get_from_url(url)
            .call()
            .map(|data| {
                let mut buf: Vec<u8> = Vec::new();
                data.into_reader().read_to_end(&mut buf).unwrap();
                buf
            })
            .expect("Failed to get resource from {url}");

        let checksum = Sha1::from(&parseable_ui_bytes).hexdigest();

        assert_eq!(
            metadata["assets-sha1"].as_str().unwrap(),
            checksum,
            "Downloaded parseable UI shasum differs from the one specified in the Cargo.toml"
        );

        create_dir_all(&parseable_ui_path)?;
        let mut zip = zip::read::ZipArchive::new(Cursor::new(&parseable_ui_bytes))?;
        zip.extract(&parseable_ui_path)?;
        resource_dir(parseable_ui_path.join("dist")).build()?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(checksum_path)?;

        file.write_all(checksum.as_bytes())?;
        file.flush()?;

        if local_assets_path.is_none() {
            // emit ui version for asset url
            let url = url::Url::parse(url).expect("valid url");
            let ui_version = url
                .path_segments()
                .expect("has segemnts")
                .find(|v| v.starts_with('v'))
                .expect("version segement");
            println!("cargo:rustc-env=UI_VERSION={}", ui_version);
        }

        Ok(())
    }
}
