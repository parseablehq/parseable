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

use clap::Command;
use clap_complete::{generate, Shell};
use std::io;
use std::path::PathBuf;

/// Generates a completion script for the specified shell.
///
/// If `output` is `Some(PathBuf)`, the script is written to the specified file.
/// Otherwise, it's written to `stdout`.
pub fn generate_completion_script(
    cmd: &mut Command,
    bin_name: &str,
    shell: Shell,
    output: Option<PathBuf>,
) -> Result<(), io::Error> {
    if let Some(file_path) = output {
        let mut file = std::fs::File::create(file_path)?;
        generate(shell, cmd, bin_name, &mut file);
    } else {
        generate(shell, cmd, bin_name, &mut io::stdout());
    }
    Ok(())
}
