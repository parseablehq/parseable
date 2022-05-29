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
 *
 */

use crossterm::style::Stylize;
use sysinfo::{System, SystemExt};

use crate::option;
use crate::utils;

pub fn print() {
    let opt = option::get_opts();
    let scheme = utils::get_scheme();
    status_info(&scheme, &opt);
    warning(&opt);
    storage_info(&opt);
    system_info();
    println!();
}

fn status_info(scheme: &str, opt: &option::Opt) {
    let url = format!("{}://{}", scheme, opt.address).underlined();
    eprintln!(
        "
    {}
    {}
    {}",
        format!("Parseable server started at: {}", url).bold(),
        format!("Username: {}", opt.username).bold(),
        format!("Password: {}", opt.password).bold(),
    )
}

fn system_info() {
    let system = System::new_all();
    eprintln!(
        "
    {}
        OS: {}
        Processor: {} logical, {} physical
        Memory: {} GiB total",
        "System:".to_string().blue().bold(),
        os_info::get(),
        num_cpus::get(),
        num_cpus::get_physical(),
        system.total_memory() / (1024 * 1024)
    )
}

fn storage_info(opt: &option::Opt) {
    eprintln!(
        "
    {}
        Local Data Path: {}
        Object Storage: {}/{}",
        "Storage:".to_string().blue().bold(),
        opt.local_disk_path,
        opt.s3_endpoint_url,
        opt.s3_bucket_name
    )
}

fn warning(opt: &option::Opt) {
    if opt.s3_endpoint_url == option::DEFAULT_S3_URL
        && opt.username == option::DEFAULT_USERNAME
        && opt.password == option::DEFAULT_PASSWORD
    {
        warning_line();
        cred_warning(opt);
        s3_warning(opt);
    } else if opt.s3_endpoint_url == option::DEFAULT_S3_URL {
        warning_line();
        s3_warning(opt);
    } else if opt.username == option::DEFAULT_USERNAME && opt.password == option::DEFAULT_PASSWORD {
        warning_line();
        cred_warning(opt);
    }
}

fn warning_line() {
    eprintln!(
        "
    {}",
        "Warning:".to_string().red().bold(),
    );
}

fn cred_warning(opt: &option::Opt) {
    if opt.username == option::DEFAULT_USERNAME && opt.password == option::DEFAULT_PASSWORD {
        eprintln!(
            "
        {}
        {}",
            "Parseable server is using default credentials."
                .to_string()
                .red(),
            format!(
                "Setup your credentials with {} and {} before storing production logs.",
                option::USERNAME_ENV,
                option::PASSOWRD_ENV
            )
            .red()
        )
    }
}

fn s3_warning(opt: &option::Opt) {
    if opt.s3_endpoint_url == option::DEFAULT_S3_URL {
        eprintln!(
            "
        {}
        {}",
            "Parseable server is using default object storage backend with public access."
                .to_string()
                .red(),
            format!(
                "Setup your object storage backend with {} before storing production logs.",
                option::S3_URL_ENV_VAR
            )
            .red()
        )
    }
}
