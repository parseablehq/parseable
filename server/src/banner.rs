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

use sysinfo::{System, SystemExt};

use crate::option;
use crate::utils;

pub fn print() {
    let opt = option::get_opts();
    let scheme = utils::get_scheme();
    print_access_info(&scheme, &opt);
    print_sysinfo();
    print_storage_info(&opt);
    println!();
}

fn print_access_info(scheme: &str, opt: &option::Opt) {
    eprintln!(
        "\n
Parseable running on: {}://{}",
        scheme, opt.address
    );
}

fn print_sysinfo() {
    let system = System::new_all();
    eprintln!(
        "
============ SYSTEM =============
OS: {}
Processor: {} logical, {} physical
Memory: {} GiB total",
        os_info::get(),
        num_cpus::get(),
        num_cpus::get_physical(),
        system.total_memory() / (1024 * 1024)
    )
}

fn print_storage_info(opt: &option::Opt) {
    eprintln!(
        "
============ STORAGE ============
Local Data Path: {}
Object Storage: {}/{}",
        opt.local_disk_path, opt.s3_endpoint_url, opt.s3_bucket_name
    )
}
