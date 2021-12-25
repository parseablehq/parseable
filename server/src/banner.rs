/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use sysinfo::{System, SystemExt};

use crate::option;

pub fn print() {
    let opt = option::get_opts();
    print_access_info(&opt);
    print_sysinfo();
    print_storage_info(&opt);
    print_curl_example(&opt);
    println!();
}

fn print_access_info(opt: &option::Opt) {
    eprintln!(
        "\n
Parseable server running on: http://{}",
        opt.http_addr
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
Local disk cache: {}
Backend S3 endpoint: {}
Backend S3 bucket: {}",
        opt.local_disk_path, opt.s3_endpoint_url, opt.s3_bucket_name
    )
}

fn print_curl_example(opt: &option::Opt) {
    let curl_create_str: String =
        "curl --location --request PUT '".to_owned() + &opt.http_addr + &"/test_str'".to_owned();
    eprintln!(
        "
============ ACCESS =============
Create a Data Stream: {}",
        curl_create_str
    )
}
