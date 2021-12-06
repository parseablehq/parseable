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
