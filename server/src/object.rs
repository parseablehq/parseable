use aws_sdk_s3::{Client,Config,Endpoint, Error};
use http::Uri;
use std::env;

#[tokio::main]
pub async fn aws_s3() -> Result<(), Error> {
    let  ep = env::var("AWS_ENDPOINT_URL").unwrap_or("none".to_string());
    let uri = ep.parse::<Uri>().unwrap();
    let endpoint = Endpoint::immutable(uri);
    let config = Config::builder().endpoint_resolver(endpoint).build();

    let client = Client::from_conf(config);
    let resp = client.list_buckets().send().await?;
    let buckets = resp.buckets.unwrap_or_default();
    let num_buckets = buckets.len();
    for bucket in &buckets {
        println!("{}", bucket.name.as_deref().unwrap_or_default());
    }

    println!();
    println!("Found {} buckets", num_buckets);

    Ok(())
}
