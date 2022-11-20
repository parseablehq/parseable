use std::collections::HashMap;
use reqwest::{Client};

#[tokio::main]
async fn main() {
    // TODO: Replace the url with your Parseable URL and stream name    
    let url = "https://<parseable-url>/api/v1/logstream/<stream-name>";

    let mut map = HashMap::new();
    map.insert("id", "434a5f5e-2f5f-11ed-a261-0242ac120002");
    map.insert("datetime", "24/Jun/2022:14:12:15 +0000");
    map.insert("host", "153.10.110.81",);
    map.insert("user-identifier", "Mozilla/5.0 Gecko/20100101 Firefox/64.0",);
    map.insert("method", "PUT",);
    map.insert("status", "500");
    map.insert("referrer", "http://www.google.com/");

    let client = Client::new();
    client
        .post(url)
        // INFO: Use X-P-META-<key>:<value> to add custom metadata to the log event
        .header("X-P-META-meta1", "value1")
        // INFO: Use X-P-TAG-<key>:<value> to add tags to the log event
        .header("X-P-TAG-tag1", "value1")
        // TODO: Replace the basic auth credentials with your Parseable credentials
        .header("Authorization", "Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==")
        .header("Content-Type", "application/json")
        .json(&map)
        .send()
        .await
        .unwrap();
}
