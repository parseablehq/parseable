use reqwest::{Client};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // TODO: Replace the url with your Parseable URL
    let url = "https://<parseable-url>/api/v1/query";

    let mut map = HashMap::new();
    // TODO: Replace the stream name with your log stream name
    map.insert("query", "select * from <stream-name>");
    // TODO: Replace the time range with your desired time range
    map.insert("startTime", "2022-11-20T08:20:00+00:00");
    map.insert("endTime", "2022-11-20T22:20:31+00:00");

    let client = Client::new();
    let res = client
        .post(url)
        // TODO: Replace the basic auth credentials with your Parseable credentials
        .header("Authorization", "Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==")
        .header("Content-Type", "application/json")
        .json(&map)
        .send()
        .await
        .unwrap();

    if res.status() != 200 {
        panic!("Error: {}", res.status());
    }

    println!("{}", res.text().await.unwrap());
}
