use reqwest::{Client};

#[tokio::main]
async fn main() {
    // TODO: Replace the url with your Parseable URL and stream name    
    let url = "https://<parseable-url>/api/v1/logstream/<stream-name>";

    let client = Client::new();
    client
        .put(url)
        // TODO: Replace the basic auth credentials with your Parseable credentials
        .header("Authorization", "Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==")
        .send()
        .await
        .unwrap();
}
