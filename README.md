<p align="center">
  <span">
    <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg#gh-light-mode-only" alt="Parseable" width="400" height="80" />
    <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png#gh-dark-mode-only" alt="Parseable" width="400" height="80" />
  </a> 
</p>

<h4 align="center">
  <p> Parseable is an open source log storage and observability platform, built for cloud native ecosystem. </p>
  <a href="https://www.parseable.io/docs/quick-start" target="_blank">Quick Start</a> |
  <a href="https://www.parseable.io/docs/introduction" target="_blank">Documentation</a> |
  <a href="https://launchpass.com/parseable" target="_blank">Community</a> |
  <a href="https://demo.parseable.io" target="_blank">Live Demo</a>
  <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/console.png" />
</h4>

## Why Parseable?

Written in Rust, Parseable has a low CPU, memory footprint and offers low latency, high throughput, out of the box. 

Parseable is designed keeping modern cloud native infrastructure at its heart. This means Parseable can be deployed in cloud and container based environments in minutes and can scale as the requirements grow. 

- [x] Highly compressed log data storage with [Parquet](https://parquet.apache.org).
- [x] Use standard SQL for querying log data.
- [x] Auto inferred schema for log streams.
- [x] Dashboard to query the log data.
- [x] Compatible with existing logging agents.
- [x] Scale with scaling up the node.
- [ ] Configurable alerting.
- [ ] Distributed, multi-node cluster.
- [ ] Log data visualization via Parseable UI.
- [ ] Access control for users and groups.
- [ ] Log data retention and export.
- [ ] Kafka plugin to ingest log data.
- [ ] Grafana plugin to visualize log data.
- [ ] Anomaly detection within log data.

## Installing

Docker is the quickest way to experience Parseable on your machine. Run the below command to deploy Parseable with a demo configuration.

```sh
mkdir -p /tmp/data
docker run \
  -p 8000:8000 \
  -v /tmp/data:/data \
  parseable/parseable:latest \
  parseable server --demo
```

Parseable dashboard is available at [http://localhost:8000](http://localhost:8000). Credentials to login to the dashboard are `parseable`, `parseable`.

For non-demo and other deployment platforms, please refer to the [installation documentation](https://www.parseable.io/docs/category/installation).

## Using Parseable
### Create a stream

```sh
curl --location --request PUT 'http://localhost:8000/api/v1/logstream/<stream-name>' \
--header 'Authorization: Basic <basic-auth-header>'
```

### Send events to the stream

```sh
curl --location --request POST 'http://localhost:8000/api/v1/logstream/<stream-name>' \
--header 'X-P-META-meta1: value1' \
--header 'X-P-TAG-tag1: value1' \
--header 'Authorization: Basic <basic-auth-header>' \
--header 'Content-Type: application/json' \
--data-raw '[
    {
        "id": "434a5f5e-2f5f-11ed-a261-0242ac120002",
        "datetime": "24/Jun/2022:14:12:15 +0000",
        "host": "153.10.110.81", 
        "user-identifier": "Mozilla/5.0 Gecko/20100101 Firefox/64.0", 
        "method": "PUT", 
        "status": 500, 
        "referrer": "http://www.google.com/"
    }
]'
```

For complete Parseable API documentation, refer to [Parseable API Docs](https://www.parseable.io/docs/category/api).

## Live Demo 

You can also try out Parseable on our [https://demo.parseable.io](https://demo.parseable.io). Credentials to login to the dashboard are `parseable` / `parseable`.
                                                                              
## Contributing

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
