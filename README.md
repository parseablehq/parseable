<h2 align="center">
    <a href="https://parseable.io" target="blank_">
      <img height="80" width="400" alt="Parseable" src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg#gh-light-mode-only" />
    </a>
    <a href="https://parseable.io" target="blank_">
      <img height="80" width="400" alt="Parseable" src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png#gh-dark-mode-only" />
    </a>
    <br>
    Cloud native log observability
</h2>

<div align="center">

[![Docker Pulls](https://img.shields.io/docker/pulls/parseable/parseable?label=DockerPulls)](https://hub.docker.com/r/parseable/parseable)
[![Docs](https://img.shields.io/badge/stable%20parseable.io%2Fdocs%2Fbrightgreen?style=flat&color=%2373DC8C&label=Docs)](https://www.parseable.io/docs)
[![Twitter](https://img.shields.io/twitter/follow/parseableio?style=flat&color=%234B78E6&logoColor=%234B78E6)](https://twitter.com/parseableio)
[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community)](https://launchpass.com/parseable)
[![Build](https://img.shields.io/github/actions/workflow/status/parseablehq/parseable/build.yaml?branch=main&color=%23FA9BFA&label=Build)](https://github.com/parseablehq/parseable/actions)

</div>

<h4 align="center">
  <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/console.png" />
  <a href="https://www.parseable.io/docs/quick-start" target="_blank">Quick Start</a> |
  <a href="https://www.parseable.io/docs" target="_blank">Documentation</a> |
  <a href="https://launchpass.com/parseable" target="_blank">Community</a> |
  <a href="https://demo.parseable.io" target="_blank">Live Demo</a>
</h4>

## :wave: Introduction

Parseable is a open source log observability platform. Written in Rust, it is designed for simplicity of deployment and use. It is compatible with standard logging agents via their HTTP output. Parseable also offers a builtin GUI for log query and analysis.

We're focussed on 

* Simplicity - ease of deployment and use. 
* Efficiency - lesser CPU, Memory usage. 
* Extensibility - freedom to do more with event data. 
* Performance - lower latency, higher throughput.

## :dart: Motivation

Given the analytical nature of log data, columnar formats like Parquet are the best way to store and analyze. Parquet offers compression and inherent analytical capabilities. However, indexing based text search engines are _still_ prevalent. We are building Parseable to take full advantage of advanced data formats like Apache Parquet and Arrow. This approach is simpler, efficient and much more scalable.

Parseable is developer friendly, cloud native, logging platforms today that is simple to deploy and run - while offering a rich set of features.

## :question: How it works

Parseable exposes REST API to ingest and query log data. Under the hood, it uses Apache Arrow and Parquet to handle and compress high volume log data. All data is stored in S3 (or compatible systems). Parseable also has a bundled web console to visualize and query log data. 

- Written in Rust. Low CPU & memory footprint, with low latency, high throughput.
- Open data format (Parquet). Complete ownership of data. Wide range of possibilities for data analysis.
- Single binary / container based deployment (including UI). Deploy in minutes if not seconds.
- Indexing free design. Lower CPU and storage overhead. Similar levels of performance as indexing based systems.
- Kubernetes and Cloud native design, build ground up for cloud native environments.

## :white_check_mark: Installing

Run the below command to deploy Parseable in local storage mode with Docker.

```sh
mkdir -p /tmp/parseable/data
mkdir -p /tmp/parseable/staging

docker run -p 8000:8000 \
  -v /tmp/parseable/data:/parseable/data \
  -v /tmp/parseable/staging:/parseable/staging \
  -e P_FS_DIR=/parseable/data \
  -e P_STAGING_DIR=/parseable/staging \
  parseable/parseable:latest \
  parseable local-store
```

Once this runs successfully, you'll see dashboard at [http://localhost:8000](http://localhost:8000). You can login to the dashboard default credentials `admin`, `admin`.

Prefer other platforms? Check out installation options (Kubernetes, bare-metal), in the [documentation](https://www.parseable.io/docs/category/installation).

#### Live demo

Instead of installing locally, you can also try out Parseable on our [Demo instance](https://demo.parseable.io). Credentials to login to the dashboard are `parseable` / `parseable`.

## :100: Usage

If you've already deployed Parseable using the above Docker command, use below commands to create stream and post event(s) to the stream. Make sure to replace `<stream-name>` with the name of the stream you want to create and post events (e.g. `my-stream`).
#### Create a stream

```sh
curl --location --request PUT 'http://localhost:8000/api/v1/logstream/<stream-name>' \
--header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ=='
```

#### Send events to the stream

```sh
curl --location --request POST 'http://localhost:8000/api/v1/logstream/<stream-name>' \
--header 'X-P-META-meta1: value1' \
--header 'X-P-TAG-tag1: value1' \
--header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==' \
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

- For complete Parseable API documentation, refer to [Parseable API Docs](https://www.parseable.io/docs/category/api).
- To configure Parseable with popular logging agents, please refer to the [agent documentation](https://www.parseable.io/docs/category/log-agents).
- To integrate Parseable with your applications directly, please refer to the [integration documentation](https://www.parseable.io/docs/category/application-integration).

## :stethoscope: Support

For questions and feedback please feel free to reach out to us on [Slack](https://launchpass.com/parseable). For bugs, please create issue on [GitHub](https://github.com/parseablehq/parseable/issues). 

For commercial support and consultation, please reach out to us at [`hi@parseable.io`](mailto:hi@parseable.io).

## :trophy: Contributing

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

#### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

#### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
