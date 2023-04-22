<h2 align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg">
      <img alt="Parseable Logo" src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg">
    </picture>
    <br>
    Cloud native log observability
</h2>

<div align="center">

[![Docker Pulls](https://img.shields.io/docker/pulls/parseable/parseable?logo=docker&label=Docker%20Pulls)](https://hub.docker.com/r/parseable/parseable)
[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community&style=flat&color=%2373DC8C&)](https://launchpass.com/parseable)
[![Docs](https://img.shields.io/badge/stable%20docs-parseable.io%2Fdocs-brightgreen?style=flat&color=%2373DC8C&label=Docs)](https://www.parseable.io/docs)
[![Build](https://img.shields.io/github/checks-status/parseablehq/parseable/main?style=flat&color=%2373DC8C&label=Checks)](https://github.com/parseablehq/parseable/actions)

</div>

Parseable is a lightweight, cloud native log observability engine. It can use either a local drive or S3 (and compatible stores) for backend data storage.

Parseable is written in Rust and uses Apache Arrow and Parquet as underlying data structures. Additionally, it uses a simple, index-free mechanism to organize and query data allowing low latency, and high throughput ingestion and query.

Parseable consumes up to **_~80% lower memory_** and **_~50% lower CPU_** than Elastic for similar ingestion throughput.

- [Parseable UI Demo (Credentials: admin,admin) ↗︎](https://demo.parseable.io)
- [Grafana Dashboard Demo ↗︎](http://demo.parseable.io:3000/dashboards)

## :rocket: Features

- Choose your own storage backend - local drive or S3 (or compatible) object store.
- Ingestion API compatible with HTTP + JSON output of log agents.
- Query log data with PostgreSQL compatible SQL.
- [Grafana ↗︎](https://github.com/parseablehq/parseable-datasource) for visualization.
- [Send alerts ↗︎](https://www.parseable.io/docs/api/alerts) to webhook targets including Slack.
- [Stats API ↗︎](https://www.postman.com/parseable/workspace/parseable/request/22353706-b32abe55-f0c4-4ed2-9add-110d265888c3) to track ingestion and compressed data.
- Single binary includes all components - ingestion, store and query. Built-in UI.

## :white_check_mark: Getting Started

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

### Create a stream

```sh
curl --location --request PUT 'http://localhost:8000/api/v1/logstream/demo' \
--header 'Authorization: Basic YWRtaW46YWRtaW4='
```

### Send events to the stream

```sh
curl --location --request POST 'http://localhost:8000/api/v1/logstream/demo' \
--header 'X-P-META-meta1: value1' \
--header 'X-P-TAG-tag1: value1' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
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

### Query the stream

You can see the events in Parseable UI, or use the below curl command to see the query response on CLI.

NOTE: Please change the `startTime` and `endTime` to the time range corresponding to the event you sent in the previous step.

```sh
curl --location --request POST 'http://localhost:8000/api/v1/query' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query":"select * from demo",
    "startTime":"2023-01-09T00:00:00+00:00",
    "endTime":"2023-01-09T23:59:00+00:00"
}'
```

## :chart_with_upwards_trend: Benchmarking

Parseable is benchmarked with [K6](https://k6.io). Please find the results and details on how to run the benchmark in your environment in the [benchmarks directory](./benchmarks/).

## :books: Documentation

- [Complete documentation ↗︎](https://www.parseable.io/docs/)
- [Roadmap ↗︎](https://github.com/orgs/parseablehq/projects/4)
- [FAQ ↗︎](https://www.parseable.io/docs/faq)

## :dart: Motivation

Traditionally, logging has been seen as a text search problem. Log volumes were not high, and data ingestion or storage were not really issues. This led us to today, where all the logging platforms are primarily text search engines.

But with log data growing exponentially, today's log data challenges involve whole lot more – Data ingestion, storage, and observation, all at scale. We are building Parseable to address these challenges.

## :stethoscope: Support

- For questions and feedback please feel free to reach out to us on [Slack ↗︎](https://launchpass.com/parseable).
- For bugs, please create issue on [GitHub ↗︎](https://github.com/parseablehq/parseable/issues).
- For commercial support and consultation, please reach out to us at [`hi@parseable.io` ↗︎](mailto:hi@parseable.io).

## :trophy: Contributing

Refer to the contributing guide [here ↗︎](https://www.parseable.io/docs/contributing).

### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
