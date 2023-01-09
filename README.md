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
[![Twitter](https://img.shields.io/twitter/follow/parseableio?logo=twitter&style=flat&color=%234B78E6&logoColor=%234B78E6)](https://twitter.com/parseableio)
[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community)](https://launchpass.com/parseable)
[![Docs](https://img.shields.io/badge/stable%20docs-parseable.io%2Fdocs-brightgreen?style=flat&color=%2373DC8C&label=Docs)](https://www.parseable.io/docs)
[![Build](https://img.shields.io/github/actions/workflow/status/parseablehq/parseable/build.yaml?branch=main&label=Build)](https://github.com/parseablehq/parseable/actions)

</div>

Parseable is a lightweight, cloud-native log storage and analysis engine. It can use either a local drive or S3 (and compatible stores) for long-term data storage.

Parseable is written in Rust and uses Apache Arrow and Parquet as underlying data structures. Additionally, it uses a simple, index-free mechanism to organize and query data allowing low latency, and high throughput ingestion and query.

Parseable consumes up to _~90% lower memory_ and _~75% lower CPU_ than Elastic for similar ingestion throughput.

## :dart: Motivation

Traditionally, logging has been seen as a text search problem. Log volumes were not high, and data ingestion or storage were not really issues. This led us to today, where all the logging platforms are primarily text search engines.

But with log data growing exponentially, today's log data challenges involve whole lot more – Data ingestion, storage, and observation, all at scale. We are building Parseable to address these challenges.

## :white_check_mark: Get Started

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

### :100: Usage

If you've already deployed Parseable using the above Docker command, use below commands to create stream and post event(s) to the stream.

#### Create a stream

```sh
curl --location --request PUT 'http://localhost:8000/api/v1/logstream/demo' \
--header 'Authorization: Basic YWRtaW46YWRtaW4='
```

#### Send events to the stream

```sh
curl --location --request POST 'http://localhost:8000/api/v1/logstream/demo' \
--header 'X-P-META-meta1: value1' \
--header 'X-P-TAG-tag1: value1' \
--header 'Authorization: Basic cYWRtaW46YWRtaW4=' \
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

- For complete Parseable API documentation, refer to [Parseable API Docs ↗︎](https://www.parseable.io/docs/category/api).
- To configure Parseable with popular logging agents, please refer to the [agent documentation ↗︎](https://www.parseable.io/docs/category/log-agents).
- To integrate Parseable with your applications directly, please refer to the [integration documentation ↗︎](https://www.parseable.io/docs/category/application-integration).

## :stethoscope: Support

- For questions and feedback please feel free to reach out to us on [Slack ↗︎](https://launchpass.com/parseable).
- For bugs, please create issue on [GitHub ↗︎](https://github.com/parseablehq/parseable/issues).
- For commercial support and consultation, please reach out to us at [`hi@parseable.io` ↗︎](mailto:hi@parseable.io).

## :trophy: Contributing

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

#### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

#### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
