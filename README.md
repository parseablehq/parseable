<h2 align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg">
      <a href="https://www.parseable.io" target="_blank"><img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg" alt="Parseable" width="600" height="150" /></a>
    </picture>
    <br>
    Cloud native log analytics
</h2>

<div align="center">

[![Docker Pulls](https://img.shields.io/docker/pulls/parseable/parseable?logo=docker&label=Docker%20Pulls)](https://hub.docker.com/r/parseable/parseable)
[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community&style=flat&color=%2373DC8C&)](https://launchpass.com/parseable)
[![Docs](https://img.shields.io/badge/stable%20docs-parseable.io%2Fdocs-brightgreen?style=flat&color=%2373DC8C&label=Docs)](https://www.parseable.io/docs)
[![Build](https://img.shields.io/github/checks-status/parseablehq/parseable/main?style=flat&color=%2373DC8C&label=Checks)](https://github.com/parseablehq/parseable/actions)

[Key Concepts](https://www.parseable.io/docs/concepts) | [Features](https://github.com/parseablehq/parseable#rocket-highlights) | [Documentation](https://www.parseable.io/docs/) | [Demo](https://demo.parseable.io/login?q=eyJ1c2VybmFtZSI6ImFkbWluIiwicGFzc3dvcmQiOiJhZG1pbiJ9) | [Integrations](https://www.parseable.io/docs/category/integrations) | [FAQ](https://www.parseable.io/docs/faq)

</div>

Parseable is a log analytics platform, built for the modern, cloud native era. Parseable uses a index-free mechanism to organize and query data allowing low latency, and high throughput ingestion and query.

To get started, download the Parseable binary from [releases page ↗︎](https://github.com/parseablehq/parseable/releases/latest) and run it on your machine.

For comparison, Parseable consumes up to **_~80% lower memory_** and **_~50% lower CPU_** than Elastic for similar ingestion throughput. Read more in the [benchmarks directory ↗︎](./benchmarks/).

For :stethoscope: commercial support and consultation, please reach out to us at [`sales@parseable.io` ↗︎](mailto:sales@parseable.io).

![Parseable Console](https://raw.githubusercontent.com/parseablehq/.github/main/images/console.png)

## :zap: Quickstart

Deploy Parseable in local storage mode with Docker.

```sh
docker run -p 8000:8000 \
  parseable/parseable:latest \
  parseable local-store
```

Once this runs successfully, you'll see dashboard at [http://localhost:8000 ↗︎](http://localhost:8000). You can login to the dashboard default credentials `admin`, `admin`.

To ingest data, run the below command. This will send logs to the `demo` stream. You can see the logs in the dashboard.

```sh
curl --location --request POST 'http://localhost:8000/api/v1/ingest' \
--header 'X-P-Stream: demo' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '[
    {
        "id": "434a5f5e-2f5f-11ed-a261-0242ac120002",
        "datetime": "24/Jun/2022:14:12:15 +0000",
        "host": "153.10.110.81"
    }
]'
```

## :rocket: Highlights

- Choose storage backend - local drive or S3 (or compatible) object store.
- Ingestion API compatible with HTTP + JSON output of log agents.
- Query log data with PostgreSQL compatible SQL.
- Single binary includes all components - ingestion, store and query. Built-in UI.

### Enterprise ready

- [Alerts ↗︎](https://www.parseable.io/docs/alerts)
- [RBAC ↗︎](https://www.parseable.io/docs/rbac)
- [OAuth2 ↗︎](https://www.parseable.io/docs/oidc)
- [Grafana ↗︎](https://github.com/parseablehq/parseable-datasource)
- [LLM ↗︎](https://www.parseable.io/docs/llm)
- [Stats ↗︎](https://www.postman.com/parseable/workspace/parseable/request/22353706-b32abe55-f0c4-4ed2-9add-110d265888c3)

## :dart: Motivation

Traditionally, logging has been seen as a text search problem. Log volumes were not high, and data ingestion or storage were not really issues. This led us to today, where all the logging platforms are primarily text search engines.

But with log data growing exponentially, today's log data challenges involve whole lot more – Data ingestion, storage, and observation, all at scale. We are building Parseable to address these challenges.

## :trophy: Contributing

[Contribution guide ↗︎](https://www.parseable.io/docs/contributing).

![Alt](https://repobeats.axiom.co/api/embed/7c4e0f51cd3b8f78d1da682c396a3b5bd855a6ba.svg "Repobeats analytics image")

### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
