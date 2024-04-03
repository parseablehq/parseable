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
[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community&style=flat&color=%2373DC8C&)](https://logg.ing/community)
[![Docs](https://img.shields.io/badge/stable%20docs-parseable.io%2Fdocs-brightgreen?style=flat&color=%2373DC8C&label=Docs)](https://logg.ing/docs)
[![Build](https://img.shields.io/github/checks-status/parseablehq/parseable/main?style=flat&color=%2373DC8C&label=Checks)](https://github.com/parseablehq/parseable/actions)

[Key Concepts](https://www.parseable.io/docs/concepts) | [Features](https://github.com/parseablehq/parseable#rocket-highlights) | [Documentation](https://www.parseable.io/docs) | [Demo](https://demo.parseable.com/login?q=eyJ1c2VybmFtZSI6ImFkbWluIiwicGFzc3dvcmQiOiJhZG1pbiJ9) | [Integrations](https://www.parseable.io/docs/category/integrations) | [FAQ](https://www.parseable.io/docs/faq)

</div>

Parseable is a **cloud native, log analytics platform, with a focus on performance & resource efficiency**. Parseable is useful for use cases where **complete data ownership, privacy, and performance are paramount**.

To experience Parseable UI, checkout [demo.parseable.com ↗︎](https://demo.parseable.com/login?q=eyJ1c2VybmFtZSI6ImFkbWluIiwicGFzc3dvcmQiOiJhZG1pbiJ9). You can also view the [demo video ↗︎](https://www.parseable.com/video.mp4).

## :zap: QuickStart

<details>
<summary><a href="https://www.parseable.com/docs/docker-quick-start">Run Parseable in local disk mode with Docker</a></summary>
<p>

You can <a href="https://www.parseable.com/docs/docker-quick-start">get started with Parseable Docker</a> with a simple Docker run and then send data via cURL to understand how you can ingest data to Parseable. Below is the command to run Parseable in local storage mode with Docker.

```bash
docker run -p 8000:8000 \
  parseable/parseable:latest \
  parseable local-store
```

Once this runs successfully, you'll see dashboard at [http://localhost:8000 ↗︎](http://localhost:8000). You can login to the dashboard default credentials `admin`, `admin`.

To ingest data, run the below command. This will send logs to the `demo` stream. You can see the logs in the dashboard.

```bash
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

</p>
</details>

<details>
<summary><a href="https://www.parseable.com/docs/docker-quick-start">Run Parseable binary</a></summary>
<p>

You can also download and run the Parseable binary on your laptop. To download the binary, run the command specific to your OS.

- Linux

```bash
wget https://github.com/parseablehq/parseable/releases/download/v0.9.0/Parseable_x86_64-unknown-linux-gnu -O parseable
chmod +x parseable
./parseable local-store
```

- MacOS (Apple Silicon)

```bash
wget https://github.com/parseablehq/parseable/releases/download/v0.9.0/Parseable_aarch64-apple-darwin -O parseable
chmod +x parseable
./parseable local-store
```

- MacOS (Intel)

```bash
wget https://github.com/parseablehq/parseable/releases/download/v0.9.0/Parseable_x86_64-apple-darwin -O parseable
chmod +x parseable
./parseable local-store
```

- Windows

```bash
Invoke-WebRequest -Uri "https://github.com/parseablehq/parseable/releases/download/v0.9.0/Parseable_x86_64-pc-windows-msvc.exe" -OutFile "C:\parseable.exe"
C:\parseable.exe local-store
```

Once this runs successfully, you'll see dashboard at [http://localhost:8000 ↗︎](http://localhost:8000). You can login to the dashboard default credentials `admin`, `admin`.

To ingest data, run the below command. This will send logs to the `demo` stream. You can see the logs in the dashboard.

```bash
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

</p>
</details>

## :question: Why Parseable

### Take control of your data

With Apache Arrow and Apache Parquet as the underlying data format, Parseable ensures that not only you have access to your data, but also that it is stored in a performant and efficient manner.

### Performance & resource efficiency

### Easy to use for developers and operators


### Enterprise ready

- [Alerts ↗︎](https://www.parseable.io/docs/alerts)
- [RBAC ↗︎](https://www.parseable.io/docs/rbac)
- [OAuth2 ↗︎](https://www.parseable.io/docs/oidc)
- [Grafana ↗︎](https://github.com/parseablehq/parseable-datasource)
- [LLM ↗︎](https://www.parseable.io/docs/llm)
- [Stats ↗︎](https://www.postman.com/parseable/workspace/parseable/request/22353706-b32abe55-f0c4-4ed2-9add-110d265888c3)

## :dart: Motivation



But with log data growing exponentially, today's log data challenges involve whole lot more – Data ingestion, storage, and observation, all at scale. We are building Parseable to address these challenges.

## :trophy: Contributing

[Contribution guide ↗︎](https://www.parseable.io/docs/contributing).

![Alt](https://repobeats.axiom.co/api/embed/7c4e0f51cd3b8f78d1da682c396a3b5bd855a6ba.svg "Repobeats analytics image")

### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

### License report

A license report lists all the licenses of all dependencies in a project. You can use [cargo-about ↗︎](https://embarkstudios.github.io/cargo-about/) to generate a license report for the Parseable.

If not already installed, install `cargo-about` using the below command.

```sh
cargo install --locked cargo-about && cargo about init
```

To generate a license report, run the below command.

```sh
cargo about generate about.hbs > parseable-license.html
```

You can see the license report in the file parseable-license.html.

### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=cb5c7633-1c88-4792-be58-6228c476cef5" />
