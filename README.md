<h2 align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg">
      <a href="https://www.parseable.com" target="_blank"><img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg" alt="Parseable logo" /></a>
    </picture>
</h2>

<div class="title-block" style="text-align: center;" align="center">

[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community&style=flat&color=%2373DC8C&)](https://logg.ing/community)
[![Docs](https://img.shields.io/badge/stable%20docs-parseable.com%2Fdocs-brightgreen?style=flat&color=%2373DC8C&label=Docs)](https://logg.ing/docs)
[![Build](https://img.shields.io/github/checks-status/parseablehq/parseable/main?style=flat&color=%2373DC8C&label=Build)](https://github.com/parseablehq/parseable/actions)

**[Introduction](https://www.parseable.com/docs/introduction) &nbsp;&nbsp;&bull;&nbsp;&nbsp;**
**[Docs](https://parseable.com/docs) &nbsp;&nbsp;&bull;&nbsp;&nbsp;**
**[Features](https://www.parseable.com/docs/features) &nbsp;&nbsp;&bull;&nbsp;&nbsp;**
**[Benchmarks](https://www.parseable.com/docs/benchmarks) &nbsp;&nbsp;&nbsp;&nbsp;**

</div>

Parseable is an open source, columnar data lake platform - purpose built for observability. Send logs, metrics & traces to Parseable via popular logging agents, OpenTelemetry, Kafka, eBPF or other integrations. Use native observability features like alerting, dashboards, anomaly detection, APM, PromQL & SQL - all within a single binary.

## Why Parseable?

Purpose built for observability and designed around proven data lake engineering patterns, Parseable gives you everything you need to make sense of your telemetry data, right out of the box, with no external dependencies or stitching together of multiple tools.

Some of the key highlights include:

- [Data lake architecture](https://www.parseable.com/docs/architecture): Parseable Data lake architecture allows running stateless compute with object storage as the backing storage. This allows scaling storage and compute independently, and avoids the pitfalls of traditional observability systems.

- [Fully featured](https://www.parseable.com/docs/features): Parseable is feature complete with alerting, dashboards, anomaly detection, APM, and more. You can do all of this and more from a single binary, without needing to stitch together multiple tools.

- [Agent ready](https://www.parseable.com/docs/integrations#ai-agents--llms): Whether you need to observe your AI agents or use LLMs to analyze your telemetry data, Parseable has you covered with native support for AI agents and LLMs.

- [OpenTelemetry native](https://www.parseable.com/docs/ingest-data/otel): With native OTel support, you can send telemetry data to Parseable without any custom modifications or plugins. Parseable can be used as a drop-in replacement for your existing OpenTelemetry Collector setup.

## Quickstart

Download and run the Parseable binary on your laptop:

```bash
curl -fsSL https://logg.ing/install | bash
```

<details> 
<summary>For Windows</summary>

```pwsh
powershell -c "irm https://logg.ing/install-windows | iex"
```
</details>

Once you have Parseable running, ingest data with the below command. This will send logs to the `demo` stream. You can see the logs in the dashboard.

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

Access the UI at http://localhost:8000. You can login to the dashboard default credentials `admin`, `admin`.

For production deployments, refer the [installation guide ↗︎](https://www.parseable.com/docs/self-hosted/installation) for best practices and hardening tips.

> [!TIP]
> Try out the [Parseable cloud](https://app.parseable.com) — 14 days free trial, no credit card required.

## Contributing

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" alt="Contributors" /></a>

[Contribution guide ↗︎](https://github.com/parseablehq/parseable/blob/main/CONTRIBUTING.md)

## Verify images

Parseable builds are attested for build provenance and integrity using the [attest-build-provenance](https://github.com/actions/attest-build-provenance) action. The attestations can be verified by having the latest version of [GitHub CLI](https://github.com/cli/cli/releases/latest) installed in your system. Then, execute the following command:

```sh
gh attestation verify PATH/TO/YOUR/PARSEABLE/ARTIFACT-BINARY -R parseablehq/parseable
```
