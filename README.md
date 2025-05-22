<h2 align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg">
      <a href="https://www.parseable.com" target="_blank"><img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg" alt="Parseable logo" /></a>
    </picture>
</h2>

<div align="center">

[![Docker Pulls](https://img.shields.io/docker/pulls/parseable/parseable?logo=docker&label=Docker%20Pulls)](https://hub.docker.com/r/parseable/parseable)
[![Slack](https://img.shields.io/badge/slack-brightgreen.svg?logo=slack&label=Community&style=flat&color=%2373DC8C&)](https://logg.ing/community)
[![Docs](https://img.shields.io/badge/stable%20docs-parseable.com%2Fdocs-brightgreen?style=flat&color=%2373DC8C&label=Docs)](https://logg.ing/docs)
[![Build](https://img.shields.io/github/checks-status/parseablehq/parseable/main?style=flat&color=%2373DC8C&label=Checks)](https://github.com/parseablehq/parseable/actions)

[Key Concepts](https://www.parseable.com/docs/concepts) | [Features](https://github.com/parseablehq/parseable#rocket-highlights) | [Documentation](https://www.parseable.com/docs) | [Demo](https://demo.parseable.com/login?q=eyJ1c2VybmFtZSI6ImFkbWluIiwicGFzc3dvcmQiOiJhZG1pbiJ9) | [FAQ](https://www.parseable.com/docs/faq)

</div>

Parseable is a disk **_less_**, cloud native database for logs, observability, security, and compliance. Parseable is built with focus on simplicity & resource efficiency. Parseable is useful for use cases where **complete data ownership, data security and privacy are paramount**.

To experience Parseable UI, checkout [demo.parseable.com ↗︎](https://demo.parseable.com/login?q=eyJ1c2VybmFtZSI6ImFkbWluIiwicGFzc3dvcmQiOiJhZG1pbiJ9).

## Quickstart :zap:

### Run Parseable

<details>
<summary><a href="https://www.parseable.com/docs/server/get-started/docker-quick-start">Docker Image</a></summary>
<p>

Get started with Parseable Docker with a single command:

```bash
docker run -p 8000:8000 \
  parseable/parseable:latest \
  parseable local-store
```

</p>
</details>

<details>
<summary><a href="https://www.parseable.com/docs/server/get-started/binary-quick-start">Executable Binary</a></summary>
<p>

Download and run the Parseable binary on your laptop:

- Linux or MacOS

```bash
curl -fsSL https://logg.ing/install | bash
```

- Windows

```pwsh
powershell -c "irm https://logg.ing/install-windows | iex"
```

</p>
</details>

### Ingestion and query

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

Access the Parseable UI at [http://localhost:8000 ↗︎](http://localhost:8000). You can login to the dashboard default credentials `admin`, `admin`.

## Getting started :bulb:

For quickstart, refer the [quickstart section ↗︎](#quickstart-zap).

This section elaborates available options to run Parseable in production or development environments.

- Distributed Parseable on Kubernetes: [Helm Installation](https://www.parseable.com/docs/server/installation/distributed/setup-distributed-parseable-on-kubernetes-via-helm).
- Distributed Parseable on AWS EC2 / VMs / Linux: [Binary Installation](https://www.parseable.com/docs/server/installation/distributed/setup-systemd-service-for-distributed-parseable-server).

## Features :rocket:

- [High availability & Cluster mode ↗︎](https://www.parseable.com/docs/concepts/distributed-architecture)
- [Hot Tier ↗︎](https://www.parseable.com/docs/features/tiering)
- [Alerts ↗︎](https://www.parseable.com/docs/alerts)
- [Role based access control ↗︎](https://www.parseable.com/docs/rbac)
- [OAuth2 support ↗︎](https://www.parseable.com/docs/oidc)
- [LLM integration ↗︎](https://www.parseable.com/docs/integrations/llm-based-sql-generation)
- [OpenTelemetry support ↗︎](https://www.parseable.com/docs/opentelemetry)

## How do people use Parseable :bulb:

- **Log Analytics** - Not all logs are created equal. For example application logs are seldom useful after a few days pass, but if same application also logs all the user interactions, that data is very valuable for product managers, and can be stored for a longer period. Several businesses store such high value logs and slice / dice them as needed.

- **Audit & Compliance** - Organizations that need to store logs in a secure, compliant manner. Parseable's direct to S3 bucket storage mode ensures that logs are stored in a secure, cost effective manner, and can be accessed only by authorized users, while all the data is queryable in real-time.

- **Observability & Monitoring** - A very large chunk of observability data is logs. Organizations that need to monitor their systems, applications, and infrastructure in real-time use Parseable as the primary log storage system so they get timely alerts, and can analyze logs in real-time.

## Motivation :dart:

Traditionally, logging has been seen as a text search problem. Log volumes were not high, and data ingestion or storage were not really issues. This led us to today, where all the logging platforms are primarily text search engines.

But with log data growing exponentially, today's log data challenges involve whole lot more – Data ingestion, storage, and observation, all at scale. We are building Parseable to address these challenges.

## Verify images :writing_hand:

Parseable builds are attested for build provenance and integrity using the [attest-build-provenance](https://github.com/actions/attest-build-provenance) action. The attestations can be verified by having the latest version of [GitHub CLI](https://github.com/cli/cli/releases/latest) installed in your system. Then, execute the following command:

```sh
gh attestation verify PATH/TO/YOUR/PARSEABLE/ARTIFACT-BINARY -R parseablehq/parseable
```

## Contributing :trophy:

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

[Contribution guide ↗︎](https://www.parseable.com/docs/contributing)
