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

[Key Concepts](https://www.parseable.com/docs/key-concepts) | [Features](https://www.parseable.com/docs/features/alerts) | [Documentation](https://www.parseable.com/docs) | [Demo](https://demo.parseable.com/login) | [FAQ](https://www.parseable.com/docs/key-concepts/data-model#faq)

</div>

Parseable is a full stack observability platform built to ingest, analyze and extract insights from all types of telemetry (MELT) data. You can run Parseable on your local machine, in the cloud, or as a managed service. To experience Parseable UI, checkout [demo.parseable.com ↗︎](https://demo.parseable.com/login).

<div align="center">
  <a href="http://www.youtube.com/watch?feature=player_embedded&v=gYn3pFAfrVA" target="_blank">
  <img src="http://img.youtube.com/vi/gYn3pFAfrVA/mqdefault.jpg" alt="Watch the video" width="300" height="240" />
  </a>
</div>

## Quickstart :zap:

### Run Parseable

<details>
<summary><a href="https://www.parseable.com/docs/quickstart/docker">Docker Image</a></summary>
<p>

Get started with Parseable Docker image with a single command:

```bash
docker run -p 8000:8000 \
  parseable/parseable:latest \
  parseable local-store
```

</p>
</details>

<details>
<summary><a href="https://www.parseable.com/docs/quickstart/binary">Executable Binary</a></summary>
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

Access the UI at [http://localhost:8000 ↗︎](http://localhost:8000). You can login to the dashboard default credentials `admin`, `admin`.

## Getting started :bulb:

For quickstart, refer the [quickstart section ↗︎](#quickstart-zap).

This section elaborates available options to run Parseable in production or development environments.

- Distributed Parseable on Kubernetes: [Helm Installation](https://www.parseable.com/docs/installation/distributed/k8s-helm).
- Distributed Parseable on AWS EC2 / VMs / Linux: [Binary Installation](https://www.parseable.com/docs/installation/distributed/linux).

## Features :rocket:

- [High availability & Cluster mode ↗︎](https://www.parseable.com/docs/key-concepts/high-availability)
- [Smart cache ↗︎](https://www.parseable.com/docs/features/smart-cache)
- [Alerts ↗︎](https://www.parseable.com/docs/features/alerts)
- [Role based access control ↗︎](https://www.parseable.com/docs/features/rbac)
- [OAuth2 support ↗︎](https://www.parseable.com/docs/features/oepnid)
- [OpenTelemetry support ↗︎](https://www.parseable.com/docs/OpenTelemetry/logs)

## Verify images :writing_hand:

Parseable builds are attested for build provenance and integrity using the [attest-build-provenance](https://github.com/actions/attest-build-provenance) action. The attestations can be verified by having the latest version of [GitHub CLI](https://github.com/cli/cli/releases/latest) installed in your system. Then, execute the following command:

```sh
gh attestation verify PATH/TO/YOUR/PARSEABLE/ARTIFACT-BINARY -R parseablehq/parseable
```

## Contributing :trophy:

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

[Contribution guide ↗︎](https://github.com/parseablehq/parseable/blob/main/CONTRIBUTING.md)
