Parseable
==========

<img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg#gh-light-mode-only" alt="Parseable logo" height="100px" align="right" />
<img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png#gh-dark-mode-only" alt="Parseable logo" height="100px" align="right" />

Parseable is a cloud native, log storage and analysis platform. Parseable is indexing free by design. Written in Rust, Parseable can be deployed on Baremetal, VMs and Kubernetes.

It ingests log data via HTTP POST calls and exposes a query API to search and analyze logs. It is compatible with logging agents like FluentBit, LogStash, FileBeat among others.

[![GitHub stars](https://img.shields.io/github/stars/parseablehq/parseable.svg?style=for-the-badge&label=Stars&logo=github)](https://github.com/parseablehq/parseable)
[![Twitter Follow](https://img.shields.io/twitter/follow/parseableio.svg?style=for-the-badge&label=Follow&logo=twitter)](https://twitter.com/parseableio)
[![Slack](https://img.shields.io/badge/join%20slack-parseable-brightgreen.svg?style=for-the-badge&label=Follow&logo=slack)](https://launchpass.com/parseable)
[![FOSSUnited](http://fossunited.org/files/fossunited-badge.svg)](https://fossunited.org/)

<h4 align="center">
  <a href="https://www.parseable.io/docs/quick-start" target="_blank">Quick Start</a> |
  <a href="https://www.parseable.io/docs/introduction" target="_blank">Documentation</a> |
  <a href="https://demo.parseable.io" target="_blank">Live Demo</a>
  <br>
</h4>

<div style="position: relative; padding-bottom: calc(61.75000000000001% + 41px); height: 0;"><iframe src="https://demo.arcade.software/dNcJyqEm3avSE5iYdiiy?embed" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

## Features

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

## Getting Started

### Docker
Parseable docker image is available on [Docker hub](https://hub.docker.com/r/parseable/parseable). 

```sh
mkdir -p /tmp/parseable
docker run \
  -p 8000:8000 \
  -v /tmp/parseable:/data \
  parseable/parseable:latest
```

### Kubernetes
Parseable helm chart is available at [Parseable charts repository](https://charts.parseable.io/). 

```sh
helm repo add parseable https://charts.parseable.io/
kubectl create namespace parseable
helm install parseable parseable/parseable --namespace parseable
```

### Binary
Parseable binary is available on [Github releases](https://github.com/parseablehq/parseable/releases). Please download the latest release for your platform. 

```sh
chmod +x parseable
./parseable
```

<h1></h1>

Parseable dashboard is available at [http://localhost:8000](http://localhost:8000). Default username and password is `parseable`.

By default Parseable uses a public bucket to store the data. Please change the object storage credentials to your own bucket, before using Parseable.

:memo: Parseable is in alpha stage and will evolve over time. There may be breaking changes between releases. Please give us your feedback in [Slack](https://launchpass.com/parseable), or [Issues](https://github.com/parseablehq/parseable/issues/new).

### Configuration

Parseable can be configured using environment variables listed below, with sample values.

```sh
export P_S3_URL="https://minio.parseable.io:9000"
export P_S3_ACCESS_KEY="minioadmin"
export P_S3_SECRET_KEY="minioadmin"
export P_S3_REGION="us-east-1"
export P_S3_BUCKET="parseable"
export P_LOCAL_STORAGE="./data"
export P_USERNAME="parseable"
export P_PASSWORD="parseable"
```

## Live Demo 

Try out Parseable server with our demo instance. Send log data to default log stream `frontend`

```sh
curl --location --request POST 'https://demo.parseable.io/api/v1/logstream/frontend' \
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

Access the Parseable dashboard to verify the log data is present

<table>
<tr>
    <td>URL</td>
    <td><a href="https://demo.parseable.io" target="_blank">https://demo.parseable.io</a></td>
</tr>
<tr>
    <td>Username</td>
    <td>parseable</td>
</tr>
<tr>
    <td>Password</td>
    <td>parseable</td>
</tr>
</table>

For complete Parseable API documentation, refer to [Parseable API workspace on Postman](https://www.postman.com/parseable/workspace/parseable/overview).

:exclamation: Please do not store any sensitive data on this server as the data is openly accessible. We'll delete the data on this server periodically.

## Contributing 

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

## License

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[https://www.gnu.org/licenses/agpl-3.0.txt](https://www.gnu.org/licenses/agpl-3.0.txt)
