# <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg#gh-light-mode-only" height=30 style="height: 2rem">
# <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logodark.png#gh-dark-mode-only" height=30 style="height: 2rem">

<p align="center">
  <a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
  <img src="https://img.shields.io/github/commit-activity/m/parseablehq/parseable" alt="commits activity monthly">
  <a href="https://launchpass.com/parseable" target="_blank"><img src="https://img.shields.io/badge/join%20slack-parseable-brightgreen.svg" alt="join slack"></a>
  <a href="https://github.com/parseablehq/parseable/stargazers" target="_blank"><img src="https://img.shields.io/github/stars/parseablehq/parseable?style=social" alt="Github stars"></a>
  <a href="https://twitter.com/parseableio" target="_blank"><img src="https://img.shields.io/twitter/follow/parseableio" alt="Twitter"></a>
</p>

<h4 align="center">
  <a href="https://www.parseable.io/docs/quick-start" target="_blank">Quick Start</a> |
  <a href="https://www.parseable.io/docs/introduction" target="_blank">Documentation</a> |
  <a href="https://demo.parseable.io" target="_blank">Live Demo</a>
  <br>
</h4>

Parseable is a cloud native, log storage and observability platform. Parseable is written in Rust and index free by design. Parseable is available as a single binary / Docker image and can be deployed on a wide range of platforms.

It ingests log data via HTTP POST calls and exposes a query API to search and analyze logs. It is compatible with all the standard logging agents via HTTP output plugins.

<img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/console.gif" />

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
Parseable docker image is available on [Docker hub](https://hub.docker.com/r/parseable/parseable). Please change the environment variables as relevant. 

```sh
cat << EOF > parseable-env
P_S3_URL=https://minio.parseable.io:9000
P_S3_ACCESS_KEY=minioadmin
P_S3_SECRET_KEY=minioadmin
P_S3_REGION=us-east-1
P_S3_BUCKET=parseable
P_LOCAL_STORAGE=/data
P_USERNAME=parseable
P_PASSWORD=parseable
EOF

mkdir -p /tmp/data
docker run \
  -p 8000:8000 \
  --env-file parseable-env \
  -v /tmp/data:/data \
  parseable/parseable:latest \
  parseable server
```

### Kubernetes
Parseable helm chart is available at [Parseable charts repository](https://charts.parseable.io/). 

```sh
helm repo add parseable https://charts.parseable.io/
helm repo update
kubectl create namespace parseable
helm install parseable parseable/parseable --namespace parseable --set parseable.demo=true
```

### Binary
Parseable binary is available on [Github releases](https://github.com/parseablehq/parseable/releases). Please download the latest release for your platform, also make sure to change the environment variables as relevant. 

```sh
export P_S3_URL="https://minio.parseable.io:9000"
export P_S3_ACCESS_KEY="minioadmin"
export P_S3_SECRET_KEY="minioadmin"
export P_S3_REGION="us-east-1"
export P_S3_BUCKET="parseable"
export P_LOCAL_STORAGE="./data"
export P_USERNAME="parseable"
export P_PASSWORD="parseable"
chmod +x parseable
./parseable
```

<h1></h1>

Parseable dashboard is available at [http://localhost:8000](http://localhost:8000). Credentials to login to the dashboard are the values you set in the environment variables.

:memo: Parseable is in alpha stage and will evolve over time. There may be breaking changes between releases. Please give us your feedback in [Slack](https://launchpass.com/parseable), or [Issues](https://github.com/parseablehq/parseable/issues/new).

## Using Parseable
`<stream-name>` is the name of the stream you want to create. `<basic-auth-header>` is the basic auth header value generated from username & password of the user you created in the environment variables. You can generate the basic auth header value using the following command.

```sh
echo -n '<user-name>:<password>' | base64
```

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

For complete Parseable API documentation, refer to [Parseable API Ref Docs](https://www.parseable.io/docs/api-reference).

## Live Demo 

You can also try out Parseable on our [https://demo.parseable.io](https://demo.parseable.io). Credentials to login to the dashboard are `parseable` / `parseable`.
                                                                              
## Contributing

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>
