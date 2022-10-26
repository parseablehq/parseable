<p align="center">
  <span">
    <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg#gh-light-mode-only" alt="Parseable" width="400" height="80" />
    <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png#gh-dark-mode-only" alt="Parseable" width="400" height="80" />
  </a> 
</p>

<h4 align="center">
  <p> Parseable is an open source log storage and observability platform, built for Kubernetes. </p>
  <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/console.png" />
  <a href="https://www.parseable.io/docs/quick-start" target="_blank">Quick Start</a> |
  <a href="https://www.parseable.io/docs/introduction" target="_blank">Documentation</a> |
  <a href="https://launchpass.com/parseable" target="_blank">Community</a> |
  <a href="https://demo.parseable.io" target="_blank">Live Demo</a>
</h4>

## Why?

As SREs, DevOps deploy and manage more and more cloud native applications, there is a glaring gap in the ecosystem for a developer friendly, cloud native, event & log observability platform. We faced this issue first hand at our jobs, and we've seen several other large scale Kubernetes users relating to this gap.

Parseable started because of this _missing_ case. We intend to enrich the ecosystem with a developer friendly, cloud native, event & log observability platform.

#### Key Differentiators

- Written in Rust. Low CPU & memory footprint, with low latency, high throughput.
- Open data format (Parquet). Complete ownership of data. Wide range of possibilities for data analysis.
- Single binary / container based deployment (including UI). Deploy in minutes if not seconds.
- Indexing free design. Lower CPU and storage overhead. Similar levels of performance as indexing based systems.

#### How it works?

Parseable exposes REST API to ingest and query log data. Under the hood, it uses Apache Arrow and Parquet to handle and compress high volume log data. All data is stored in S3 (or compatible systems). Parseable also has a bundled web console to visualize and query log data. 

Parseable can be deployed in cloud and container based environments in minutes and can scale as the requirements grow.

## Installing

Docker is the quickest way to experience Parseable on your machine. Run the below command to deploy Parseable in demo mode.

```sh
mkdir -p /tmp/data
docker run \
  -p 8000:8000 \
  -v /tmp/data:/data \
  parseable/parseable:latest \
  parseable server --demo
```

Once this runs successfully, you'll see dashboard at [http://localhost:8000](http://localhost:8000). You can login to the dashboard with `parseable`, `parseable` as the credentials. Please make sure not to post any important data while in demo mode.

For non-demo and other installation options (Kubernetes, bare-metal), please refer to the [documentation](https://www.parseable.io/docs/category/installation).

#### Live Demo 

Instead of installing locally, you can also try out Parseable on our [Demo instance](https://demo.parseable.io). Credentials to login to the dashboard are `parseable` / `parseable`.

## Usage

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

For complete Parseable API documentation, refer to [Parseable API Docs](https://www.parseable.io/docs/category/api).
                                                                              
## Contributing

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

#### Contributors

<a href="https://github.com/parseablehq/parseable/graphs/contributors"><img src="https://contrib.rocks/image?repo=parseablehq/parseable" /></a>

#### Supported by

<a href="https://fossunited.org/" target="_blank"><img src="http://fossunited.org/files/fossunited-badge.svg"></a>
