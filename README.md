<p align="center">
  <span">
    <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo.svg#gh-light-mode-only" alt="Parseable" width="600" height="110" />
    <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/logo-dark.png#gh-dark-mode-only" alt="Parseable" width="650"/>
  </a> 
</p>

<p align="center">
  <img src="https://img.shields.io/website?down_message=red&up_color=green&up_message=online&url=https%3A%2F%2Fwww.parseable.io" alt="website status">
  <img src="https://img.shields.io/github/contributors/parseablehq/parseable" alt="contributors">
  <img src="https://img.shields.io/github/commit-activity/m/parseablehq/parseable" alt="commits activity monthly">
  <a href="https://github.com/parseablehq/parseable/stargazers" target="_blank"><img src="https://img.shields.io/github/stars/parseablehq/parseable" alt="Github stars"></a>
  <img src="https://img.shields.io/github/license/parseablehq/parseable" alt="License">  
  <a href="https://twitter.com/parseableio" target="_blank"><img src="https://img.shields.io/twitter/follow/parseableio" alt="Twitter"></a>
</p>

<h4 align="center">
  <a href="https://demo.parseable.io" target="_blank">Live Demo</a> |
  <a href="https://www.parseable.io" target="_blank">Website</a> | 
  <a href="https://www.postman.com/parseable/workspace/parseable/overview" target="_blank">API Workspace on Postman</a>
  <br>
</h4>

Parseable is a log storage and observability platform for modern, cloud native workloads. Parseable is built for high volume log data from sources like Fluent Bit, Logstash etc or directly from applications.
## Why Parseable

* Developer first platform, designed for ease of use and flexibility.
* Log data stored as [Parquet](https://parquet.apache.org) - columnar, open data format, designed for analytics.
* Stateless, index free design with object storage as primary storage.
* SDK less, simple REST API calls for log ingestion.

## Features

* SQL compatible API for querying log data.
* Intuitive dashboard to parse and query the log data.
* Bring your own analytics platform for deeper analysis of log data.
* Auto inferred schema.

<p align="center">
  <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/overview.svg#gh-light-mode-only" alt="Parseable Overview" width="800" height="650" />
  <img src="https://raw.githubusercontent.com/parseablehq/.github/main/images/overview-dark.svg#gh-dark-mode-only" alt="Parseable Overview" width="800" height="650" />
</p>

## Live Demo 

Try out Parseable server with our demo instance. Send log data to default log stream `frontend`

```sh
curl --location --request POST 'https://demo.parseable.io/api/v1/logstream/frontend' \
--header 'X-P-META-meta1: value1' \
--header 'X-P-TAGS-tag1: value1' \
--header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==' \
--header 'Content-Type: application/json' \
--data-raw '[
    {
        "id": "434a5f5e-2f5f-11ed-a261-0242ac120002",
        "datetime": "24/Jun/2022:14:12:15 +0000",
        "host": "153.10.110.81", 
        "user-identifier": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0", 
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
## Getting Started
### Docker
Parseable docker image is available on [Docker Hub](https://hub.docker.com/r/parseable/parseable). 

```sh
mkdir -p /tmp/parseable
docker run \
  -p 8000:8000 \
  -v /tmp/parseable:/data \
  parseable/parseable:v0.0.1
```

### Binary
Parseable binary is available on [Github releases](https://github.com/parseablehq/parseable/releases). Please download the latest release for your platform. 

```sh
chmod +x parseable
./parseable
```

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

## Contributing 

Refer to the contributing guide [here](https://www.parseable.io/docs/contributing).

## License

Copyright (c) 2022 [Parseable, Inc.](https://parseable.io)

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[https://www.gnu.org/licenses/agpl-3.0.txt](https://www.gnu.org/licenses/agpl-3.0.txt)
