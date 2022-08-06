<p align="center">
  <a href="https://www.parseable.io" target="_blank"><img src="https://raw.githubusercontent.com/parseablehq/docs/main/static/img/logo.svg" alt="Parseable" width="600" height="150" /></a>
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
</h4>

Parseable is an open source, cloud native, log storage and management platform. 

Parseable helps you ingest high volumes of log data from various sources (Fluentbit, Logstash etc or directly from applications). Parseable stores log data into highly compressed Parquet file format. With object storage as primary storage for Parseable, you get seamless scale and flexibility for ever growing log data.

<p align="center">
  <img src="https://github.com/parseablehq/docs/raw/main/static/img/overview.png" alt="Parseable Overview" width="800" height="650" />
</p>

<h1></h1>

## Live Demo 

Try out Parseable server with our demo instance.

1. Post log data to default log stream `frontend`

```sh
curl --location --request POST 'https://demo.parseable.io/api/v1/logstream/frontend' \
--header 'X-P-META-label1: value1' \
--header 'X-P-META-label2: value2' \
--header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==' \
--header 'Content-Type: application/json' \
--data-raw '[
    {
        "log": "{\"id\": 4, \"host\":\"153.10.110.81\", \"user-identifier\":\"-\", \"datetime\":\"24/Jun/2022:14:12:15 +0000\", \"method\": \"GET\", \"request\": \"/virtual/drive\", \"protocol\":\"HTTP/2.0\", \"status\":500, \"bytes\":21969, \"referer\": \"http://www.seniordisintermediate.net/mesh/users\"}",
        "meta_Host": "10.116.0.3",
        "meta_Source": "10.244.0.147",
        "meta_ContainerName": "log-generator"
    }
]'
```

2. Then access the Parseable dashboard to verify the log data is present

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

**Note**: Please do not store any sensitive data on this server as the data is openly available. We'll delete the data on this server periodically.
## Why Parseable

* Indexing free design.
* Highly compressed log data, stored in [Parquet](https://parquet.apache.org) - columnar, open data format. Query logs via Parseable or bring your own analytics platform.
* Stateless design allowing seamless scale and flexibility.
* Own your log data on your object storage buckets.
* SDK less, simple REST API calls for log ingestion.

## Features

* Kubernetes native design, allowing to get started with pod logs in minutes.
* Filter log data on tags.
* Query for specific keywords and fields.
* Intuitive dashboard to parse and query the log data.
