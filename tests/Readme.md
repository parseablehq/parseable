## Parseable integration tests

This directory contains integration tests for Parseable server. Tests are written in shell script and bundled in a container. 

### Build test container locally

```
docker build . -t parseable/test:edge
```

### Running tests

```
docker run parseable/test:edge
```

You should get an output like this

```
Executing smoke test
Creating Stream: dded6ce88f64383ed6f6
Creating input.json file with 50 events
/tests/input.json is created.
Preparing input.json file to be used as raw data for POST
Posting 50 events into dded6ce88f64383ed6f6
Getting the list of streams
Querying the stream: dded6ce88f64383ed6f6
Number of events stored in dded6ce88f64383ed6f6: 50
Validation successful. Count of events returned from query is same as the ones posted.
Deleting the /tests/input.json file
```
