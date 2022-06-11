## Parseable integration tests

This directory contains integration tests for Parseable server. Tests are written in shell script and bundled in a container. 

### Build test container locally

```
docker build . -t parseable/test:edge
```

### Running tests

```
docker run parseable/test:edge smoke http://api.parseable.io
```
