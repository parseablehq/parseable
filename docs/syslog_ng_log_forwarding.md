# Syslog-ng log forwarding to Parseable


Syslog-ng implement [syslog](https://en.wikipedia.org/wiki/Syslog) protocol for Unix and Unix-like systems, 
to forward syslog-ng log to Parseable you can follow the guide below.

## Syslog-ng setup

For this documentation we use syslog-ng in form of docker image, first setup docker-compose file:

```
---
version: "2.1"
services:
  syslog-ng:
    image: lscr.io/linuxserver/syslog-ng:latest
    container_name: syslog-ng
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=Europe/London
    volumes:
      - /your/local/path/syslog-ng/config:/config
      - /your/local/path/syslog-ng/log:/var/log #optional
    ports:
      - 514:5514/udp
      - 601:6601/tcp
      - 6514:6514/tcp
    restart: unless-stopped
```

Run `docker-compose up` and syslog-ng will listen on port UDP `514` and in `/your/local/path/syslog-ng/config`
path will contain default configuration file, we only need to customize destination log in main config file,
`syslog-ng.conf`.

```
$ cat syslog-ng.conf
############################################################################
# Default syslog-ng.conf file which collects all local logs into a
# single file called /var/log/messages tailored to container usage.

@version: 3.35
@include "scl.conf"

source s_local {
  internal();
};

source s_network_tcp {
  syslog(transport(tcp) port(6601));
};

source s_network_udp {
  syslog(transport(udp) port(5514));
};

destination d_local {
  file("/var/log/messages");
  file("/var/log/messages-kv.log" template("$ISODATE $HOST $(format-welf --scope all-nv-pairs)\n") frac-digits(3));
};

log {
  source(s_local);
  source(s_network_tcp);
  source(s_network_udp);
  destination(d_local);
};
```



## Sending syslog log

First ensure log stream in Parseable already created.

```
$ curl --request PUT \
  --url http://localhost:8000/api/v1/logstream/{logstream-name} \
  --header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ=='
```

Add following section in `syslog-ng.conf` file:

```
destination d_http {
    http(
        url("http://host.docker.internal:8000/api/v1/logstream/logstream-name")
        method("POST")
        user-agent("syslog-ng User Agent")
        user("parseable")
        password("parseable")
        headers("X-P-META-Host: 192.168.1.10", "X-P-TAGS-Language: syslog")
        headers("Content-Type: application/json")
        body-suffix("\n")
        body('$(format-json --scope rfc5424 --key ISODATE)')
    );
};
```

and then in `log` section add `d_http` as destination:

```
log {
  source(s_local);
  source(s_network_tcp);
  source(s_network_udp);
  destination(d_local);
  destination(d_http);
};
```

`url()` define the address of parseable server, use `host.docker.internal` if you following 
docker compose step. `user(), password()` to define HTTP Basic Auth, required by Parseable to authenticate
sending log request. `headers()` to customize custom header and set content-type of the payload. And
finally `body('$(format-json --scope rfc5424 --key ISODATE)')` to format syslog as JSON.

After editing `syslog-ng.conf` restart the docker-compose instance.


## Testing

To test you can use following python script.

```python
import logging
import logging.handlers

my_logger = logging.getLogger('MyLogger')
my_logger.setLevel(logging.DEBUG)

handler = logging.handlers.SysLogHandler(address=('127.0.0.1', 514))

my_logger.addHandler(handler)

my_logger.debug('test-app this is debug')
my_logger.critical('test-app this is critical')
```

Execute those code and then you can query Parseable using

```
$ curl --request POST \
  --url http://localhost:8000/api/v1/query \
  --header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==' \
  --header 'Content-Type: application/json' \
  --data '{
	"startTime": "2022-10-16T13:11:00+00:00",
	"query": "select * from logstream-name limit 10",
	"endTime": "2022-10-16T13:21:00+00:00"
}'
```

Or view it in Parseable dashboard.