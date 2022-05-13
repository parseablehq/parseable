## Installation

This document explains the steps required to setup systemd service for Parseable server.

## Prerequisites

- S3 or a compatible object storage URL.
- Credentials to read / write access the object storage.
- Bucket name created on object storage.
- Parseable binary available at `/usr/local/bin/`. Find and download the relevant binary from https://github.com/parseablehq/parseable/releases.

## Create configuration

```sh
cat <<EOT >> /etc/default/parseable
P_S3_BUCKET=<s3-bucket>
P_S3_ACCESS_KEY=<access-key>
P_S3_SECRET_KEY=<secret-key>
P_S3_REGION=<region>
P_S3_URL=<s3-url>
EOT
```

## Systemctl

Download `parseable.service` in  `/etc/systemd/system/`
```
( cd /etc/systemd/system/; curl -O https://raw.githubusercontent.com/parseablehq/parseable/main/deploy/systemd/parseable.service )
```

Note: If you want to bind to a port < 1024 with the service running as a regular user, you will need to add bind capability via the AmbientCapabilities directive in the parseable.service file:

```
[Service]
AmbientCapabilities=CAP_NET_BIND_SERVICE
WorkingDirectory=/usr/local/
```

### Enable startup on boot
```
systemctl enable parseable.service
```

### Disable Parseable service
```
systemctl disable parseable.service
```
