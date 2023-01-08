## Installation

This document explains the steps required to setup systemd service for Parseable server, in local store mode. For production setup, we recommend using a object store like S3, please refer to [S3 setup guide](https://github.com/parseablehq/parseable/tree/main/deploy/linux-systemd/s3.md).

## Prerequisites

- Parseable binary available at `/usr/local/bin/`. Download the relevant binary from https://github.com/parseablehq/parseable/releases.

## Create configuration

```sh
cat <<EOT >> /etc/default/parseable
P_USERNAME="parseable"
P_PASSWORD="parseable"
P_ADDR="0.0.0.0:8000"
P_STAGING_DIR="/var/lib/parseable/staging"
P_FS_DIR="/var/lib/parseable/data"
EOT
```

## Systemctl

Download `parseable.service` in  `/etc/systemd/system/`
```
( cd /etc/systemd/system/; curl -O https://raw.githubusercontent.com/parseablehq/parseable/main/deploy/linux-systemd/parseable.local.service )
```

Note: If you want to bind to a port < 1024 with the service running as a regular user, you will need to add bind capability via the AmbientCapabilities directive in the parseable.service file:

```
[Service]
AmbientCapabilities=CAP_NET_BIND_SERVICE
WorkingDirectory=/usr/local/
```

## Enable startup on boot

```
systemctl enable parseable.local.service
```

## Disable Parseable service

```
systemctl disable parseable.local.service
```
