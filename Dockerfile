# Parseable Server (C) 2022 - 2024 Parseable, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# build stage
FROM rust:1.83.0-alpine AS builder

LABEL org.opencontainers.image.title="Parseable"
LABEL maintainer="Parseable Team <hi@parseable.io>"
LABEL org.opencontainers.image.vendor="Parseable Inc"
LABEL org.opencontainers.image.licenses="AGPL-3.0"

# Install dependencies for build
RUN apk add --no-cache musl-dev gcc git

WORKDIR /parseable
COPY . .
RUN cargo build --release

# Final stage with a minimal runtime image
FROM alpine:latest

WORKDIR /parseable

# Copy the static shell into base image.
COPY --from=builder /parseable/target/release/parseable /usr/bin/parseable

CMD ["/usr/bin/parseable"]
