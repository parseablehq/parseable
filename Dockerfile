# Parseable Server (C) 2022 - 2023 Parseable, Inc.
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


FROM rust:slim-bullseye as build

LABEL org.opencontainers.image.title="Parseable"
LABEL maintainer="Parseable Team <hi@parseable.io>"
LABEL org.opencontainers.image.vendor="Cloudnatively Pvt Ltd"
LABEL org.opencontainers.image.licenses="AGPL-3.0"

WORKDIR /parseable

COPY . .
RUN cargo build --release

RUN mkdir -p /app/lib && \
    cp -LR $(ldd /parseable/target/release/router | grep "=>" | cut -d ' ' -f 3) /app/lib

FROM gcr.io/distroless/cc-debian11:nonroot

WORKDIR /parseable

COPY --from=build   /app/lib /app/lib
COPY --from=build   /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2
COPY --from=build   /app/target/release/router /app/router
COPY --from=build   /parseable/target/release/parseable /usr/bin/parseable

ENV LD_LIBRARY_PATH=/app/lib
CMD ["parseable"]
