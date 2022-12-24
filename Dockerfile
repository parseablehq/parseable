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

# Compile
FROM    rust:1.65-alpine AS compiler

WORKDIR /parseable

RUN     apk add --no-cache musl-dev

COPY . .

RUN     set -eux; \
        apkArch="$(apk --print-arch)"; \
        if [ "$apkArch" = "aarch64" ]; then \
            export JEMALLOC_SYS_WITH_LG_PAGE=16; \
        fi && \
        cargo build --release

# Run
FROM    alpine:3.16

RUN     apk update --quiet \
        && apk add -q --no-cache tini

# Create appuser
ENV     USER=parseable
ENV     UID=10001

RUN     adduser \
        --disabled-password \
        --gecos "" \
        --home "/nonexistent" \
        --shell "/sbin/nologin" \
        --no-create-home \
        --uid "${UID}" \
        "${USER}"

WORKDIR /parseable

COPY    --from=compiler /parseable/target/release/parseable /bin/parseable
RUN     chown -R parseable /parseable/

USER    parseable:parseable

EXPOSE  8000/tcp

ENTRYPOINT  ["tini", "--"]
CMD     /bin/parseable
