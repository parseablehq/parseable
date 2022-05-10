# Copyright (C) 2022 Parseable, Inc.
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
FROM    rust:alpine3.14 AS compiler

RUN     apk add -q --update-cache --no-cache build-base openssl-dev

WORKDIR /parseable

COPY    . .
RUN     set -eux; \
        apkArch="$(apk --print-arch)"; \
        if [ "$apkArch" = "aarch64" ]; then \
            export JEMALLOC_SYS_WITH_LG_PAGE=16; \
        fi && \
        cargo build --release

# Run
FROM    alpine:3.14

RUN     apk update --quiet \
        && apk add -q --no-cache libgcc curl

# add parseable to the `/bin` so you can run it from anywhere and it's easy
# to find.
COPY    --from=compiler /parseable/target/release/parseable /bin/parseable

# This directory should hold all the data related to parseable so we're going
# to move our PWD in there.
WORKDIR /parseable/data

EXPOSE  5678/tcp

CMD    ["/bin/parseable"]
