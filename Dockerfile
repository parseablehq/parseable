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

FROM rust:1.67.0 as builder

WORKDIR /parseable

COPY . .

RUN cargo build --release
 
FROM gcr.io/distroless/cc:latest

WORKDIR /parseable

COPY --from=builder /lib/x86_64-linux-gnu/liblzma.so* /lib/x86_64-linux-gnu/
COPY --from=builder /parseable/target/release/parseable /usr/local/bin/parseable

CMD ["/usr/local/bin/parseable"]
