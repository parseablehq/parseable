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

FROM    alpine:3.14

RUN     apk update --quiet \
        && apk add -q --no-cache libgcc curl
        
# Create appuser
ENV USER=parseable
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

# This directory should hold all the data related to parseable so we're going
# to move our PWD in there.
WORKDIR /parseable

ADD     https://github.com/parseablehq/parseable/releases/latest/download/parseable_linux_x86_64 /bin/parseable

USER    parseable:parseable

EXPOSE  8000/tcp

CMD     ["/bin/parseable"]
