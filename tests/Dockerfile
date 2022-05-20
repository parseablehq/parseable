#!/bin/sh
#
# Parseable Server (C) 2022 Parseable, Inc.
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
#

FROM registry.access.redhat.com/ubi8/ubi:8.1

WORKDIR /tests

COPY main.sh .
COPY testcases/smoke_test.sh ./testcases/

RUN yum -y install wget

RUN wget https://github.com/mingrammer/flog/releases/download/v0.4.3/flog_0.4.3_linux_amd64.tar.gz \
  && tar -xvf flog_0.4.3_linux_amd64.tar.gz \
  && cp flog /usr/local/bin

ENTRYPOINT ./main.sh smoke http://api.parseable.io
