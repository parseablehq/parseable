# Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

FROM python:3.13-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

RUN pip install confluent-kafka
RUN pip install faker

WORKDIR /app
COPY kafka_log_stream_generator.py /app/

CMD ["python", "/app/kafka_log_stream_generator.py"]
