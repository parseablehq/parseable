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

parseable_url=$1
stream_name=$2
events=50
input_file=$PWD/input.json
curl_std_opts=( -sS --header 'Content-Type: application/json' --header 'Authorization: Basic cGFyc2VhYmxlOnBhcnNlYWJsZQ==' -w '\n\n%{http_code}' )
alert_body='{"alerts":[{"name":"server-fail-alert1","message":"server reported error status","rule":{"field":"http_status","contains":"500","repeats":"5","within":"10m"},"target":[{"name":"slack-target","server_url":"http://mailgun.com","api_key":"xxxx"}]}]}'
schema_body='{"fields":[{"name":"host","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"user-identifier","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"datetime","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"method","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"request","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"protocol","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"status","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"bytes","data_type":"Int64","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"referer","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false},{"name":"labels","data_type":"Utf8","nullable":true,"dict_id":0,"dict_is_ordered":false}]}'

# Generate events using flog (https://github.com/mingrammer/flog) and store it in input.json file
create_input_file () {
  flog -f json -n "$events" -t log -o "$input_file"
  sleep 2
  sed -i '1s/^/[/;$!s/$/,/;$s/$/]/' "$input_file"
  return $?
}

# Create stream
create_stream () {
  printf "Test create_stream: running\n"
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/logstream/"$stream_name")
  
  if [ $? -ne 0 ]; then
    echo "Failed to create log stream $stream_name with exit code: $?"
    printf "Test create_stream: failed\n"
    exit 1
  fi
  
  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to create log stream $stream_name with http code: $http_code and response: $content"
    printf "Test create_stream: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "Created log stream $stream_name" ]; then
    echo "Failed to create log stream $stream_name with response: $content"
    printf "Test create_stream: failed\n"
    exit 1
  fi

  printf "Test create_stream: successful\n"
  return 0
}

# Post log data to the stream
post_event_data () {
  printf "Test post_event_data: running\n"
  create_input_file
  if [ $? -ne 0 ]; then
    echo "Failed to create log data to be posed to $stream_name with exit code: $?"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  content=$(cat "$input_file")
  
  response=$(curl "${curl_std_opts[@]}" --request POST "$parseable_url"/api/v1/logstream/"$stream_name" --data-raw "$content")
  if [ $? -ne 0 ]; then
    echo "Failed to post log data to $stream_name with exit code: $?"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to create log stream $stream_name with http code: $http_code and response: $content"
    printf "Test post_event_data: failed\n"
    exit 1
  fi
  
  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "Successfully posted $events events" ]; then
    echo "Failed to post log data to $stream_name with response: $content"
    printf "Test post_event_data: failed\n"
    exit 1
  fi

  printf "Test post_event_data: successful\n"
  return 0
}

# List all log stream and [TODO] verify if the stream is created
list_log_streams () {
  printf "Test list_log_streams: running\n"

  #List Streams
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream)
  if [ $? -ne 0 ]; then
    echo "Failed to list log streams with exit code: $?"
    printf "Test list_log_streams: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to list all log streams with http code: $http_code and response: $content"
   printf "Test list_log_streams: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  echo "$content"

  printf "Test list_log_streams: successful\n"
  return 0
}

# Get Stream's schema and [TODO] validate its schema
get_streams_schema () {
  printf "Test get_streams_schema: running\n"
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream/"$stream_name"/schema)
  if [ $? -ne 0 ]; then
    echo "Failed to fetch stream's schema with exit code: $?"
    printf "Test get_streams_schema: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to get schema for stream $stream_name with http code: $http_code and response: $content"
    printf "Test get_streams_schema: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "$schema_body" ]; then
    echo "Get schema response doesn't match with expected schema."
    echo "Schema expected: $schema_body"
    echo "Schema returned: $content"
    printf "Test get_streams_schema: failed\n"
    exit 1
  fi

  printf "Test get_streams_schema: successful\n"
  return 0
}

# Query the log stream and verify if count of events is equal to the number of events posted
query_log_stream() {
  printf "Test query_log_stream: running\n"
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/query --data-raw '{
    "query": "select count(*) from '$stream_name'"
  }')
  if [ $? -ne 0 ]; then
    echo "Failed to query log data from $stream_name with exit code: $?"
    printf "Test query_log_stream: successful\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to query stream $stream_name with http code: $http_code and response: $content"
    printf "Test query_log_stream: successful\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  queryResult=$(echo "$content" | cut -d ':' -f2 | cut -d '}' -f1)
  if [ "$queryResult" != $events ]; then
    echo "Validation failed. Count of events returned from query does not match with the ones posted."
    printf "Test query_log_stream: failed\n"
    exit 1
  fi
  printf "Test query_log_stream: successful\n"
  return 0
}

# Set Alert
set_alert () {
  printf "Test set_alert: running\n"

  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/logstream/"$stream_name"/alert --data-raw "$alert_body")
  if [ $? -ne 0 ]; then
    echo "Failed to set alert for $stream_name with exit code: $?"
    printf "Test set_alert: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to set alert for $stream_name with http code: $http_code and response: $content"
    printf "Test set_alert: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "Set alert configuration for log stream $stream_name" ]; then
    echo "Failed to set alert on log stream $stream_name with response: $content"
    printf "Test set_alert: failed\n"
    exit 1
  fi

  printf "Test set_alert: successful\n"
  return 0
}

# Get Alert
get_alert () {
  printf "Test get_alert: running\n"

  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream/"$stream_name"/alert)
  if [ $? -ne 0 ]; then
    echo "Failed to get alert for $stream_name with exit code: $?"
    printf "Test get_alert: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to get alert for $stream_name with http code: $http_code and response: $content"
    printf "Test get_alert: failed\n"
    exit 1
  fi
  
  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "$alert_body" ]; then
    echo "Get alert response doesn't match with Alert config returned."
    echo "Alert set: $alert_body"
    echo "Alert returned: $content"
    printf "Test get_alert: failed\n"
    exit 1
  fi
  printf "Test get_alert: successful\n"
  return 0
}

# Delete stream
delete_stream () {
  printf "Test delete_stream: running\n"

  response=$(curl "${curl_std_opts[@]}" --request DELETE "$parseable_url"/api/v1/logstream/"$stream_name")

  if [ $? -ne 0 ]; then
    echo "Failed to delete log stream $stream_name with exit code: $?"
    printf "Test delete_stream: failed\n"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to delete log stream $stream_name with http code: $http_code and response: $content"
    printf "Test delete_stream: failed\n"
    exit 1
  fi

  content=$(sed '$ d' <<< "$response")
  if [ "$content" != "log stream $stream_name deleted" ]; then
    echo "Failed to delete log stream $stream_name with response: $content"
    printf "Test delete_stream: failed\n"
    exit 1
  fi

  printf "Test delete_stream: successful\n"
  return 0
}

cleanup () {
  rm -rf "$input_file"
  return $?
}

printf "=======Starting smoke tests=======\n"
printf "** Randomised log stream name: %s\n" "$stream_name"
printf "** Total events being stored: %s\n" "$events"
create_stream
post_event_data
list_log_streams
get_streams_schema
query_log_stream
set_alert
get_alert
delete_stream
cleanup
