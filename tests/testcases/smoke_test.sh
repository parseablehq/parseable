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
curl_std_opts=( -sS --header 'Content-Type: application/json' -w '\n\n%{http_code}' )

# Create stream
create_stream () {
  echo Creating Stream: "$stream_name"
  response=$(curl "${curl_std_opts[@]}" --request PUT "$parseable_url"/api/v1/logstream/"$stream_name")
  
  if [ $? -ne 0 ]; then
    echo "Failed to create log stream $stream_name with exit code: $?"
    exit 1
  fi
  
  http_code=$(tail -n1 <<< "$response")
  content=$(sed '$ d' <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to create log stream $stream_name with http code: $http_code and response: $content"
    exit 1
  fi
  
  if [ "$content" != "Created LogStream $stream_name" ]; then
    echo "Failed to create log stream $stream_name with response: $content"
    exit 1
  fi
  return 0
}

# Generate events using flog (https://github.com/mingrammer/flog) and store it in input.json file
create_input_file () {
  printf "Creating input.json file with %s events\n" $events 
  flog -f json -n "$events" -t log -o "$input_file"
  sleep 2
  printf "Preparing input.json file to be used as raw data for POST\n"
  sed -i '1s/^/[/;$!s/$/,/;$s/$/]/' "$input_file"
  return $?
}

# Post log data to the stream
post_event_data () {
  content=$(cat "$input_file")
  printf "Posting $events events into %s\n" "$stream_name"
  response=$(curl "${curl_std_opts[@]}" --request POST "$parseable_url"/api/v1/logstream/"$stream_name" --data-raw "$content")
  if [ $? -ne 0 ]; then
    echo "Failed to post log data to $stream_name with exit code: $?"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  content=$(sed '$ d' <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to create log stream $stream_name with http code: $http_code and response: $content"
    exit 1
  fi

  if [ "$content" != "Successfully posted $events events" ]; then
    echo "Failed to post log data to $stream_name with response: $content"
    exit 1
  fi
  return 0
}

# List all log stream and [TODO] verify if the stream is created
list_log_streams () {
  echo "Getting the list of streams"
  #List Streams
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream)
  if [ $? -ne 0 ]; then
    echo "Failed to list log streams with exit code: $?"
    exit 1
  fi
  return 0
}

# Get Stream's schema and [TODO] validate its schema
get_streams_schema () {
  echo "Getting stream's schema"
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/logstream/"$stream_name"/schema)
  if [ $? -ne 0 ]; then
    echo "Failed to fetch stream's schema with exit code: $?"
    exit 1
  fi
  return 0
}

# Query the log stream and verify if count of events is equal to the number of events posted
query_log_stream() {
  echo "Querying the stream: $stream_name"
  response=$(curl "${curl_std_opts[@]}" --request GET "$parseable_url"/api/v1/query --data-raw '{
    "query": "select count(*) from '$stream_name'"
  }')
  if [ $? -ne 0 ]; then
    echo "Failed to query log data from $stream_name with exit code: $?"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  content=$(sed '$ d' <<< "$response")
  queryResult=$(echo "$content" | cut -d ':' -f2 | cut -d '}' -f1)
  echo Number of events stored in "$stream_name": "$queryResult"
  if [ "$queryResult" = $events ]; then
    echo "Validation successful. Count of events returned from query is same as the ones posted."
  else
    echo "Validation failed. Count of events returned from query does not match with the ones posted."
    exit 1
  fi
  return 0
}

# Delete stream
delete_stream () {
  echo Deleting Stream: "$stream_name"
  response=$(curl "${curl_std_opts[@]}" --request DELETE "$parseable_url"/api/v1/logstream/"$stream_name")

  if [ $? -ne 0 ]; then
    echo "Failed to delete log stream $stream_name with exit code: $?"
    exit 1
  fi

  http_code=$(tail -n1 <<< "$response")
  content=$(sed '$ d' <<< "$response")
  if [ "$http_code" -ne 200 ]; then
    echo "Failed to delete log stream $stream_name with http code: $http_code and response: $content"
    exit 1
  fi
  
  if [ "$content" != "LogStream $stream_name deleted" ]; then
    echo "Failed to delete log stream $stream_name with response: $content"
    exit 1
  fi

  if [ "$content" = "LogStream $stream_name deleted" ]; then
    echo "$stream_name successfully deleted"
  fi
  return 0
}

cleanup () {
  echo "Deleting the $input_file file"
  rm -rf "$input_file"
  return $?
}

create_stream
create_input_file
post_event_data
list_log_streams
get_streams_schema
query_log_stream
delete_stream
cleanup
