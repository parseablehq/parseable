#!/bin/sh

echo Creating Stream: $2

#Create stream
CREATESTREAM=$(curl --location --request PUT $1/api/v1/logstream/$2 \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==')
echo $CREATESTREAM

sleep 1

#Generate 50 events using flog and store it in input.json file
echo Creating input.json file with 50 events
./flog -f json -n 50 -t log -o input.json

#Preparing newinput.json to be used as JSON request for POST:
echo Creating newinput.json file to be used as raw data for POST
sed '1s/^/[/;$!s/$/,/;$s/$/]/' input.json > newinput.json


sleep 2

echo Posting 50 events into $2
#Put 50 events in stream using input.json file
PUTSTREAM=$(curl --location --request POST $1/api/v1/logstream/$2 \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==' \
--header 'Content-Type: application/json' \
-d "@newinput.json"
)
echo $PUTSTREAM

sleep 1

echo Getting the list of streams
#List Streams
LISTSTREAMS=$(curl --location --request GET $1/api/v1/logstream \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==')
echo $LISTSTREAMS

sleep 1

echo Querying the stream: $2
#Query stream
QUERYSTREAM=$(curl --location --request GET $1/api/v1/query \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==' \
--header 'Content-Type: application/json' \
--data-raw '{
"query": "select count(*) from '$2'"
}')
echo Number of events stored in $2: $QUERYSTREAM
if [[ $QUERYSTREAM == 50 ]]
then
  echo "Validation successful. Count of events returned from query is same as the ones posted."
else
  echo "Validation failed. Count of events returned from query does not match with the ones posted."
fi
sleep 3

#Delete the input.json file
echo deleting the input.json and newinput.json files
rm -rf input.json newinput.json