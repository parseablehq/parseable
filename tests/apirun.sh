#!/bin/sh

echo CREATESTREAM
echo PUT API = $1/api/v1/logstream/$2
#Create stream
CREATESTREAM=$(curl --location --request PUT $1/api/v1/logstream/$2 \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==')
echo $CREATESTREAM

sleep 1

echo PUTSTREAM
#Put 1 event in stream
PUTSTREAM=$(curl --location --request POST $1/api/v1/logstream/$2 \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==' \
--header 'Content-Type: application/json' \
-d "@eventpost.json"
)
echo $PUTSTREAM

sleep 1

echo LISTSTREAMS
#List Streams
LISTSTREAMS=$(curl --location --request GET $1/api/v1/stream \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==')
echo $LISTSTREAMS

sleep 1

echo QUERYSTREAM
#Query stream
QUERYSTREAM=$(curl --location --request GET $1/api/v1/query \
--header 'Authorization: Basic YmQ5MTA1MDEtYzg4NC00MDcyLWI5YjctZjk5ZjQ5NGVkZmU0OjRhODZmNWQyLTM1ZDgtNDMwZi1hODdmLTMwOTU5ZDA1YTk5Mg==' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "select * from '$2'"
}')
echo $QUERYSTREAM
