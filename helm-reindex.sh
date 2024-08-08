#!/bin/bash

helm package helm -d helm-releases/

helm repo index --merge index.yaml --url https://charts.parseable.com .
