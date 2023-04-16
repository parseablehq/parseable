#!/bin/bash

helm package helm -d helm-releases/
helm package ../operator/helm/operator -d helm-releases/

helm repo index --merge index.yaml --url https://charts.parseable.io .
