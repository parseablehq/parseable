#!/bin/bash

helm package helm/parseable -d helm-releases/
helm package ../collector/helm/collector -d helm-releases/

helm repo index --merge index.yaml --url https://charts.parseable.io .
