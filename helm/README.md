# Parseable Helm Chart

## Install (standalone, local-store)

```sh
kubectl create secret generic parseable-env-secret \
  --from-literal=addr=0.0.0.0:8000 \
  --from-literal=username=admin \
  --from-literal=password=admin \
  --from-literal=staging.dir=/parseable/staging \
  --from-literal=fs.dir=/parseable/data

helm install parseable ./ -n parseable --create-namespace
```

## Distributed on a cloud (querier + ingestors)

Create the secret with the keys for your cloud (see the matching values file), then:

```sh
# AWS (S3)
helm install parseable ./ -n parseable --create-namespace -f values-aws.yaml

# GCP (GCS)
helm install parseable ./ -n parseable --create-namespace -f values-gcp.yaml

# Azure (Blob)
helm install parseable ./ -n parseable --create-namespace -f values-azure.yaml
```

## Upgrade

```sh
helm upgrade parseable ./ -n parseable -f values.yaml
```

## Uninstall

```sh
helm uninstall parseable -n parseable
```
