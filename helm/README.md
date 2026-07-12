# Parseable Helm Chart

`values.yaml` holds the defaults. Layer an overlay from `overlays/` with `-f` to
pick a topology / store, and stack `overlays/enterprise.yaml` for Enterprise.

```
overlays/
  standalone.yaml   single all-in-one pod (local-store)
  aws.yaml          distributed on S3
  gcp.yaml          distributed on GCS
  azure.yaml        distributed on Azure Blob
  enterprise.yaml   adds prism; layer on top of a store overlay
```

## Standalone (single pod, local-store)

```sh
kubectl create namespace parseable

kubectl -n parseable create secret generic parseable-env-secret \
  --from-literal=addr=0.0.0.0:8000 \
  --from-literal=username=admin \
  --from-literal=password=admin

helm install parseable ./ -n parseable -f overlays/standalone.yaml
```

## Distributed (querier + ingestors)

Create the namespace, then the object-store secret for your cloud (below).

```sh
kubectl create namespace parseable
```

### AWS (S3)

```sh
kubectl -n parseable create secret generic parseable-env-secret \
  --from-literal=addr=0.0.0.0:8000 \
  --from-literal=username=admin --from-literal=password=admin \
  --from-literal=s3.url=<endpoint> \
  --from-literal=s3.access.key=<key> --from-literal=s3.secret.key=<secret> \
  --from-literal=s3.bucket=<bucket> --from-literal=s3.region=<region>

helm install parseable ./ -n parseable -f overlays/aws.yaml
```

### GCP (GCS)

```sh
kubectl -n parseable create secret generic parseable-env-secret \
  --from-literal=addr=0.0.0.0:8000 \
  --from-literal=username=admin --from-literal=password=admin \
  --from-literal=gcs.url=<url> --from-literal=gcs.bucket=<bucket>

kubectl -n parseable create secret generic parseable-gcs-key \
  --from-file=key.json=<path-to-service-account-key.json>

helm install parseable ./ -n parseable -f overlays/gcp.yaml
```

### Azure (Blob)

```sh
kubectl -n parseable create secret generic parseable-env-secret \
  --from-literal=addr=0.0.0.0:8000 \
  --from-literal=username=admin --from-literal=password=admin \
  --from-literal=azr.access_key=<key> --from-literal=azr.account=<account> \
  --from-literal=azr.container=<container> \
  --from-literal=azr.url=https://<account>.blob.core.windows.net

helm install parseable ./ -n parseable -f overlays/azure.yaml
```

## Enterprise (adds prism)

Stack `overlays/enterprise.yaml` on top of a store overlay, and create the license secret.

```sh
kubectl -n parseable create secret generic parseable-license \
  --from-file=parseable_license.json=<path>/parseable_license.json \
  --from-file=parseable_license.sig=<path>/parseable_license.sig

helm install parseable ./ -n parseable \
  -f overlays/aws.yaml -f overlays/enterprise.yaml
```

## Upgrade / Uninstall

```sh
helm upgrade parseable ./ -n parseable -f overlays/aws.yaml
helm uninstall parseable -n parseable
```
