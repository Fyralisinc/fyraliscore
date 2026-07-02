# Fyralis Local BYOC Rehearsal Chart

This chart is the zero-spend rehearsal package for the Fyralis BYOC data plane.
It runs the core customer-cloud shape on a local Kubernetes cluster with bundled
Postgres/pgvector, Kafka, MinIO, Redis, gateway, migration/topic/bucket jobs,
and the default ingestion worker set.

It does not create AWS resources and it does not require source secret values.

## Runbook

Generate local BYOC artifacts:

```bash
fyralis byoc agent local-rehearsal \
  --region us-east-1 \
  --workdir .fyralis/local-rehearsal \
  --json
```

Build and load the local app image:

```bash
docker build -t fyralis/local:dev .
kind create cluster --name fyralis-byoc
kind load docker-image fyralis/local:dev --name fyralis-byoc
```

Render and install:

```bash
helm template fyralis ./deploy/helm/fyralis \
  --namespace fyralis-system \
  -f .fyralis/local-rehearsal/provider/aws-cloudformation/helm-values.json

helm upgrade --install fyralis ./deploy/helm/fyralis \
  --namespace fyralis-system \
  --create-namespace \
  -f .fyralis/local-rehearsal/provider/aws-cloudformation/helm-values.json
```

Check the local gateway:

```bash
kubectl -n fyralis-system rollout status deployment/fyralis-gateway
kubectl -n fyralis-system port-forward svc/fyralis-gateway 8000:8000
curl http://localhost:8000/healthz
```
