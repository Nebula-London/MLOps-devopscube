# MLOps Platform

Local MLOps environment using [Floci](https://github.com/floci) (AWS EKS emulator), Apache Airflow, and PostgreSQL.

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Directory Structure](#directory-structure)
- [Components](#components)
  - [Infrastructure](#infrastructure)
  - [Kubernetes Manifests](#kubernetes-manifests)
  - [Helm Charts](#helm-charts)
  - [Scripts](#scripts)
- [Configuration](#configuration)
  - [Airflow](#airflow)
  - [PostgreSQL](#postgresql)
- [Usage](#usage)
  - [Deploy an ML Model](#deploy-an-ml-model)
  - [Access Airflow UI](#access-airflow-ui)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)
- [License](#license)

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Kubernetes (k3s)                   │
│  ┌───────────┐  ┌───────────┐  ┌─────────────────┐  │
│  │ Airflow   │  │ PostgreSQL│  │  ML Model       │  │
│  │ Webserver │  │ (Stateful │  │  (Deployment)   │  │
│  │ Scheduler │  │    Set)   │  │                 │  │
│  │ Worker    │  │           │  │                 │  │
│  └─────┬─────┘  └─────┬─────┘  └────────┬────────┘  │
│        │              │                  │           │
│        └──────────────┼──────────────────┘           │
│                       │                              │
└───────────────────────┼──────────────────────────────┘
                        │
         ┌──────────────┴──────────────┐
         │    Floci EKS Emulator       │
         │    (localhost:4566)          │
         │    + ECR Registry           │
         └─────────────────────────────┘
```

## Prerequisites

| Tool | Purpose | Install |
|------|---------|---------|
| Docker Desktop | Container runtime | [docker.com](https://docs.docker.com/get-docker/) |
| kubectl | Kubernetes CLI | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/) |
| Helm | Package manager for K8s | [helm.sh](https://helm.sh/docs/intro/install/) |
| AWS CLI v2 | Interact with Floci EKS API | [aws.amazon.com](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) |
| jq | JSON parsing in scripts | `brew install jq` |

## Quick Start

```bash
# 1. Start Floci emulator
cd infrastructure
docker compose up -d

# 2. Wait ~30s for Floci to initialize, then create the EKS cluster
./create-cluster.sh

# 3. Configure kubectl
export KUBECONFIG=/tmp/floci-eks-kubeconfig.yaml

# 4. Deploy PostgreSQL
kubectl apply -f k8s/postgresql-statefulset.yaml

# 5. Install Airflow
helm install airflow apache-airflow/airflow \
  --version 1.22.0 \
  --namespace airflow \
  -f helm/airflow-values.yaml

# 6. Deploy ML model
kubectl apply -f k8s/ml-model.yaml
```

## Directory Structure

```
MLOps/
├── infrastructure/            # Floci emulator & cluster management
│   ├── docker-compose.yml     # Floci services definition
│   ├── Dockerfile.postgresql  # Custom PostgreSQL image
│   ├── create-cluster.sh      # Create EKS cluster
│   ├── delete-cluster.sh      # Delete EKS cluster
│   └── set-env.sh             # AWS env variables for Floci
├── k8s/                       # Kubernetes manifests
│   ├── ml-model.yaml          # ML model deployment & service
│   └── postgresql-statefulset.yaml
├── helm/                      # Helm chart values
│   └── airflow-values.yaml    # Airflow Helm overrides
├── scripts/                   # Utility scripts
│   └── deploy-model.sh        # Automated ML model deployment
├── docs/                      # Documentation
│   └── README-OLD.md          # Previous README
└── README.md                  # This file
```

## Components

### Infrastructure

The Floci emulator provides a local AWS EKS environment without real AWS resources.

| Service | Port | Description |
|---------|------|-------------|
| `floci` | 4566 | AWS EKS API emulator (k3s-based) |
| `floci-ecr` | — | Local container registry mimicking AWS ECR |

**Environment variables** (`infrastructure/set-env.sh`):

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

### Kubernetes Manifests

| Manifest | Resource | Namespace |
|----------|----------|-----------|
| `k8s/postgresql-statefulset.yaml` | PostgreSQL 16 StatefulSet (10Gi PVC) | `airflow` |
| `k8s/ml-model.yaml` | ML model Deployment + LoadBalancer Service | `default` |

### Helm Charts

| Values File | Chart | Version |
|-------------|-------|---------|
| `helm/airflow-values.yaml` | `apache-airflow/airflow` | 1.22.0 |

### Scripts

| Script | Description |
|--------|-------------|
| `scripts/deploy-model.sh` | Wait for cluster, configure kubectl, deploy ML model |

## Configuration

### Airflow

Configured via `helm/airflow-values.yaml`:

- **Executor**: SequentialExecutor
- **Database**: External PostgreSQL (`airflow-postgresql:5432`)
- **Image**: `apache/airflow:2.11.2-python3.11`
- **Default user**: `airflow` / `airflow` (Admin role)

Key settings:
- Built-in PostgreSQL disabled — uses manually deployed StatefulSet
- Redis enabled for Celery executor (if switched later)
- StatsD enabled for metrics

### PostgreSQL

- **Image**: `postgres:16-alpine`
- **Auth**: Trust (no password required)
- **Storage**: 10Gi PersistentVolume (`local-path` StorageClass)
- **Port**: 5432
- **Init container**: Fixes volume permissions before startup

## Usage

### Deploy an ML Model

```bash
# Using the helper script (handles kubeconfig setup)
cd scripts
./deploy-model.sh

# Or manually
kubectl apply -f k8s/ml-model.yaml
```

To use your own model image, edit `k8s/ml-model.yaml` and replace `nginx:latest` with your container image.

### Access Airflow UI

```bash
# Port-forward to local machine
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow

# Open http://localhost:8080
# Login: airflow / airflow
```

## Troubleshooting

### Floci not responding

```bash
# Check Floci health
curl -s http://localhost:4566/_localstack/health | jq .services.eks

# Restart if needed
cd infrastructure && docker compose restart
```

### Cluster creation fails

```bash
# Verify environment variables
source infrastructure/set-env.sh
aws eks list-clusters

# Check Floci logs
docker compose -f infrastructure/docker-compose.yml logs floci
```

### kubectl connection refused

```bash
# Re-extract kubeconfig from Floci container
docker exec floci-eks-mlops-cluster cat /etc/rancher/k3s/k3s.yaml \
  | sed 's|server: https://127.0.0.1:6443|server: https://localhost:6501|' \
  > ~/.kube/config
```

### PostgreSQL pods in CrashLoopBackOff

```bash
# Check logs
kubectl logs airflow-postgresql-0 -n airflow

# Common fix: re-apply the StatefulSet
kubectl delete -f k8s/postgresql-statefulset.yaml
kubectl apply -f k8s/postgresql-statefulset.yaml
```

## Cleanup

```bash
# Remove Airflow
helm uninstall airflow -n airflow

# Remove PostgreSQL
kubectl delete -f k8s/postgresql-statefulset.yaml

# Remove ML model
kubectl delete -f k8s/ml-model.yaml

# Delete EKS cluster
cd infrastructure && ./delete-cluster.sh

# Stop Floci
docker compose down -v
```

## License

MIT
