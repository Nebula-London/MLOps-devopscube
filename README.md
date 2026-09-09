# MLOps Platform

Local MLOps environment using [Floci](https://github.com/floci) (AWS EKS emulator with LocalStack-compatible S3), Apache Airflow, PostgreSQL, and a **DVC dataset pipeline** running as Kubernetes pods.

The `dataset_pipeline` DAG is fully reproducible from a fresh clone:

1. `pull_data` — clones the Git repo (SSH key **or** HTTPS PAT), runs `dvc pull` from S3/LocalStack
2. `modify_data` — appends a new synthetic employee row to the CSV with pandas
3. `push_data` — runs `dvc add` + `dvc push` to S3/LocalStack, commits and pushes the new `.dvc` pointer to Git

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [DVC Dataset Pipeline](#dvc-dataset-pipeline)
  - [1. Start infrastructure](#1-start-infrastructure)
  - [2. Install Airflow](#2-install-airflow)
  - [3. Create pipeline prerequisites](#3-create-pipeline-prerequisites)
  - [4. Build & load the DVC worker image](#4-build--load-the-dvc-worker-image)
  - [5. Bootstrap the DVC remote](#5-bootstrap-the-dvc-remote)
  - [6. Run the DAG](#6-run-the-dag)
  - [Forking & configuration](#forking--configuration)
- [Deploy an ML Model](#deploy-an-ml-model)
- [Directory Structure](#directory-structure)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)
- [License](#license)

## Architecture

```
┌─────────────────────────────────────────────────────┐
│             Kubernetes (k3s inside Floci)            │
│  ┌─────────────────────────────────────────────┐    │
│  │ Apache Airflow 2.11                         │    │
│  │  Webserver │ Scheduler │ Worker │ Triggerer │    │
│  │  git-sync → this repo (dags/)               │    │
│  │  └── dataset_pipeline DAG                   │    │
│  │        pull_data → modify_data → push_data  │    │
│  │        (KubernetesPodOperator = DVC pods)   │    │
│  └─────────────────────────────────────────────┘    │
│         │                                   │       │
│         │ shared PVC (airflow-shared-pvc)   │       │
│         │ (5Gi local-path, across 3 tasks)  │       │
│         │                                   v       │
│  ┌──────────────┐                 ┌───────────────────┐
│  │ PostgreSQL   │                 │ S3 (LocalStack)   │
│  │ (StatefulSet)│                 │ bucket: ml-dvc-store│
│  └──────────────┘                 └───────────────────┘
└─────────────────────────────────────────────────────┘
```

Data flows through a shared PVC between the three pod tasks; DVC objects travel between the PVC and the S3 bucket (`ml-dvc-store`), and Git pointers live in this repository.

## Prerequisites

| Tool | Purpose |
|------|---------|
| Docker Desktop | Container runtime, hosts the Floci k3s cluster |
| kubectl | Kubernetes CLI |
| Helm | Install Airflow |
| AWS CLI v2 | Interact with the Floci EKS/S3 API |
| jq | JSON parsing in scripts |
| git | Required by the DVC pods |
| DVC CLI | Bootstrap the S3 remote from your host |
| SSH key (or GitHub PAT) | Git auth used by the pipeline pods |

## Quick Start

```bash
# 1. Start Floci emulator
cd infrastructure
docker compose up -d

# 2. Wait ~30s for Floci to initialize, then create the EKS cluster
./create-cluster.sh

# 3. Configure kubectl
source infrastructure/set-env.sh
export KUBECONFIG=/tmp/floci-eks-kubeconfig.yaml

# 4. Deploy PostgreSQL
kubectl apply -f k8s/postgresql-statefulset.yaml

# 5. Install Airflow (chart v1.22.x, Airflow 2.11.2)
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow \
  --version 1.22.0 \
  --namespace airflow --create-namespace \
  -f helm/airflow-values.yaml

# 6. Follow the DVC pipeline steps below
```

> `helm/airflow-values.yaml` enables `dags.gitSync` pointing at the repo's `dags/`
> subdirectory. When you run the DAG worker tasks directly, the repo is cloned
> into the pod over SSH/HTTPS, so the same repo must contain
> `phase-1-local-dev/datasets/employee_attrition.csv.dvc` and `.dvc/config`.

## DVC Dataset Pipeline

### 1. Start infrastructure

From the [Quick Start](#quick-start) above: Floci running, cluster created,
kubeconfig exported, PostgreSQL deployed, Airflow installed.

Expose the S3/API endpoint on the host and install the DVC CLI:

```bash
pip install "dvc[s3]"
```

### 2. Install Airflow

Already installed in the Quick Start. Verify:

```bash
kubectl get pods -n airflow | grep -E "webserver|scheduler|worker"
helm list -n airflow
```

Wait for git-sync to fetch this repo's `dags/` (usually <60s after install):

```bash
kubectl exec deployment/airflow-scheduler -n airflow -c scheduler -- \
  airflow dags list | grep dataset_pipeline
```

### 3. Create pipeline prerequisites

```bash
kubectl apply -f k8s/dvc/   # airflow-dvc-sa + airflow-shared-pvc
```

The DVC pods authenticate to GitHub one of two ways – create **one** of these
secrets:

**Option A – SSH deploy key (recommended for this repo):**

```bash
kubectl create secret generic airflow-git-ssh -n airflow \
  --from-file=gitSshKey=~/.ssh/id_ed25519_mlops
```

The key must belong to a GitHub account (or deploy key) with **read+write**
access to the pipeline repo.

**Option B – HTTPS fine-grained PAT:**

```bash
kubectl create secret generic git-credentials -n airflow \
  --from-literal=GIT_SYNC_USERNAME=<your-github-username> \
  --from-literal=GIT_SYNC_PASSWORD=<personal-access-token>
```

The PAT needs `Contents: Read and write` on the pipeline repo. Both secrets are
optional mounts (`optional: true`), the pod fails fast with a clear error if
neither exists.

### 4. Build & load the DVC worker image

The pipeline runs a slim Python image with `git`, `openssh-client`, `dvc[s3]`
and `pandas`. The default image name is `airflow-dvc-worker:1.0.0` and the DAG
uses `imagePullPolicy: IfNotPresent`.

**Option A – local k3s-in-Docker (no registry needed, matches this repo):**

```bash
docker build -t airflow-dvc-worker:1.0.0 docker/dvc-worker
docker save airflow-dvc-worker:1.0.0 | \
  docker exec -i $NODE_CONTAINER ctr -n k8s.io images import -
```

where `$NODE_CONTAINER` is the k3s container (e.g. `floci-eks-mlops-cluster`).

**Option B – published image (works anywhere):**

```bash
docker build -t <registry>/airflow-dvc-worker:1.0.0 docker/dvc-worker
docker push <registry>/airflow-dvc-worker:1.0.0
```

Then set `DVC_IMAGE` for the DAG (see [configuration](#forking--configuration))
and keep `imagePullPolicy` default/`Always`.

### 5. Bootstrap the DVC remote

The S3 bucket and the first version of the data must exist before the DAG can
`dvc pull`. This is a one-time, host-side step:

```bash
# Create the bucket
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url http://127.0.0.1:4566 s3 mb s3://ml-dvc-store

# Restore the dataset (it is gitignored as a DVC-tracked file)
#   either: dvc pull
#   or download the source CSV into phase-1-local-dev/datasets/employee_attrition.csv

# Track + push the first version
dvc add phase-1-local-dev/datasets/employee_attrition.csv
dvc push
```

`.dvc/config` already points `remote "storage"` at `s3://ml-dvc-store` with
`endpointurl http://127.0.0.1:4566` and localstack credentials (`test`/`test`).

Commit the resulting `.dvc` file and `.dvc/config` so the pod clone contains
them.

### 6. Run the DAG

```bash
kubectl exec deployment/airflow-scheduler -n airflow -c scheduler -- \
  airflow dags trigger dataset_pipeline
```

Expected result (check `kubectl exec -it airflow-dvc-pull-<id> -n airflow -- bash`
logs, or watch the run in the Airflow UI):

| Task | Result |
|------|--------|
| `pull_data` | clone + `dvc pull` → CSV present on shared PVC |
| `modify_data` | `Added employee ID <N>, total rows: 74499` |
| `push_data` | `1 file pushed`, `[main <sha>] Data version: ...` |

Then confirm two objects in S3:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url http://127.0.0.1:4566 s3 ls s3://ml-dvc-store --recursive
```

### Forking & configuration

Airflow env vars override DAG defaults (set them via
`airflow.config` in `helm/airflow-values.yaml` or the webserver UI → Admin →
Configuration):

| Env var | Default | Purpose |
|---------|---------|---------|
| `MLOPS_GIT_REPO` | `git@github.com:Nebula-London/MLOps-devopscube.git` | Repo cloned/pushed by DVC pods |
| `MLOPS_GIT_BRANCH` | `main` | Git branch used by the pipeline |
| `DVC_IMAGE` | `airflow-dvc-worker:1.0.0` | DVC worker container image |
| `DVC_REMOTE_ENDPOINT` | `http://172.17.0.1:4566` | S3 endpoint **reachable from cluster pods** |

If you fork this repo:

- update `helm/airflow-values.yaml` → `dags.gitSync.repo` to your fork and
  re-run `helm upgrade airflow apache-airflow/airflow ... -f helm/airflow-values.yaml`
- set `MLOPS_GIT_REPO` to your fork (HTTPS URL if using the PAT secret)
- create the git secret with credentials that own your fork
- if your cluster isn't k3s-in-Docker at the default `172.17.0.1` bridge
  gateway, set `DVC_REMOTE_ENDPOINT` to the address the pods use to reach S3
  (see [Troubleshooting](#troubleshooting)).

## Deploy an ML Model

```bash
cd scripts
./deploy-model.sh
```

To use your own model image, edit `k8s/ml-model.yaml` and replace `nginx:latest`.

### Access Airflow UI

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
# Open http://localhost:8080 -- login airflow / airflow
```

## Directory Structure

```
MLOps/
├── infrastructure/                 # Floci emulator & cluster management
│   ├── docker-compose.yml          # Floci services (eks, ec2, iam, s3)
│   ├── Dockerfile.postgresql       # Custom PostgreSQL image
│   ├── create-cluster.sh           # Create EKS cluster
│   ├── delete-cluster.sh           # Delete EKS cluster
│   └── set-env.sh                  # AWS env variables for Floci
├── k8s/
│   ├── dvc/
│   │   ├── serviceaccount.yaml     # airflow-dvc-sa (DVC pod identity)
│   │   └── pvc.yaml                # airflow-shared-pvc (5Gi shared volume)
│   ├── ml-model.yaml               # ML model deployment & service
│   └── postgresql-statefulset.yaml
├── helm/
│   ├── airflow-values.yaml         # Airflow Helm overrides + git-sync
│   ├── sa.yaml                     # airflow-runner SA (pod-launcher RBAC)
│   └── rolebinding.yaml            # airflow-admin cluster-admin binding
├── docker/
│   └── dvc-worker/Dockerfile       # DVC worker image (dvc[s3], pandas, git)
├── dags/
│   └── dataset-pipeline.py         # dataset_pipeline DAG (git-synced)
├── scripts/
│   └── deploy-model.sh             # Automated ML model deployment
├── .dvc/config                     # DVC remote → LocalStack S3
├── phase-1-local-dev/datasets/     # employee_attrition.csv (.dvc-tracked)
└── README.md                       # This file
```

## Configuration

### Airflow

- **Executor**: SequentialExecutor
- **Database**: External PostgreSQL (`airflow-postgresql:5432`)
- **Image**: `apache/airflow:2.11.2-python3.11`
- **User**: `airflow` / `airflow` (Admin)
- **DAGs**: git-synced from this repo's `dags/` (see `helm/airflow-values.yaml`)

### DVC / LocalStack

- `.dvc/config` → `remote "storage" = s3://ml-dvc-store`
- endpoint `http://127.0.0.1:4566` (host-side push), access key `test`/`test`
- the DAG overrides the endpoint to `http://172.17.0.1:4566` **inside pods**
  (the docker bridge gateway the k3s node uses to reach the host)

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Pod stuck `ImagePullBackOff` "no match for platform" | The worker image is amd64-only or missing. Build locally for arm64 and import with `ctr` (step 4) or publish a multi-arch image |
| `serviceaccount "airflow-dvc-sa" not found` | Run `kubectl apply -f k8s/dvc/` |
| `chmod: ... Read-only file system` | SSH keys from a Secret mount are read-only; the DAG copies the key to `/tmp` first – don't chmod the mounted file |
| `Connect timeout on endpoint URL ... 4566` from a pod | `DVC_REMOTE_ENDPOINT` must be the address the pod reaches for S3 (docker bridge gateway, e.g. `172.17.0.1`), not `127.0.0.1` |
| `NoSuchBucket: ml-dvc-store` | Run the `s3 mb` step from section 5 |
| Git clone inside pod: "Repository not found" | SSH key/PAT belongs to an account without access to `MLOPS_GIT_REPO` |
| DAG not showing in Airflow | git-sync needs <60s; verify `helm list -n airflow` and the `dags.gitSync.repo` value |
| `host.docker.internal` doesn't work | On macOS the container-to-host alias is unavailable; use the docker bridge IP instead |

## Cleanup

```bash
helm uninstall airflow -n airflow
kubectl delete -f k8s/postgresql-statefulset.yaml
kubectl delete -f k8s/ml-model.yaml
kubectl delete -f k8s/dvc/
cd infrastructure && ./delete-cluster.sh
docker compose down -v
```

## License

MIT