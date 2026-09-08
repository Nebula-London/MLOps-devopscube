#!/bin/bash
# Source the environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/set-env.sh"

# Wait for the cluster to be active (we assume the cluster was created by create-cluster.sh)
echo "Waiting for cluster to be active..."
aws eks wait cluster-active --name mlops-cluster

# Get cluster details
CLUSTER_INFO=$(aws eks describe-cluster --name mlops-cluster --query "cluster" --output json)
if [ $? -ne 0 ]; then
    echo "Failed to describe cluster. Make sure the cluster was created."
    exit 1
fi

# Get the k3s kubeconfig from the Floci EKS container
# The container name follows the pattern: floci-eks-<cluster-name>
CONTAINER_NAME="floci-eks-mlops-cluster"
echo "Getting kubeconfig from Floci EKS container..."
KUBECONFIG_YAML=$(docker exec "$CONTAINER_NAME" cat /etc/rancher/k3s/k3s.yaml)

# Replace the server URL from internal (127.0.0.1:6443) to external (localhost:6501)
KUBECONFIG_YAML=$(echo "$KUBECONFIG_YAML" | sed 's|server: https://127.0.0.1:6443|server: https://localhost:6501|')

# Save to a temporary file
KUBECONFIG_FILE="/tmp/floci-eks-kubeconfig.yaml"
echo "$KUBECONFIG_YAML" > "$KUBECONFIG_FILE"

echo "Kubeconfig saved to $KUBECONFIG_FILE"

mkdir -p ~/.kube
cp "$KUBECONFIG_FILE" ~/.kube/config
echo "Kubectl configured - you can now run kubectl commands directly"

# Apply the Kubernetes manifest using the Floci EKS kubeconfig
echo "Deploying ML model..."
KUBECONFIG="$KUBECONFIG_FILE" kubectl apply -f "$SCRIPT_DIR/k8s/ml-model.yaml"

echo "Deployment completed. Check status with:"
echo "KUBECONFIG=$KUBECONFIG_FILE kubectl get deployments"
echo "KUBECONFIG=$KUBECONFIG_FILE kubectl get services"
echo "KUBECONFIG=$KUBECONFIG_FILE kubectl get pods"
