#!/bin/bash
# Source the environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/set-env.sh"

# Delete the EKS cluster
aws eks delete-cluster --name mlops-cluster

echo "EKS cluster deletion initiated. Check status with:"
echo "aws eks describe-cluster --name mlops-cluster --query 'cluster.status'"