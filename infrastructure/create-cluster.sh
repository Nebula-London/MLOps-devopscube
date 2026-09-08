#!/bin/bash
# Source the environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/set-env.sh"

# Create an EKS cluster
# Note: Floci may not support all parameters, so we keep it simple
aws eks create-cluster \
    --name mlops-cluster \
    --role-arn arn:aws:iam::000000000000:role/floci \
    --resources-vpc-config subnetIds=subnet-12345,subnet-67890 \
    --region us-east-1

echo "EKS cluster creation initiated. Check status with:"
echo "aws eks describe-cluster --name mlops-cluster"