#!/bin/bash
# Set environment variables for AWS CLI to point to Floci emulator
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
echo "Environment variables set for Floci AWS emulator."