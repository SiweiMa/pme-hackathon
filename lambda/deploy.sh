#!/usr/bin/env bash
# deploy.sh — Build, push, and update the PME Lambda container image.
#
# Usage:
#   ./lambda/deploy.sh          (run from repo root)
#
# Prerequisites:
#   - Docker running
#   - AWS CLI configured with credentials
#   - ECR repository created via Terraform (infra/lambda.tf)
#   - Lambda function created via Terraform (infra/lambda.tf)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION="${AWS_REGION:-us-east-2}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-651767347247}"
ECR_REPO="pwe-hackathon-pme-lambda"
LAMBDA_FUNCTION="pwe-hackathon-pme-encrypt"
IMAGE_NAME="${ECR_REPO}"
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"

# Tag with git short SHA for traceability
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "=== PME Lambda Deploy ==="
echo "Region:   ${AWS_REGION}"
echo "Account:  ${AWS_ACCOUNT_ID}"
echo "ECR:      ${ECR_URI}"
echo "Function: ${LAMBDA_FUNCTION}"
echo "Git SHA:  ${GIT_SHA}"
echo ""

# ---------------------------------------------------------------------------
# 1. ECR login
# ---------------------------------------------------------------------------

echo ">>> Logging in to ECR..."
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin \
    "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# ---------------------------------------------------------------------------
# 2. Build container image
# ---------------------------------------------------------------------------

echo ">>> Building container image..."
docker build \
  --platform linux/amd64 \
  -f lambda/Dockerfile \
  -t "${IMAGE_NAME}:${GIT_SHA}" \
  -t "${IMAGE_NAME}:latest" \
  .

# ---------------------------------------------------------------------------
# 3. Tag for ECR
# ---------------------------------------------------------------------------

echo ">>> Tagging for ECR..."
docker tag "${IMAGE_NAME}:${GIT_SHA}" "${ECR_URI}:${GIT_SHA}"
docker tag "${IMAGE_NAME}:latest" "${ECR_URI}:latest"

# ---------------------------------------------------------------------------
# 4. Push to ECR
# ---------------------------------------------------------------------------

echo ">>> Pushing to ECR..."
docker push "${ECR_URI}:${GIT_SHA}"
docker push "${ECR_URI}:latest"

# ---------------------------------------------------------------------------
# 5. Update Lambda function code
# ---------------------------------------------------------------------------

echo ">>> Updating Lambda function code..."
aws lambda update-function-code \
  --function-name "${LAMBDA_FUNCTION}" \
  --image-uri "${ECR_URI}:${GIT_SHA}" \
  --region "${AWS_REGION}" \
  --no-cli-pager

# ---------------------------------------------------------------------------
# 6. Wait for update to complete
# ---------------------------------------------------------------------------

echo ">>> Waiting for Lambda function update..."
aws lambda wait function-updated \
  --function-name "${LAMBDA_FUNCTION}" \
  --region "${AWS_REGION}"

echo ""
echo "=== Deploy complete ==="
echo "Image:    ${ECR_URI}:${GIT_SHA}"
echo "Function: ${LAMBDA_FUNCTION}"
