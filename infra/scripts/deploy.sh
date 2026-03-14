#!/usr/bin/env bash
set -euo pipefail

ENV="${1:?Usage: deploy.sh <dev|prod> [--skip-build]}"
SKIP_BUILD="${2:-}"

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
    echo "Error: Environment must be 'dev' or 'prod'"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO="ecommerce-pipeline"
ECR_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"
IMAGE_TAG="$(git rev-parse --short HEAD)"

if [[ "$ENV" == "prod" ]]; then
    TAG="stable-${IMAGE_TAG}"
else
    TAG="dev-${IMAGE_TAG}"
fi

echo "═══════════════════════════════════════════"
echo "  Deploying to: ${ENV}"
echo "  Image tag:    ${TAG}"
echo "  ECR URL:      ${ECR_URL}"
echo "═══════════════════════════════════════════"

if [[ "$SKIP_BUILD" != "--skip-build" ]]; then
    echo ""
    echo "→ Logging into ECR..."
    aws ecr get-login-password --region "$AWS_REGION" | \
        docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

    echo "→ Building image..."
    docker build -t "${ECR_URL}:${TAG}" .

    echo "→ Pushing image..."
    docker push "${ECR_URL}:${TAG}"

    ALIAS_TAG=$( [[ "$ENV" == "prod" ]] && echo "stable" || echo "latest" )
    docker tag "${ECR_URL}:${TAG}" "${ECR_URL}:${ALIAS_TAG}"
    docker push "${ECR_URL}:${ALIAS_TAG}"
fi

FAMILY="${ECR_REPO}-${ENV}"

echo ""
echo "→ Updating ECS task definition: ${FAMILY}"

TASK_DEF=$(aws ecs describe-task-definition \
    --task-definition "$FAMILY" \
    --query 'taskDefinition' \
    --output json)

NEW_TASK_DEF=$(echo "$TASK_DEF" | \
    jq --arg IMAGE "${ECR_URL}:${TAG}" \
    '.containerDefinitions[0].image = $IMAGE |
     del(.taskDefinitionArn, .revision, .status, .requiresAttributes, .compatibilities, .registeredAt, .registeredBy)')

aws ecs register-task-definition \
    --cli-input-json "$NEW_TASK_DEF" > /dev/null

echo ""
echo "✓ Deployed ${ECR_URL}:${TAG} to ${FAMILY}"
