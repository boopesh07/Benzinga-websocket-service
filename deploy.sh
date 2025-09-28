#!/usr/bin/env bash
set -euo pipefail

echo "Deploy script started."
set -a
. ./.env.prod
set +a

export AWS_DEFAULT_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}

if [[ -z "${ACCOUNT_ID:-}" ]]; then
  if ! ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null); then
    echo "ERROR: ACCOUNT_ID not set and could not be derived via AWS STS." >&2
    exit 1
  fi
fi

sed -e "s|<ACCOUNT_ID>|$ACCOUNT_ID|g" \
    -e "s|<REGION>|$AWS_DEFAULT_REGION|g" \
    -e "s|<S3_BUCKET>|${S3_BUCKET:-}|g" \
    -e "s|<S3_PREFIX>|${S3_PREFIX:-benzinga}|g" \
    -e "s|<BENZINGA_API_KEY>|$BENZINGA_API_KEY|g" \
    -e "s|<LOG_FORMAT>|${LOG_FORMAT:-json}|g" \
    -e "s|<LOG_LEVEL>|${LOG_LEVEL:-INFO}|g" \
    infra/ecs-task-def.json > /tmp/ecs-task-def.rendered.json

aws ecs register-task-definition --cli-input-json file:///tmp/ecs-task-def.rendered.json | cat

export CLUSTER=${CLUSTER:-etl-pipeline-cluster}
export SUBNET_ID=${SUBNET_ID:?SUBNET_ID env required}
export SECURITY_GROUP_ID=${SECURITY_GROUP_ID:?SECURITY_GROUP_ID env required}

TASK_ARN=$(aws ecs run-task \
  --cluster "$CLUSTER" \
  --launch-type FARGATE \
  --task-definition "bz-ws-sink" \
  --platform-version LATEST \
  --network-configuration "awsvpcConfiguration={subnets=[\"$SUBNET_ID\"],securityGroups=[\"$SECURITY_GROUP_ID\"],assignPublicIp=\"ENABLED\"}" \
  --query 'tasks[0].taskArn' --output text)
echo "Started task: $TASK_ARN"
echo "Deploy complete."


