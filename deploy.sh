

#!/bin/bash

# Define variables
PROJECT_ID="valor-sales"
REGION="northamerica-northeast2 "
JOB_NAME="stlth-customer-tag-sync"

SERVICE_ACCOUNT="valor-scheduler-invoker@valor-sales.iam.gserviceaccount.com"

echo "Deploying Cloud Run Job: ${JOB_NAME}..."

gcloud run jobs deploy ${JOB_NAME} \
  --source . \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --service-account ${SERVICE_ACCOUNT} \
  --set-env-vars GOOGLE_CLOUD_PROJECT=${PROJECT_ID} \
  --set-secrets SHOPIFY_STORE_PASSWORD=stlth_shopify_cred:latest \
  --max-retries 1 \
  --task-timeout 10m

echo "Deployment complete!"