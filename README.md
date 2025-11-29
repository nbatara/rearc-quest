# How this project works (no fluff)

This repo is a small tour of the Rearc Data Quest. The code stays minimal so you can point to a file, describe what it does, and move on.

## Daily ingest at a glance
- **Lambda entrypoint:** `src/lambda_handlers/ingest_handler.py` kicks things off on a schedule.
- **BLS files:** `src/bls_sync.py` lists the key downloads (`pr.data.0.Current`, `pr.series`) and stages them.
- **Population data:** `src/datausa_fetch.py` pulls the Honolulu population API, saves the raw JSON, and writes a tidy table.
- **Storage helper:** Everything goes through `src/common/aws.py`, which ships a tiny in-memory S3 helper so you can run locally without AWS credentials. Swap in `boto3` when you deploy.

## Analytics when triggered
- **Lambda entrypoint:** `src/lambda_handlers/analytics_handler.py` calls `src/analytics.py` when a message arrives.
- **Queries (pandas):**
  - Mean and standard deviation for the 2013–2018 population.
  - Best year per BLS `series_id` based on summed quarterly values.
  - `PRS30006032` Q01 values paired with the matching population year.
- Results are DataFrames you can print, log, or store back to S3.

## AWS wiring
- `infrastructure/cloudformation/template.yaml` defines the S3 bucket, EventBridge rule for ingest, SQS queue for population updates, and the analytics Lambda that listens to the queue.
- `infrastructure/cloudformation/parameters.example.json` shows the few inputs you need (bucket name, prefixes, schedule, contact email).

## Running locally
- Use the in-memory S3 client to trace each step without cloud access.
- When you are ready for AWS, replace the client with `boto3` and deploy with the CloudFormation template.

## Helper scripts
- `scripts/parameters_to_overrides.py infrastructure/cloudformation/parameters.json` prints a `Key=Value` string you can drop into `aws cloudformation deploy --parameter-overrides`.

## Command Log
### Configure parameters.json and .env

### Configure AWS
Created IAM User
Created and attached policy AdministratorAccess to make things easy for this quest. We should probably not do this for a real client.
### Make your S3 buckets!
aws s3 mb s3://$DATA_BUCKET --region us-east-2
aws s3 mb s3://$DEPLOYMENT_BUCKET --region us-east-2

### Build the libraries in the requirements file using docker lambda image and zip for s3 upload
docker run --rm --platform linux/amd64 \
  -v "$PWD":/var/task \
  -w /var/task \
  --entrypoint /bin/bash \
  public.ecr.aws/lambda/python:3.11 \
  -c "
    yum install -y zip gcc gcc-c++ make >/dev/null 2>&1 || true
    rm -rf build lambda-package.zip
    mkdir -p build
    pip install --upgrade pip >/dev/null 2>&1
    pip install -r requirements.txt -t build --only-binary=:all:
    cp -R src build/
    # kill anything that makes numpy think it's in a source tree at the root
    find build -maxdepth 1 -type f \( -name 'setup.py' -o -name 'pyproject.toml' -o -name 'PKG-INFO' \) -delete
    cd build
    zip -r ../lambda-package.zip . >/dev/null
  "

aws s3 cp lambda-package.zip s3://$DEPLOYMENT_BUCKET/lambda-package.zip --region "$REGION"
### Deploy Stack
PARAM_OVERRIDES=$(
  jq -r '.Parameters
         | to_entries
         | map("\(.key)=\(.value|@sh)")
         | join(" ")' infrastructure/cloudformation/parameters.json
)

eval aws cloudformation deploy \
  --region "$REGION" \
  --stack-name "$STACK_NAME" \
  --template-file infrastructure/cloudformation/template.yaml \
  --parameter-overrides $PARAM_OVERRIDES \
  --capabilities CAPABILITY_NAMED_IAM

### If you need to update your Lambda functions (have to find and update the function-names because they are unique for your deploy):
aws lambda update-function-code \
--function-name rearc-quest-deploy-AnalyticsFunction-VtVDQFIzc51O \
--s3-bucket rearc-quest-deploy-bucket \
--s3-key lambda-package.zip \
--region us-east-2;
aws lambda update-function-code \
--function-name rearc-quest-deploy-IngestFunction-4faxGAROSrEF \
--s3-bucket rearc-quest-deploy-bucket \
--s3-key lambda-package.zip \
--region us-east-2;

### If You have issues you can investigate with

aws cloudformation describe-stack-events \
--region us-east-2 \
--stack-name rearc-quest-deploy

aws cloudformation list-change-sets \
--region "$REGION" \
--stack-name "$STACK_NAME"

aws cloudformation describe-change-set \
--region us-east-2 \
--stack-name $STACK_NAME \
--change-set-name [change set name from output of above command here]

aws cloudformation delete-stack \
--stack-name $STACK_NAME \
--region "$REGION"

### Test Functions
aws --no-cli-pager lambda invoke \
  --function-name rearc-quest-deploy-IngestFunction-4faxGAROSrEF \
  --region "$REGION" \
  /dev/stdout