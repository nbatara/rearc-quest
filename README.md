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
