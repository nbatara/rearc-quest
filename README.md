# Nicolas Batara — Rearc Quest Submission (2025-11-29)
This was a lot of fun! I took a look at who’s been in the commit history of this repo — y’all are awesome.

Thanks for considering me as a candidate for Rearc, and happy holidays!

Nick

## Published S3 Directory
You can view both the reports and data here:

s3://rearc-quest-data-nbatara-2025  
http://rearc-quest-data-nbatara-2025.s3-website.us-east-2.amazonaws.com

## .ipynb
See notebooks/analysis.ipynb

## Deployment & Testing
You can deploy the CloudFormation stack and test the Lambdas using the Makefile:

```bash
source ./.env
make init
make deploy
make test-ingest
make test-analytics
```

## How did I use AI for this quest?
I used AI (Gemini Code Assist VS Code Plugin and the OpenAI Codex Plugin) to create a project scaffold and help me debug issues I found. It was extremely helpful and saved me alot of time. Writing cloudformation yamls and makefiles aren't that much fun.

My prompts generally looked like:
- Why am I getting this error?
- Can you generate a makefile for me with the commands I put in the README.md file
- How do python dependencies work in the AWS lambda environment.
- Check for grammar errors in this markdown ^

## SETUP INSTRUCTIONS:
### Configure parameters.json and .env
The parameters.example.json and .env.example files are templates. Copy and remove the .example from these files and configure them!
### Configure AWS
Created IAM User
Created and attached policy AdministratorAccess to make things easy for this quest. We should not do this for a real client!




## Daily ingest
- **Lambda entrypoint:** `src/lambda_handlers/ingest_handler.py` kicks things off on a schedule.
- **BLS files:** `src/bls_sync.py` lists the key downloads (`pr.data.0.Current`, `pr.series`) and stages them.
- **Population data:** `src/datausa_fetch.py` pulls the Honolulu population API, saves the raw JSON, and writes a tidy table.
- **Storage helper:** Everything goes through `src/common/aws.py`, which ships a tiny in-memory S3 helper so you can run locally without AWS credentials. Swap in `boto3` when you deploy.

## Analytics when triggered
- **Lambda entrypoint:** `src/lambda_handlers/analytics_handler.py` calls `src/analytics.py` when a message arrives.
- **Queries (pandas):**
  - Mean and standard deviation for the 2013–2018 population.
  - Best year per BLS `series_id` based on summed quarterly values.

## AWS wiring
- `infrastructure/cloudformation/template.yaml` defines the S3 bucket, EventBridge rule for ingest, SQS queue for population updates, and the analytics Lambda that listens to the queue.
- `infrastructure/cloudformation/parameters.example.json` shows the few inputs you need (bucket name, prefixes, schedule, contact email).
