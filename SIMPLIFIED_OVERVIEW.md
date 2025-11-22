# How the pieces fit together (plain English)

This project intentionally keeps the code small and readable so you can walk an interviewer through the entire flow without jumping between tools.

## Daily ingest
1. `src/lambda_handlers/ingest_handler.py` is the scheduled Lambda entrypoint.
2. It calls:
   - `src/bls_sync.py` to list the important BLS files (`pr.data.0.Current` and `pr.series`) and record them in a simple in-memory S3 mock.
   - `src/datausa_fetch.py` to fetch the Honolulu population API response, store the raw JSON, and write a normalized table (CSV + Parquet) to the same mock S3 bucket.
3. All S3 interactions use `src/common/aws.py`, which ships a tiny `InMemoryS3Client` so you can run the flow locally and explain the steps without real AWS access. Swap it for boto3 when you are ready to deploy.

## Analytics on demand
1. `src/lambda_handlers/analytics_handler.py` triggers `src/analytics.py`.
2. `analytics.py` loads the BLS CSV and the population Parquet from the mock S3 bucket and runs three pandas-only queries:
   - `population_stats`: mean + standard deviation for 2013–2018 population.
   - `best_year_by_series`: which year each BLS `series_id` peaked.
   - `series_with_population`: join `PRS30006032` Q01 values with the population for that year.
3. Results are returned as DataFrames (or logged in Lambda) so you can show the outputs immediately.

## Infrastructure at a glance
- `infrastructure/cloudformation/template.yaml` wires a single S3 bucket, EventBridge schedule for ingest, an SQS queue for new population loads, and an analytics Lambda subscribed to that queue.
- `infrastructure/cloudformation/parameters.example.json` shows the handful of parameters needed (bucket name, prefixes, schedule, contact email).

Because everything runs against an in-memory S3 mock by default, you can open each module and follow the logic line by line without worrying about credentials. When you're ready for AWS, the same functions are structured to accept a real S3 client.
