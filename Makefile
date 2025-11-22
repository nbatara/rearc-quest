.PHONY: package-ingest package-analytics deploy analyze-local

package-ingest:
@echo "Package ingest lambda (zip or image)"

package-analytics:
@echo "Package analytics lambda with pandas/duckdb layer or image"

deploy:
@echo "Deploy CloudFormation stack using infrastructure/cloudformation/template.yaml"

analyze-local:
@echo "Run analytics locally with python -m src.analytics"
