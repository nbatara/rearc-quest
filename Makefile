
.PHONY: build deploy clean test-ingest test-analytics test

build:
	@echo "Building lambda package..."
	docker run --rm --platform linux/amd64 \
	-v "$$PWD":/var/task \
	-w /var/task \
	--entrypoint /bin/bash \
	public.ecr.aws/lambda/python:3.11 \
	-c 'yum install -y zip gcc gcc-c++ make >/dev/null 2>&1 || true; \
	    rm -rf build lambda-package.zip; \
	    mkdir -p build; \
	    pip install --upgrade pip >/dev/null 2>&1; \
	    pip install -r requirements.txt -t build --only-binary=:all:; \
	    cp -R src build/; \
	    cd build; \
	    zip -r ../lambda-package.zip . >/dev/null'
	@echo "Build complete."

deploy: build
	@echo "Deploying lambda package..."
	aws s3 cp lambda-package.zip s3://rearc-quest-deploy-bucket/lambda-package.zip
	aws --no-cli-pager lambda update-function-code \
		--function-name rearc-quest-deploy-AnalyticsFunction-VtVDQFIzc51O \
		--s3-bucket rearc-quest-deploy-bucket \
		--s3-key lambda-package.zip \
		--region us-east-2
	aws --no-cli-pager lambda update-function-code \
		--function-name rearc-quest-deploy-IngestFunction-4faxGAROSrEF \
		--s3-bucket rearc-quest-deploy-bucket \
		--s3-key lambda-package.zip \
		--region us-east-2
	@echo "Deployment complete."

clean:
	@echo "Cleaning up build artifacts..."
	rm -rf build dist lambda-package.zip
	@echo "Cleanup complete."

package-ingest:
	@echo "Package ingest lambda (zip or image)"

package-analytics:
	@echo "Package analytics lambda with pandas layer or image"

analyze-local:
	@echo "Run analytics locally with python -m src.analytics"

# You'll need to update the function-name after you deploy your cloudformation stack.
test-ingest:
	@echo "Invoking ingest lambda..."
	aws --no-cli-pager lambda invoke \
	  --function-name rearc-quest-deploy-IngestFunction-4faxGAROSrEF \
	  --region "$$REGION" \
	  /dev/stdout

# You'll need to update the function-name after you deploy your cloudformation stack.
test-analytics:
	@echo "Invoking analytics lambda..."
	aws --no-cli-pager lambda invoke \
	  --function-name rearc-quest-deploy-AnalyticsFunction-VtVDQFIzc51O \
	  --region "$$REGION" \
	  /dev/stdout

test: test-ingest test-analytics
