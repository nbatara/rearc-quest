# Packaging

This directory can hold Lambda build artifacts or layer definitions.

Suggested targets:
- `package-ingest`: bundle `src/` dependencies for the ingest Lambda.
- `package-analytics`: bundle pandas via a Lambda layer or container image.

Populate this folder with build scripts or Dockerfiles as needed for deployment.
