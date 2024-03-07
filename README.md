# Sigma Software Internal Dataplatform
Internal framework for ingestion, ETL/ELT workloads.

## Project Description
The project contains of three main parts: ingestion, processor, and utility packages.

Ingestion package is responsible for the extraction of the data from data sources, doing minor 
enrichments, and loading data into data warehouse.

Processor package serves as a post-loading data modification tool.

Utility package is a place for the service-oriented modules, focused on working
with the data assets as is and supplementary meta-information.

## Building
To build Docker image of the project run:
```bash
cd dwh-etl
docker build -f Dockerfile -t <image-name>:<image-tag> .
```

## Running
```bash
docker run <image-name>:<image-tag> <command>
```
To list available commands run the container from image without command specification:
```bash
docker run <image-name>:<image-tag>
```

## Testing and Linting
Before testing locally, you should install packages into your environments using:
```bash
make init
```
---

Test with the HTML-coverage report can be run locally using:
```bash
make INCLUDE_HTML=true <command>
```
Commands can be inferred from the `Makefile`.
> Note: coverage is measured only for those files that are run during unit testing.

---
To run all tests run:
```bash
make test-all
```

To run the linter use:
```bash
make lint
```

Linter config can be adjusted in `pylintrc` file.

## Troubleshooting