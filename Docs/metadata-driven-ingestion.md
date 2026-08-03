# Metadata-Driven Ingestion

## Overview

The ingestion framework uses a master Azure Data Factory pipeline to orchestrate ingestion of multiple datasets.

Instead of manually executing the generic ingestion pipeline for each dataset, a ForEach activity iterates through ingestion metadata.

## Architecture

Metadata
    |
    v
pl_master_ingestion
    |
    v
ForEach Dataset
    |
    v
Execute Pipeline
    |
    v
pl_ingest_file
    |
    v
Landing -> Bronze

## Metadata

The master pipeline contains an Array parameter named:

ingestion_metadata

Current datasets:

- customers
- hospitals

Each metadata entry contains:

- directory_name
- file_name

## Pipeline Responsibilities

### pl_master_ingestion

Responsible for orchestration.

It reads the metadata and iterates through each dataset using a ForEach activity.

### pl_ingest_file

Responsible for copying one dataset from the Landing layer to the Bronze layer.

Parameters:

- directory_name
- file_name

## Dynamic Execution

Inside the ForEach activity:

@item().directory_name

provides the current source directory.

@item().file_name

provides the current file name.

These values are passed to pl_ingest_file.

## Current Execution Strategy

The ForEach activity runs sequentially so pipeline execution and failures are easier to observe.

Parallel execution can be introduced later for improved throughput.

## Future Improvements

- External metadata/configuration store
- Parallel ingestion
- Error handlinggit
- Logging and auditing
- Retry strategy
- Data validation
- Incremental ingestion