# ADF Ingestion Architecture

## Overview

Azure Data Factory (ADF) is used to ingest raw insurance data from the Landing layer into the Bronze layer in Azure Data Lake Storage Gen2.

The ingestion pipeline is designed to be reusable using pipeline and dataset parameters instead of creating separate pipelines for every source.

## Data Flow

Landing Layer
    |
    v
ds_landing_csv
    |
    v
pl_ingest_file
    |
    v
Copy Activity
    |
    v
ds_bronze_csv
    |
    v
Bronze Layer

## Generic Pipeline

Pipeline:

pl_ingest_file

The pipeline copies CSV files from the Landing layer to the Bronze layer.

### Pipeline Parameters

- directory_name
- file_name

These parameters allow the same pipeline to process different datasets.

## Generic Datasets

### Source Dataset

ds_landing_csv

Source path:

insurance/landing/{directory_name}/{file_name}

### Sink Dataset

ds_bronze_csv

Destination path:

insurance/bronze/{directory_name}/{file_name}

## Example - Customers

Parameters:

directory_name = customers
file_name = customers.csv

Data movement:

insurance/landing/customers/customers.csv
→
insurance/bronze/customers/customers.csv

## Example - Hospitals

Parameters:

directory_name = hospitals
file_name = hospitals.csv

Data movement:

insurance/landing/hospitals/hospitals.csv
→
insurance/bronze/hospitals/hospitals.csv

## Why Parameterization?

Parameterization avoids creating separate datasets and pipelines for every source entity.

Instead of creating:

- pl_ingest_customers
- pl_ingest_hospitals
- pl_ingest_claims
- pl_ingest_policies

a single reusable pipeline can process multiple datasets by receiving different parameter values.

This improves:

- Reusability
- Scalability
- Maintainability
- Consistency

## Testing

The pipeline was tested using ADF Debug mode.

Test 1:

directory_name = customers
file_name = customers.csv

Test 2:

directory_name = hospitals
file_name = hospitals.csv

Both tests copy data from the Landing layer to the Bronze layer using the same parameterized pipeline.

## Next Improvement

The current pipeline requires parameter values to be supplied manually.

The next improvement is to make ingestion metadata-driven so multiple datasets can be processed automatically without manually running the pipeline for each dataset.