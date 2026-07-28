# Insurance Claims Data Platform Architecture

## Business Requirement

The insurance company receives data related to customers, policies, hospitals, claims, and payments from multiple source systems.

The platform will ingest this data into Azure, preserve the raw data, clean and validate it, and create business-ready datasets for analytics.

## High-Level Architecture

Source Systems
      |
      v
Azure Data Factory
      |
      v
ADLS Gen2 - Bronze
      |
      v
Azure Databricks + PySpark
      |
      v
ADLS Gen2 - Silver
      |
      v
Gold Layer
      |
      v
Analytics

## Architecture Components

### Azure Data Factory

Used to ingest data from source systems and orchestrate data pipelines.

### Azure Data Lake Storage Gen2

Used as the central storage layer for the data platform.

### Azure Databricks

Used to process and transform large datasets using PySpark.

## Medallion Architecture

### Bronze Layer

Stores raw data received from source systems with minimal modification.

### Silver Layer

Stores cleaned, validated, standardized, and deduplicated data.

### Gold Layer

Stores business-ready datasets designed for analytics and reporting.

## Planned Data Sources

The platform will initially work with:

- Customers
- Policies
- Hospitals
- Claims
- Payments