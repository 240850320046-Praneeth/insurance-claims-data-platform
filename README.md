# Insurance Claims Data Platform

## Business Problem

An insurance company receives claims data from multiple sources. The incoming data may contain duplicate records, missing values, invalid formats, and inconsistent information.

The goal of this project is to build an Azure Data Engineering pipeline that ingests, validates, transforms, and prepares insurance claims data for analytics.

## High-Level Architecture

Source Systems
→ Azure Data Factory
→ ADLS Gen2 Bronze
→ Azure Databricks / PySpark
→ Silver
→ Gold
→ Analytics

## Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- SQL
- Delta Lake
- Git/GitHub

## Project Progress

### Day 1

- Activated Azure for Students subscription
- Created project resource group
- Defined the business problem
- Defined the high-level architecture
- Created GitHub repository
- Set up Git-based project structure

### Day 2

- Designed the insurance source data model
- Defined primary and foreign key relationships
- Created Azure Storage Account
- Enabled ADLS Gen2 hierarchical namespace
- Created the insurance data lake container