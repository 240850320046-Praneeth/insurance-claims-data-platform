# Synthetic Data Generation

## Purpose

The project uses synthetic insurance datasets to simulate a large-scale
insurance data platform without using real customer information.

## Target Scale

| Dataset | Target Records |
|---|---:|
| Customers | 500,000 |
| Hospitals | 10,000 |
| Policies | 1,000,000 |
| Claims | 5,000,000 |
| Payments | 3,000,000 |

## Data Quality Simulation

The source data intentionally contains quality issues such as:

- Missing values
- Invalid dates
- Invalid categorical values
- Duplicates
- Invalid foreign keys
- Invalid monetary values

These issues will later be detected during Silver-layer processing.

## Generation Strategy

Large datasets are generated incrementally rather than building the
entire dataset in memory.

Generated source datasets are not committed to Git.
The generator code is version controlled so datasets can be reproduced.