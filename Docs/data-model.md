# Insurance Claims Data Model

## Overview

The platform processes five core datasets:

- Customers
- Policies
- Hospitals
- Claims
- Payments

## Customers

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| first_name | Customer first name |
| last_name | Customer last name |
| date_of_birth | Customer date of birth |
| gender | Customer gender |
| email | Email address |
| phone | Phone number |
| city | Customer city |
| created_at | Customer registration timestamp |

Primary Key: customer_id

## Policies

| Column | Description |
|---|---|
| policy_id | Unique policy identifier |
| customer_id | Customer owning the policy |
| policy_type | Type of insurance policy |
| start_date | Policy start date |
| end_date | Policy end date |
| coverage_amount | Maximum policy coverage |
| premium_amount | Policy premium |
| policy_status | Current policy status |
| updated_at | Last update timestamp |

Primary Key: policy_id

Foreign Key: customer_id → Customers

## Hospitals

| Column | Description |
|---|---|
| hospital_id | Unique hospital identifier |
| hospital_name | Hospital name |
| city | Hospital city |
| hospital_type | Type of hospital |
| network_status | Insurance network status |

Primary Key: hospital_id

## Claims

| Column | Description |
|---|---|
| claim_id | Unique claim identifier |
| policy_id | Policy associated with claim |
| customer_id | Customer submitting claim |
| hospital_id | Hospital providing treatment |
| claim_date | Date claim was submitted |
| claim_amount | Amount requested |
| claim_type | Type of claim |
| claim_status | Current claim status |
| updated_at | Last update timestamp |

Primary Key: claim_id

Foreign Keys:

- policy_id → Policies
- customer_id → Customers
- hospital_id → Hospitals

## Payments

| Column | Description |
|---|---|
| payment_id | Unique payment identifier |
| claim_id | Claim associated with payment |
| payment_date | Payment date |
| payment_amount | Amount paid |
| payment_status | Payment status |
| payment_method | Payment method |
| updated_at | Last update timestamp |

Primary Key: payment_id

Foreign Key: claim_id → Claims

## Relationships

Customer → Policy → Claim → Payment

Hospital → Claim