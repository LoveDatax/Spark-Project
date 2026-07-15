# Project Title
Financial Transactions Analytics Platform (PySpark & Spark SQL)

# Project Overview

This project implements a scalable batch data pipeline using PySpark and Spark SQL to ingest, clean, transform, and analyze large-scale financial transaction data. The pipeline produces curated analytical tables that can be consumed by BI tools and downstream analytics workloads and answer some business questions.

The goal is to transform raw financial datasets (transactions, customers, and cards) into business-ready analytical tables that support customer behavior analysis, card risk monitoring, and merchant performance insights and also questions like; which card type has the highest transactions.

The project follows modern data engineering best practices, including:

- Layered data architecture (Raw → Processed → Curated),
- Schema standardization and data quality handling
- Data Normalization
- Spark SQL–driven analytics,
- Analytics-ready data modeling

# Problem Statement

Financial institutions generate large volumes of transactional data daily. However, raw data alone cannot support effective decision-making.

Key challenges addressed:

- Inconsistent schemas and data types
- Poor data quality (string-based numeric fields, missing values, errors),
- Lack of business-aligned analytical models,
- Difficulty answering high-impact financial and risk-related questions

This project solves these challenges by building a scalable Spark-based pipeline that converts raw financial data into structured, query-optimized analytics tables.

The objective of this project is to design and implement a scalable batch data pipeline using PySpark and Spark SQL that cleans, standardizes, and transforms raw financial transaction data into curated, analytics-ready tables.

# Data Source

The dataset represents anonymized individual financial transaction records containing transactions, users and cards data, simulating real-world banking data.

Transaction Data:
Each record in the transaction data corresponds to a single transaction and includes customer identifiers, merchant details, timestamps, transaction amounts, chip transactions and errors.

Users Data:
Each record in the users data contains personal information of a single user such as age, birth year and month, gender, location and financial activity of the user(capita per income, yearly income, debts, etc.)

Cards Data:
Each records contain card informations(including sensitive info like cvv and card number) which includes card expiry date, card type, card brand, and when card pin was changed

# Architecture Overview
The pipeline follows a layered data architecture pattern:

1. Raw Layer
   - Stores source data in CSV format
   - Represents data as received from upstream systems

2. Processed Layer
   - Data is cleaned, deduplicated, and standardized using PySpark
   - Invalid records are filtered out
   - Data types are normalized
   - Sensitive data dropped

3. Curated Layer
   - Business-ready tables are generated using Spark SQL
   - Aggregations and metrics are computed for analytics use cases
   - Important columns are computed

Link to all data - https://drive.google.com/drive/folders/1TgscAhQtz0izpGobow1cZTGDxMQbh0ny?usp=sharing


## Data Flow:
Raw Data → PySpark Transformations → Spark SQL Analytics → Curated Tables (Parquet) → Joining data to answer some business questions

# Transformation Logic 
The pipeline performs the following transformations:

1. Schema Enforcement
   - Explicit data types are applied to ensure consistency
   - Invalid records with missing transaction IDs are removed
   - Some NULL values were replaced with "ONLINE" as they were online transactions

2. Data Cleaning
   - Duplicate transactions are dropped
   - Transactions with negative or zero amounts are filtered out

3. Data Standardization
   - Transaction dates are converted to a standard date format
   - Currency values are normalized

4. Aggregation
   - Customer-level metrics such as total spend and transaction count are computed
   - Aggregations are performed using Spark SQL for optimized execution

# Business Questions
- Which merchant cities have the highest transaction volume?
- Which merchant cities have the highest transaction error?
- How does card type influence spending behavior?
- Which card type is most used?
- What is the average transaction value per customer?
- How does age affect customer spending?
- Does age influence credit score?

# Data Model
## Curated Tables
1. customer_financial_metrics
- One row per customer (client_id)
### Purpose:
- Customer lifetime value
- Spending behavior
- Credit and income analysis
### Key metrics:
- Total transactions
- Total and average spend
- Credit score, income, debt
- First and last transaction dates

2. card_usage_risk_metrics
-One row per card (card_id)
### Purpose:
- Card usage analysis
- Error and risk monitoring
- Chip usage evaluation
### Key metrics:
- Transaction count
- Total card spend
- Error rate
- Chip vs non-chip transactions
- Expired card usage

3. merchant_geography_metrics
- City × State × Merchant Category (MCC)
### Purpose:
- Merchant performance tracking
- Geographic transaction concentration
- Error identification
### Key metrics:
- Transaction volume
- Total spend
- Error counts and 
- Error rates

# How to Run Project
spark-submit /DataProject.py

# Tech Stack
- Apache Spark (PySpark)
- Spark SQL
- Python
- Parquet (columnar storage)
- Git & GitHub
- Linux environment

# Key Skills Demonstrated
- End-to-end data pipeline design
- PySpark data processing
- Spark SQL analytics
- Financial data modeling
- Data quality and schema enforcement
- Business-driven analytics engineering

# Future Improvements
- Airflow orchestration
- Data quality checks
- Incremental data processing
- Cloud deployment (S3 + EMR / Databricks)
- BI integration (Power BI / Tableau)

# Author
Love Adeola - Data Engineer
