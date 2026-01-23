# Project Title
Financial Transactions Analytics Platform (PySpark & Spark SQL)

## Project Overview

This project implements a scalable batch data pipeline using PySpark and Spark SQL to ingest, clean, transform, and analyze large-scale financial transaction data. The pipeline produces curated analytical tables that can be consumed by BI tools and downstream analytics workloads.

The goal is to transform raw financial datasets (transactions, customers, and cards) into business-ready analytical tables that support customer behavior analysis, card risk monitoring, and merchant performance insights and also questions like; which card type has the highest transactions

The project follows modern data engineering best practices, including:

Layered data architecture (Raw → Processed → Curated)

Schema standardization and data quality handling

Spark SQL–driven analytics

Analytics-ready data modeling

This repository is designed to demonstrate real-world data engineering skills expected by recruiters and senior engineers.

## Problem Statement

Financial institutions generate large volumes of transactional data daily. However, raw data alone cannot support effective decision-making.

Key challenges addressed:

Inconsistent schemas and data types

Poor data quality (string-based numeric fields, missing values, errors)

Lack of business-aligned analytical models

Difficulty answering high-impact financial and risk-related questions

This project solves these challenges by building a scalable Spark-based pipeline that converts raw financial data into structured, query-optimized analytics tables.

The objective of this project is to design and implement a scalable batch data pipeline using PySpark and Spark SQL that cleans, standardizes, and transforms raw financial transaction data into curated, analytics-ready tables.

## Data Source

The dataset represents anonymized individual financial transaction records containing transactions, users and cards data, simulating real-world banking data.
The transaction data represents 
Each record corresponds to a single transaction and includes customer identifiers, timestamps, transaction amounts, and status indicators.


## Architecture Overview

The pipeline follows a layered data architecture pattern:

1. Raw Layer
   - Stores source transaction data in CSV format
   - Represents data as received from upstream systems

2. Processed Layer
   - Data is cleaned, deduplicated, and standardized using PySpark
   - Invalid records are filtered out
   - Data types are normalized
   - Sensitive data dropped

3. Curated Layer
   - Business-ready tables are generated using Spark SQL
   - Aggregations and metrics are computed for analytics use cases

Data Flow:
Raw Data → PySpark Transformations → Spark SQL Analytics → Curated Tables (Parquet) → Joining data to answer some business questions






 