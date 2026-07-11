# Flight-Booking-Analytics-Platform-Databricks-x-dbt-

# FMCG Sales Analytics Lakehouse

An end-to-end Data Engineering project built using Databricks implementing the Medallion Architecture (Bronze, Silver, Gold) with Delta Lake, PySpark, Lakeflow Declarative Pipelines, and dbt.

## Overview

This project demonstrates how modern data engineering pipelines are built using Databricks. Raw sales data is incrementally ingested, transformed, validated, and modeled into analytics-ready datasets using scalable ETL pipelines.

The project implements reusable pipelines for data ingestion, dimensional modeling, Slowly Changing Dimensions (SCD Type 1), and dynamic fact table generation.

---

## Tech Stack

- Databricks
- PySpark
- SQL
- Delta Lake
- Lakeflow Declarative Pipelines (DLT)
- Databricks Auto Loader
- dbt
- Git

---

## Architecture

Landing Data

↓

Bronze Layer
- Incremental ingestion
- Auto Loader
- Delta Tables

↓

Silver Layer
- Data Cleaning
- Validation
- Transformations
- DLT Pipelines

↓

Gold Layer
- Star Schema
- Dimension Tables
- Fact Tables
- SCD Type 1

↓

dbt Models

↓

BI Reporting

---

## Features

- Incremental file ingestion using Auto Loader
- Dynamic ETL pipelines
- Medallion Architecture
- Delta Lake
- Slowly Changing Dimension (Type 1)
- Dynamic Fact Table Builder
- Star Schema
- Data Validation
- Analytics-ready datasets

---

## Project Structure

```
setup/
bronze/
silver/
gold/
parameters/
scd/
README.md
```

---

## Workflow

1. Configure workspace and parameters

2. Ingest raw data into Bronze layer

3. Clean and transform data into Silver layer

4. Build Dimension tables

5. Build Fact tables

6. Apply SCD Type 1

7. Generate Gold layer

8. Build reporting models using dbt

---

## Key Learnings

- Databricks Lakehouse
- Incremental ETL Pipelines
- Delta Lake
- Dynamic PySpark notebooks
- SCD Type 1
- Star Schema
- Data Quality Validation
- dbt Integration

---

## Author

**Manaswee Balkawade**
