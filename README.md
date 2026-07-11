# Flight-Booking-Analytics-Platform-Databricks-x-dbt-

An end-to-end Data Engineering project built on Databricks that processes flight booking data using the Medallion Architecture. The project demonstrates incremental data ingestion, scalable ETL pipelines, dimensional modeling, and analytics-ready datasets using PySpark, Delta Lake, Lakeflow Declarative Pipelines, and dbt.

---

## Overview

This project simulates a production-grade data engineering workflow for a flight booking system. Raw booking data is ingested into the Bronze layer, transformed and validated in the Silver layer, and modeled into a Star Schema in the Gold layer for reporting and analytics.

The pipeline supports incremental data loading, Slowly Changing Dimensions (SCD Type 1), dynamic fact table generation, and SQL-based transformations with dbt.

---

## Tech Stack

- Databricks
- PySpark
- SQL
- Delta Lake
- Databricks Auto Loader
- Lakeflow Declarative Pipelines (DLT)
- dbt
- AWS S3
- Git & GitHub

---

## Architecture

```
AWS S3
   │
   ▼
Bronze Layer
(Auto Loader)
   │
   ▼
Silver Layer
(Data Cleaning & Validation)
   │
   ▼
Gold Layer
(Star Schema)
   │
   ▼
dbt Models
   │
   ▼
Analytics & Reporting
```

---

## Features

- Incremental data ingestion using Databricks Auto Loader
- Medallion Architecture (Bronze, Silver, Gold)
- ETL pipelines developed with PySpark
- Delta Lake for reliable storage
- Slowly Changing Dimension (SCD Type 1)
- Dynamic Fact Table Builder
- Star Schema implementation
- Data quality validation
- Incremental processing
- dbt integration for modular SQL transformations

---

## Data Pipeline

### Bronze Layer
- Ingests raw flight booking files from AWS S3
- Supports incremental loading
- Handles schema evolution
- Stores raw Delta tables

### Silver Layer
- Cleans and validates booking data
- Removes duplicate records
- Standardizes data formats
- Applies business transformation logic

### Gold Layer
- Creates dimension tables
- Creates fact tables
- Implements SCD Type 1
- Produces analytics-ready datasets for reporting

---

## Project Structure

```
Flight-Booking-Analytics/

├── Setup.py
├── SrcParameters.py
├── BronzeLayer.py
├── dltPipeline.py
├── GOLD_DIMS.py
├── GOLD_FACT.py
├── Initial Load/
├── Incremental Load/
├── SCD data load/
└── README.md
```

---

## Key Learnings

- Built scalable ETL pipelines using Databricks and PySpark
- Implemented incremental data ingestion with Auto Loader
- Applied Medallion Architecture for data organization
- Built reusable SCD Type 1 and Fact Table pipelines
- Performed dimensional modeling using Star Schema
- Integrated dbt with Databricks for SQL transformations
- Implemented data quality validation and incremental processing

---

## Future Improvements

- Apache Airflow orchestration
- CI/CD using GitHub Actions
- Data observability and monitoring
- Automated testing
- Real-time streaming with Kafka

---

## Author

**Manaswee Balkawade**
