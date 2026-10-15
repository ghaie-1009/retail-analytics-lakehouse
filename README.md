# Retail Analytics Lakehouse

A hands-on data engineering project building a multi-layer lakehouse using
PySpark and Delta Lake, based on the Brazilian E-Commerce (Olist) dataset.

## Project Status

**In progress** — Bronze ingestion and Silver transformations and data quality
validation have been implemented. Gold analytics and further testing are planned.

## Tech Stack

- Python
- Apache Spark (PySpark)
- Delta Lake
- Git and GitHub

## Architecture

The project follows a medallion architecture:

- **Bronze:** Ingest source datasets into Delta Lake tables.
- **Silver:** Clean and transform datasets, validate keys and relationships,
  and perform data quality checks.
- **Gold:** Planned layer for analytics-ready fact tables and business metrics.

## Current Implementation

- Ingested multiple e-commerce datasets into Bronze.
- Built Silver transformations for orders, dimensions, geolocation,
  reviews, and transaction-related datasets.
- Validated primary keys, duplicate composite keys, and relationships between
  datasets.
- Checked order statuses, delivery timestamps, review data, and geolocation
  records.
- Investigated data quality issues found during validation.

## Repository Structure

```text
retail-analytics-lakehouse/
├── data/
│   ├── raw/       # Local source datasets; not committed
│   ├── bronze/    # Generated Delta tables; not committed
│   ├── silver/    # Generated Delta tables; not committed
│   └── gold/      # Planned analytics layer; not committed
├── src/           # Ingestion, transformation, and validation scripts
├── .gitignore
└── README.md
