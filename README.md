# Entertainment Data Pipeline

## Project Overview

Cloud-native Data Engineering pipeline built on Google Cloud Platform (GCP) to ingest, process, and analyze trending entertainment data from the TMDB API.

This project follows a Medallion Architecture (Bronze → Silver → Gold) using PySpark transformations and cloud storage.

The pipeline extracts trending movie and TV show data, performs data cleaning and transformations, and generates analytical datasets for downstream reporting and dashboards.

---

# Architecture

TMDB API
↓
Bronze Layer (Raw JSON)
↓
Silver Layer (Cleaned & Standardized Data)
↓
Gold Layer (Business Aggregations)
↓
SQL Analytics
↓
Dashboard / Reporting

---

# Tech Stack

- Python
- PySpark
- Google Cloud Storage (GCS)
- Google Cloud Platform (GCP)
- TMDB API
- SQL
- Dataproc (Planned)
- BigQuery (Planned)
- Looker Studio (Planned)

---

# Features

## Bronze Layer
- TMDB API extraction
- Pagination handling
- Metadata enrichment
- GCS upload
- Logging and error handling

## Silver Layer
- Schema refinement
- Null handling
- Deduplication
- String standardization
- Business-friendly transformations
- Schema validation

## Gold Layer
- Trending analytics
- Popularity KPIs
- Rating aggregations
- Language distribution analytics
- Business-ready datasets

---

# Project Structure

```bash
Data_Engineer_Project/

├── extract/
│   └── tmdb_extract.py
│
├── transform/
│   ├── silver_transform.py
│   └── gold_transform.py
│
├── sql/
│   └── analytics_queries.sql
│
├── configs/
├── logs/
├── architecture/
├── dashboard/
│
├── README.md
├── requirements.txt
├── .gitignore# gcp-data-engineering-project
Cloud-native TMDB analytics pipeline built using Python , PySpark, GCS, BigQuery, and Dataproc on GCP.
