# Utility Telemetry Lakehouse

A production style data engineering pipeline built on operational domain expertise from over 3 years of infrastructure monitoring at United Utilities — one of the UK's largest regulated water companies, serving over 7 million customers across the North West.

Built by **Emi Shyngle** | [linkedin.com/in/emishyngle](https://linkedin.com/in/emishyngle) | [@datafromthepipe](https://github.com/datafromthepipe)

---

## Domain Context

Working across water, wastewater, telemetry, and network operations, I interpret high volume real-time sensor data from pumping stations, service reservoirs, CSOs and WwTW assets —triaging, managing compliance events end to end, and making data driven decisions where the cost of a wrong call is measured in service disruption to millions of people, fines and loss of confidence in the business by customers.

That operational depth is the foundation of this project. The anomaly detection thresholds, telemetry freshness checks, data quality rules, and risk scoring logic are not arbitrary, they are derived from years of watching what good and bad data looks like in a live production environment where data quality has real world consequences.

This DE portfolio project uses a dataset engineered from the ground up using the same thinking that drives operational decisions in a regulated infrastructure environment.

---

## Architecture Overview

```
Raw Sources
    │
    ▼
┌─────────────────────────────────────────┐
│  Ingestion Layer (Python)               │
│  • Synthetic SCADA generator            │
│  • Environment Agency Flood API         │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│  BRONZE LAYER (Delta Lake / Databricks) │
│  Raw data preserved exactly as ingested │
│  Audit columns: ingested_at, source_file│
│  Partitioned by site_id                 │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│  SILVER LAYER (PySpark)                 │
│  Cleaned, validated, feature-engineered │
│  Window functions: rapid rise detection │
│  Deduplication and null handling        │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│  GOLD LAYER (SQL / Delta Lake)          │
│  Star schema: fact + dimension tables   │
│  Hourly risk scoring with CTEs          │
│  Surrogate keys, referential integrity  │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│  PRESENTATION LAYER (Power BI)          │
│  Operational dashboard                  │
│  KPI cards, risk table, trend charts    │
│  Colour-coded site risk classification  │
└─────────────────────────────────────────┘
```

---

## Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.11, SQL |
| Data Processing | Apache Spark, PySpark |
| Lakehouse Platform | Databricks, Delta Lake |
| Orchestration | Azure Data Factory |
| Cloud | Microsoft Azure |
| BI and Dashboards | Power BI |
| ML Layer | scikit-learn (Random Forest classifier) |
| Version Control | Git, GitHub |
| Libraries | Pandas, NumPy, requests, python-dotenv, SQLAlchemy |

---

## Data Sources

**Synthetic SCADA Data**
Python-generated sensor readings for 5 monitoring sites with realistic normal distributions and a deliberately injected rapid level rise anomaly — simulating the kind of pump failure or pipe burst event I respond to operationally at UU. This anomaly is what the Silver layer window function detects.

**Environment Agency Flood Monitoring API**
Live operational data from UK flood monitoring stations. Free, no authentication required, updated in real time. Endpoint: `https://environment.data.gov.uk/flood-monitoring/id/stations`

---

## Project Structure

```
utility-telemetry-lakehouse/
├── src/
│   ├── config.py              # Centralised config and path management
│   ├── logger.py              # Structured pipeline logging
│   ├── generate_scada.py      # Synthetic SCADA data generator
│   ├── ingest_ea_api.py       # Environment Agency API ingestion
│   └── quality_gate.py        # Data validation and quality rules
├── notebooks/
│   ├── 01_bronze_ingestion.py # Databricks: Bronze Delta table load
│   ├── 02_silver_cleaning.py  # Databricks: Silver PySpark transforms
│   ├── 03_gold_sql.sql        # Gold layer risk scoring and star schema
│   └── 04_ml_model.py         # Risk classification model
├── data/
│   ├── raw/                   # Raw ingested data — never modified
│   └── processed/             # Validated and transformed outputs
├── logs/                      # Pipeline execution logs
├── tests/                     # Unit tests
└── README.md
```

---

## Key Engineering Decisions

**Why Delta Lake over plain Parquet?**
Delta adds ACID transactions, time travel, and schema enforcement on top of Parquet. In a production pipeline serving a regulated organisation, the ability to audit historical states and roll back bad loads is not optional.

**Why partition by site_id in the Bronze layer?**
With hundreds of monitoring sites each sending readings every 15 minutes, unpartitioned tables become full table scans at query time. Partitioning means Spark reads only the relevant site folder when querying a specific asset.

**Why window functions for anomaly detection rather than simple thresholds?**
A static threshold on level_m would flag any high reading regardless of rate of change. A rapid rise of 0.4 metres in 15 minutes is operationally significant. A steady level of 2.8 metres that has been stable for hours may not be. The window function calculates the delta between each reading and the previous one, the same mental calculation I make operationally when assessing whether an alarm is genuine.

**Why a star schema in the Gold layer?**
Power BI performs significantly better against a properly modelled star schema than against a flat table. Dimension tables also allow the business to add descriptive attributes to sites, dates, and events without modifying the fact table, standard data warehouse practice.

---

## Work in Progress

| Week | Status | What I built |
|---|---|---|
| Week 1 | ✅ Complete | Project setup, config.py, logger.py, README |
| Week 2 | ⏳ Up next | Synthetic SCADA data generator with anomaly injection |
| Week 3 | ⏳ Coming | Environment Agency API ingestion layer |
| Week 4 | ⏳ Coming | Data quality gate with row-level validation rules |
| Week 5 | ⏳ Coming | Bronze Delta Lake layer in Databricks |
| Week 6 | ⏳ Coming | Silver layer PySpark transforms and window functions |
| Week 7 | ⏳ Coming | Gold layer SQL risk scoring and star schema |
| Week 8 | ⏳ Coming | Power BI operational dashboard |

---

## Background

**MSc Data Science** . Machine learning, NLP, statistical modelling, Python, R.

**BSc Statistics** — Quantitative analysis and applied research.

***PGDiploma Business Administration** - Business, Accounting and Statistics.

**8+ years** of data analysis, BI development, and operational analytics across regulated environments.


---

## Contact

Open to Data Engineer, BI Analyst, and Data Analyst roles — remote or hybrid across the North West.

**Email:** emishyngle@gmail.com
**LinkedIn:** [linkedin.com/in/emishyngle](https://linkedin.com/in/emishyngle)
**Content:** [@datafromthepipe](https://tiktok.com/@datafromthepipe) — building this pipeline in public