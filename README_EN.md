
# Data Engineering Project – Medallion Pipeline with Azure Blob Storage (JIRA SLA)

## Overview

This project implements a **professional Python Data Engineering pipeline** following the **Medallion Architecture (Bronze, Silver, Gold)** to process **JIRA issue data** and calculate **SLA (Service Level Agreement)** metrics.

The pipeline is fully automated and includes:
- Data ingestion from **Azure Blob Storage** (with local fallback);
- Layered data processing and normalization;
- Business rules for SLA calculation;
- Generation of analytical datasets and Excel-ready reports.

This README is written in a **non-technical and explanatory way**, allowing non-developers to understand the project value.

---

## What Is a Data Pipeline?

A data pipeline is an automated process that:
1. Collects data from a source;
2. Cleans and structures the data;
3. Applies business rules;
4. Delivers reliable information for analysis.

---

## Pipeline Architecture – Medallion

### 🥉 Bronze – Raw Data Ingestion
- Reads raw JIRA JSON data;
- Primary source: Azure Blob Storage;
- Fallback: local file for development;
- No business rules applied.

Output:
- `data/bronze/bronze_issues.parquet`

---

### 🥈 Silver – Clean and Normalized Data
- Column standardization;
- Text normalization;
- Date and timestamp processing;
- Analytics-ready dataset.

Output:
- `data/silver/silver_issues.parquet`

---

### 🥇 Gold – Business Rules and SLA Metrics
- Filters completed issues;
- Calculates resolution time in business days;
- Excludes weekends and Brazilian national holidays;
- Defines SLA targets by priority;
- Flags SLA compliance;
- Generates management reports.

Output:
- `data/gold/gold_sla_issues.parquet`
- Excel reports.

---

## SLA Calculation Logic

| Priority | Expected SLA |
|---------|--------------|
| High    | 24 hours     |
| Medium  | 72 hours     |
| Low     | 120 hours    |

Rules:
- Only business days are considered;
- One business day equals 24 hours;
- Weekends and national holidays are excluded;
- SLA is met when resolution time is less than or equal to the expected SLA.

---

## Data Dictionary – Final Table

### `gold_sla_issues`

| Column | Description |
|------|-------------|
| issue_id | Unique issue identifier |
| issue_type | Issue type |
| status | Final status |
| priority | Issue priority |
| assignee_id | Analyst ID |
| assignee_name | Analyst name |
| assignee_email | Analyst email |
| created_at | Creation timestamp |
| resolved_at | Resolution timestamp |
| resolution_hours | Resolution time in business hours |
| sla_expected_hours | Expected SLA |
| is_sla_met | SLA compliance flag |

---

## How to Run

```bash
python run_pipeline.py
```

Dependencies are installed automatically.

---

## Business Value

- SLA monitoring;
- Operational performance analysis;
- Bottleneck identification;
- Decision support with reliable data.

---

## Conclusion

This project demonstrates a complete, scalable and production-ready data pipeline aligned with industry best practices.
