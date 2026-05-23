# TRS Pension & Benefits Analytics Platform

## Overview

This project simulates an enterprise retirement and benefits analytics platform inspired by large public pension systems like the Teacher Retirement System of Texas (TRS).

The platform processes pension contributions, retirement transitions, healthcare claims, and pension payouts using PySpark and medallion architecture principles.

---

## Architecture

Bronze Layer → Raw pension and healthcare datasets

Silver Layer → Cleansed and transformed member lifecycle data

Gold Layer → Aggregated analytics datasets powering executive dashboards

---

## Technologies Used

- Python
- PySpark
- Spark SQL
- Delta Lake
- Tableau
- Git/GitHub

---

## Features

- Multi-source data ingestion
- Incremental contribution processing
- Retirement eligibility logic
- Pension payout analytics
- Healthcare claims analytics
- Slowly changing dimension simulation
- Gold-layer KPI aggregation
- Logging and monitoring
- Apache Airflow orchestration for automated ETL workflow scheduling
- Modular DAG-based pipeline execution
- Bronze, Silver, and Gold medallion architecture implementation

---

## Business KPIs

- Active vs Retired Members
- Total Pension Payouts
- Employee Contributions
- Employer Contributions
- Healthcare Claims Totals
- Average Salary Analysis

---

## Example Workflow

1. Load raw pension and healthcare datasets
2. Transform and cleanse data in Silver layer
3. Apply retirement eligibility business rules
4. Process incremental monthly contributions
5. Simulate member retirement lifecycle transitions
6. Generate Gold analytics datasets
7. Export dashboard-ready data for Tableau

---

## Future Enhancements

- Airflow orchestration concepts for modular ETL scheduling and workflow automation
- Docker containerization
- CI/CD with GitHub Actions
- Delta MERGE operations
- Real-time streaming ingestion
- Snowflake integration
- 

---

## Author

Oluwasegun Ogundele
