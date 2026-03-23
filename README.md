### 📌 Overview

This project implements a small data warehouse and reporting mart using dbt on Databricks, based on a “mini DWH” dataset containing relational tables and event data.

### The goal is to:

build reliable datasets for reporting
define consistent business metrics
handle real-world data challenges (events, JSON)
deliver a clean, BI-ready KPI mart
### 🧱 Architecture

The project follows a layered dbt architecture:

sources (landing)
    ↓
staging (stg_)
    ↓
intermediate (int_)
    ↓
mart (mart_)
### Layers
staging (stg_)
column selection, renaming, casting
no business logic
intermediate (int_)
### business logic
joins, FX conversion
event aggregation
JSON normalization
mart (mart_)
final KPI aggregation
BI-ready dataset
### 📊 Final Deliverable
mart_transfers_kpi_daily

Daily KPI mart with the following grain:

metric_date (based on operated_date)
welcome_city_id
hailing_type
driver_type
Metrics
transfers_booked
transfers_operated
cancellation_rate
gmv
coupon_discount
driver_perfect_app_usage_rate
### 🧠 Key Design Decisions
1. Operated-Date Anchoring

The mart is anchored on operated_date, as most operational and financial metrics are tied to execution time.

This results in an operational view of performance rather than a booking funnel view.

2. Event Handling
Events are aggregated at transfer level
Converted into flags (presence-based)

This avoids fragile sequencing logic and ensures robustness.

3. JSON Normalization
Exchange rates JSON is parsed and exploded into a relational format

Enables standard joins and scalable currency handling.

4. Financial Accuracy
FX applied at operated_date
monetary calculations use decimal
safe division handling implemented
### 🧪 Testing Strategy

The project includes:

Basic tests
not_null
unique
accepted_values
Business logic tests
is_booked = 1
is_operated + is_cancelled <= 1
Custom test
Grain validation:
ensures mart row count matches distinct upstream grain

Protects against data duplication or loss during aggregation.

### ⚙️ Materialization
Layer	Materialization
staging	view
intermediate	view
mart	table

Views were used for simplicity and transparency, while the mart is materialized for performance.

### 🚀 How to Run
dbt deps
dbt build
### 🔧 Future Improvements
CI/CD pipelines for automated testing and deployment
source freshness and monitoring
separate booking vs operational marts
incremental models for scalability
semantic layer for KPI governance
### 📁 Project Structure
models/
  staging/
  intermediate/
  marts/

tests/
  generic/
### 🏁 Summary

This project demonstrates:

structured dbt modeling
handling of messy real-world data
consistent KPI definition
BI-ready dataset design

The result is a clean, scalable foundation for analytical reporting.

### 🔥 Optional (very nice touch)

At the top of your README, you can add:
