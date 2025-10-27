                              **OpsPulse Analytics — Supply Chain KPI Dashboard**

A Business-Driven Data Analytics Application

OpsPulse Analytics is an end-to-end data pipeline and visualization platform that monitors supplier performance, delivery reliability, and spend efficiency across the supply chain. It automates data ingestion, transformation, and machine learning–based risk prediction — visualized through an interactive Streamlit dashboard.

**Project Overview**

Organizations often struggle to monitor supplier performance across multiple sources and detect inefficiencies in time.
OpsPulse Analytics provides a unified analytics view of:

>On-Time Delivery Rate (OTD) — Measures delivery consistency.
>Fill Rate (FR) — Indicates supplier fulfillment capability.
>Contract Utilization — Tracks actual vs. committed spend.
>Spend Leakage — Quantifies purchases outside active contracts.
>ML-Based Late Shipment Prediction — Identifies risk of late shipments using historical delivery patterns.

| Layer            | Tools / Frameworks                          |
| ---------------- | ------------------------------------------- |
| Language         | Python 3.12                                 |
| Data Processing  | **DuckDB**, **Pandas**, **NumPy**           |
| Visualization    | **Streamlit**, **Matplotlib**               |
| Machine Learning | **Scikit-Learn (Logistic Regression)**      |
| ETL Workflow     | Custom Python ETL scripts                   |
| Data Storage     | Parquet Files + DuckDB local data warehouse |


**Schema:**

          ┌────────────────────────────┐
          │     Raw Data Generator     │
          │   (generate_data.py)       │
          └────────────┬───────────────┘
                       │
                       ▼
          ┌────────────────────────────┐
          │   ETL Transform & Validate │
          │   (transform_validate.py)  │
          └────────────┬───────────────┘
                       │
                       ▼
          ┌────────────────────────────┐
          │   DuckDB Local Warehouse   │
          │  (DDL + KPI SQL models)    │
          └────────────┬───────────────┘
                       │
                       ▼
          ┌────────────────────────────┐
          │   ML Model Training (Late) │
          │  Logistic Regression (SkL) │
          └────────────┬───────────────┘
                       │
                       ▼
          ┌────────────────────────────┐
          │   Streamlit KPI Dashboard  │
          │   (app/app.py)             │
          └────────────────────────────┘

 **Key Features**
**Automated ETL Pipeline**
Extracts, cleans, and transforms data into curated tables.
Dynamically builds DuckDB schemas with KPI models.

**Machine Learning Layer**
Trains a logistic regression model to predict shipment delays.
Calculates rolling averages of supplier metrics for trend forecasting.

**Interactive Dashboard**
Dynamic supplier filters.
KPI trend charts with 3-month moving averages and target benchmarks.
Tabular summaries of contract utilization and spend leakage.

**Execution Steps**
**1) Setup Environment:**

git clone https://github.com/<your-username>/opspulse-analytics.git
cd opspulse-analytics/opspulse-analytics
python -m venv .venv312
.\.venv312\Scripts\activate
pip install -r requirements.txt

**2)Run the Pipeline**
python run_pipeline.py

**3)Run the Dashboard**
streamlit run app/app.py

**ML Model:**
Algorithm: Logistic Regression
Target: Predict whether upcoming shipments will be late (target_late)
Features:
Rolling on-time rate (7-day window)
Rolling defect rate
Rolling fill rate
Quantity ordered/delivered

Order value
Metric: AUC and classification report
Output: late_shipment_model.pkl

**Results:**
| KPI                              | Description                          | Outcome                        |
| -------------------------------- | ------------------------------------ | ------------------------------ |
| **OTD (On-Time Delivery)**       | Tracks supplier delivery punctuality | 95–100% trend stability        |
| **Fill Rate**                    | Measures supply fulfillment          | ~90% average performance       |
| **Spend Leakage**                | Spend outside contract               | <15% leakage                   |
| **Late Shipment Prediction AUC** | Model performance                    | ~0.43 baseline AUC (prototype) |

**Possible Extention**

Integrate real-time supplier data APIs.
Enhance model using XGBoost / Time Series Forecasting (Prophet).
Add alerting & anomaly detection for supply risks.
Containerize app using Docker and deploy on AWS EC2 / Streamlit Cloud.

Author

Akhila Madanapati
Data Anlyst
M.S. Data Science
vinayrb217@gmail.com
[Linkdlen](https://www.linkedin.com/in/vinay-bera-b345a4367/)
