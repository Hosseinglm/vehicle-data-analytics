# 🚗 Real-time Vehicle Data Analytics Pipeline On AWSFrom Streaming To ML Insights

![Uploading image.png…]()


This repository contains a complete, production-ready data pipeline for ingesting, transforming, analyzing, and modeling vehicle tracking data using **AWS services** and **Python-based tools**. The project demonstrates real-time data engineering and machine learning capabilities in the cloud.

---

## 📌 Table of Contents

- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Directory Structure](#directory-structure)
- [Pipeline Components](#pipeline-components)
- [Setup Instructions](#setup-instructions)
- [Results Summary](#results-summary)
- [Usage Examples](#usage-examples)
- [License](#license)

---

## 📍 Overview

This pipeline was designed to simulate real-world vehicle tracking data and process it through a complete cloud-based ETL and ML workflow. The system includes:

- Simulated real-time vehicle tracking using AWS Kinesis.
- Data ingestion and cleaning using AWS Glue and PySpark.
- Schema inference and cataloging with Glue Crawlers.
- Data storage in S3 (raw and processed formats).
- Querying with Amazon Athena.
- Modeling and anomaly detection using Amazon SageMaker and Python.

---

## 📊 Architecture Diagram

![Architecture](Diagram.png)

---

## ✨ Features

- Real-time data simulation and ingestion using `boto3` and Kinesis.
- Scalable data transformation using AWS Glue + PySpark.
- Automated schema discovery using Glue Crawlers.
- Querying processed data directly from S3 using Amazon Athena.
- Predictive modeling and anomaly detection (Z-Score, Isolation Forest, LOF).
- Exploratory Data Analysis (EDA) visualized using Matplotlib and Seaborn.

---

## 🧰 Technologies Used

- **AWS Services**: Kinesis, S3, Lambda, Glue, Glue Crawler, Athena, SageMaker  
- **Programming Languages**: Python (3.8+)  
- **Libraries**:  
  - Data: `pandas`, `numpy`, 'Sk-learn', `PysPark`  
  - AWS: `boto3`, `Athena`, `AWS Glue`, 'S3', 'Kinesis', 'Lambda', 'SageMaker', 'IAM'  
  - ML: `scikit-learn`, `xgboost`  
  - Visualization: `matplotlib`, `seaborn`

---


---

## ⚙️ Pipeline Components

### 1. Data Ingestion
The `vehicle-data-stream.py` script streams rows from a CSV file to an AWS Kinesis Data Stream using `boto3`.

### 2. Data Storage
AWS Lambda is triggered by Kinesis and writes raw events to an S3 bucket in JSON format.

### 3. ETL & Processing
The `pyspark_cleaning.py` script (run in AWS Glue) performs:

- JSON parsing from the `details` column  
- Type casting and schema normalization  
- Missing value imputation  
- Duplicate removal  
- Writes clean data to S3 in Parquet and CSV formats

### 4. Schema Discovery
AWS Glue Crawler infers schema from the processed data and registers it in the Glue Data Catalog.

### 5. Data Querying
The `athena_connection.py` script allows querying the S3-structured data via Athena and returns results as Pandas DataFrames.

### 6. Machine Learning & Analytics
- **EDA**: Speed distribution, traffic trends by hour/day, vehicle class frequency  
- **Prediction**: Estimating `estimated_speed` using:
  - Linear Regression
  - Random Forest
  - SVR
  - XGBoost  
- **Anomaly Detection**:
  - Z-Score method
  - Isolation Forest
  - Local Outlier Factor
  - Consensus approach

---

## 📈 Results Summary

### 🔢 Regression Models

| Model               | RMSE   | MAE   | R²   |
|--------------------|--------|-------|------|
| Linear Regression  | 13.61  | 9.26  | 0.57 |
| Random Forest      | 13.71  | 9.47  | 0.56 |
| SVR                | 20.76  | 11.65 | -0.00 |
| **XGBoost**        | 13.58  | 9.38  | 0.57 |

### 🚨 Anomaly Detection

| Method              | Description                                | Anomalies | % of Data |
|---------------------|---------------------------------------------|-----------|-----------|
| Z-Score             | Statistical method                          | 322       | 1.15%     |
| Isolation Forest    | Tree-based method                           | 1404      | 5.00%     |
| Local Outlier Factor| Density-based (local density comparisons)   | 1404      | 5.00%     |
| Consensus Approach  | Agreement across methods                    | 383       | 1.36%     |

---

## 🧪 Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/your-username/vehicle-data-pipeline.git
cd vehicle-data-pipeline

## 📜 License
This project is licensed under the MIT License.
See the LICENSE file for full details.

