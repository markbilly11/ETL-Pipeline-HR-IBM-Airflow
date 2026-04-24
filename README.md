# 📊 HR Employee Data Pipeline Automation & Validation  
**Milestone 3 — Hacktiv8 Data Analytics Bootcamp**

An end-to-end data engineering project that automates data processing, validation, and storage using PySpark, Airflow, and MongoDB.

---

## 🧭 Project Overview  

This project builds an automated data pipeline to process HR employee data, ensuring the data is clean, validated, and ready for analysis.

---

## 🎯 Objective  

- Automate ETL pipeline using Apache Airflow  
- Perform data transformation using PySpark  
- Validate data using Great Expectations  
- Store processed data into MongoDB  

---

## 📂 Dataset  

IBM HR Analytics Dataset  

Contains:
- Employee demographics (Age, Gender, Education)  
- Job information (Department, Job Role, Income)  
- Work condition (Overtime, Work-life balance)  
- Attrition indicator  

---

## ⚙️ Data Pipeline  
Raw CSV
↓
Extract (PySpark)
↓
Transform (Cleaning & Processing)
↓
Validation (Great Expectations)
↓
Load (MongoDB)
↓
Airflow DAG (Scheduling)

---

## 🔄 ETL Process  

### Extract 
-> Load CSV using PySpark  -> Convert into Spark DataFrame  
### Transform 
-> Handle missing values  -> Convert data types  -> Clean and prepare data  
### Load 
-> Store processed data into MongoDB  -> Structured for NoSQL usage  
---

## 🧪 Data Validation  
Using Great Expectations:- Unique ID validation  - Numeric range validation  - Valid categorical values  - Data type validation  - Null value check  - Business rule validation  - Column existence check  All validations return **success = true**

--- 

## ⏱️ Workflow Orchestration  
Using Apache Airflow with 3 main tasks:- Extract  - Transform  - Load  Schedule:- Every Saturday  - 09:10 – 09:30  - Interval 10 minutes  

---

## 🛠️ Tech Stack  - Python  - PySpark  - Apache Airflow  - MongoDB  - Great Expectations  ---

## 📁 Project Structure  
P2-M3/
├── data_raw.csv
├── extract.py
├── transform.py
├── load.py
├── DAG.py
├── notebook.ipynb
├── validation.ipynb
├── screenshot_mongo.jpg
└── README.md

---

## 💡 Key Learnings  
- Data pipeline automation improves efficiency  
- Data validation ensures reliability  
- PySpark enables scalable processing  
- Airflow simplifies scheduling and monitoring  
--- 

## 🚀 Conclusion  
This project demonstrates how to build an automated data pipeline that ensures data quality and readiness for analysis, supporting better decision-making.

--- 

## 👤 Author :  Mark Siallagan  