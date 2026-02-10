
﻿# Project Overview
 This project demonstrates a real-time data engineering pipeline for healthcare, designed to analyze patient flow across hospital departments using Azure cloud services.
The pipeline ingests streaming data, processes it in Databricks (PySpark), and stores it in Azure Synapse SQL Pool for analytics and visualization.

 Build the real-time ingestion + transformation pipeline.

## Architecture
![Architecture Diagram](healthcare_architecture.png)

## 🎯 Objectives

- Ingest real-time patient data using Azure Event Hubs

- Process and transform data in Databricks using Bronze–Silver–Gold layers

- Build a Star Schema in Azure Synapse Analytics for analytics

- Enable version control and collaboration with Git

 ## 🛠️ Tools & Technologies

- Azure Event Hub – Real-time data ingestion
- Azure Databricks – PySpark-based ETL processing
- Azure Data Lake Storage – Staging raw and curated data
- Azure Synapse SQL Pool – Data warehouse for analytics
- Power BI – Dashboarding (future step)
- Python 3.9+ – Core programming
- Git – Version control

## 📐 Data Architecture

- The pipeline follows a multi-layered architecture:

- Bronze Layer: Raw JSON data from Event Hub stored in ADLS.
- Silver Layer: Cleaned and structured data (validated - types, null handling).
- Gold Layer: Aggregated and transformed data ready for BI consumption.


 ## ⭐ Star Schema Design

- The Gold layer data in Synapse follows a star schema for optimized analytics:
- Fact Table: FactPatientFlow (patient visits, timestamps, wait times, discharge)
- Dimension Tables:
- DimDepartment – Department details
- DimPatient – Patient demographic info
- DimTime – Date and time dimension

## ⚙️ Step-by-Step Implementation
### 1. Event Hub Setup
  - Created Event Hub namespace and patient-flow hub.
  - Configured consumer groups for Databricks streaming.
### 2. Data Simulation
  - Developed Python script patient_flow_generator.py to     stream fake patient data (departments, wait time,        discharge status) to Event Hub.
    
 👉[Producer Code](simulator/patient_flow_generator.py)
  
### 3. Storage Setup
- Configured Azure Data Lake Storage (ADLS Gen2).
- Created containers for bronze, silver, and gold layers.
  
### 4. Databricks Processing
 - [Notebook 1](databricks_notebooks/01eventhub_to_bronze.py): Reads Event Hub stream into Bronze.
-  [Notebook 2](databricks_notebooks/02_silver_cleandata.py): Cleans and validates schema.
-  [Notebook 3](databricks_notebooks/03_dim_department.py): Builds the Department dimension table for the Gold layer.
-  [Notebook 4](databricks_notebooks/03_gold_dim_patients.py):Creates the Patient dimension table for the Gold layer.
-  [Notebook 5](databricks_notebooks/03_gold_fact.py):Generates fact tables with business metrics for analytical querying.
  
### 5. Synapse SQL Pool
  - Created dedicated SQL Pool.
  - Executed schema and fact/dimension creation queries      from:
       - [DDL Qureis](sql_pool_queries/sql_pool_queries.sql)
