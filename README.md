# azure-medallion-data-pipeline
Project Overview: End-to-End Data Pipeline using Azure & Databricks
This project implements an end-to-end data ingestion and transformation pipeline following the Medallion Architecture (Bronze → Silver → Gold) using Azure cloud services and Databricks.

**Data Ingestion (Bronze Layer)**

Multiple source files in CSV, JSON, and Parquet formats are ingested into an Azure Data Lake Storage (ADLS Gen2) account.
These files are delivered to the raw (bronze) layer on a weekly schedule (every Sunday at 7:00 AM ET).
The bronze layer acts as a landing zone and preserves the data in its original format for traceability and audit purposes.


**Data Processing & Validation (Silver Layer)**

Data from the bronze layer is processed using Databricks notebooks, where the following steps are applied:
•	Schema validation and standardization
•	Data quality checks and basic validations
•	Addition of metadata columns such as ingestion_date
•	Deduplication and cleansing
•	Conversion to Delta Lake format
A total of 8 raw datasets are processed and stored in the silver layer as clean, structured Delta tables, optimized for downstream consumption.


**Data Aggregation & Analytics (Gold Layer)**

Business-level transformations and aggregations are performed on the silver layer to generate analytics-ready datasets.
From the processed data, 4 curated gold tables are created to support:
•	Business reporting
•	Dashboards
•	Data-driven decision making
The gold layer provides optimized, well-modeled tables for consumption by BI tools and analytics teams.


**Azure Cloud Services & Technologies Used**

**Azure Databricks**

Used as the core ETL and data processing engine to perform large-scale transformations across Bronze, Silver, and Gold layers. Databricks notebooks handle schema validation, data cleansing, enrichment, aggregations, and Delta Lake writes.

**Azure Data Lake Storage Gen2 (ADLS Gen2)**

Serves as the central data lake for storing all layers of the Medallion Architecture:
•	Bronze – raw, immutable source data
•	Silver – cleaned, validated, and structured Delta tables
•	Gold – curated, analytics-ready datasets

**Azure Data Factory (ADF)**

Acts as the orchestration and workflow management layer, responsible for:
•	Scheduling and triggering pipelines
•	Managing dependencies between activities
•	Executing Databricks notebooks
•	Configuring linked services and datasets
•	Handling retries, logging, and monitoring
ADF ensures reliable, automated end-to-end pipeline execution.

Note : Azure Data Factory changes are maintained and published in a separate repository for easier management and version control.
https://github.com/karthiktammana29/azure-data-factory

**Databricks Clusters**

Compute clusters are configured to execute ETL jobs efficiently, with:
•	Auto-scaling and auto-termination enabled
•	Optimized Spark runtime with Delta Lake support
•	Job clusters for cost-efficient, isolated execution


**Azure Service Principal**

Used for secure, programmatic authentication across Azure services.
The service principal enables controlled access between Databricks, Data Factory, and ADLS using role-based access control (RBAC).

**Azure Key Vault**

Provides secure secret management by storing sensitive information such as:
•	Storage account credentials
•	Service principal secrets
•	Access tokens
Key Vault integration eliminates hardcoded credentials and improves security posture.

**Databricks Secret Scopes**

Integrated with Azure Key Vault to securely retrieve secrets at runtime.
Secret scopes allow Databricks notebooks and clusters to authenticate to ADLS Gen2 without exposing credentials in code.








