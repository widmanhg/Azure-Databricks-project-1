# 📊 Netflix Data Pipeline with Azure Data Factory & Databricks

A complete data engineering pipeline built on Azure and Databricks, implementing a layered architecture (Raw → Bronze → Silver → Gold), with automation, orchestration, and transformation of Netflix data from GitHub to Delta Lake.

## 🚀 Architecture Overview

**Resource Group:** Deployed in Canada Central

**Storage Account (Data Lake):**
- Containers: `raw/`, `bronze/`, `silver/`, `gold/`, `metastore/`

  ![WhatsApp Image 2025-04-29 at 09 47 38_61e7a36e](https://github.com/user-attachments/assets/74518a65-5b31-4baa-af0c-9a2f8f6cc1cb)


**Azure Data Factory:** Orchestrates data ingestion and staging

**Azure Databricks:** Transforms and processes data with PySpark

**Unity Catalog:** Manages governance and access control

**Databricks Workflows:** Automates multi-step transformations

## 🏗️ Pipeline Stages

### 1. Azure Data Factory – Ingestion Layer
📁 **Source:** CSVs from GitHub  
📍 **Destination:** `raw/` and `bronze/` folders in the Data Lake

**ADF Activities:**
- **Web Activity:** Fetches metadata about the GitHub-hosted CSVs
- **Set Variable:** Stores dataset information dynamically
- **Validation Activity:** Confirms that netflix_titles.csv exists
- **ForEach Activity:** Loops through each dataset (categories, cast, countries, directors)
- **Copy Activity:** Moves raw CSVs to the `bronze/` layer

![image](https://github.com/user-attachments/assets/448f2d20-bca6-4cc8-bc84-b17ea9711b52)


### 2. Azure Databricks – Processing Layer
Created a Unity Catalog metastore and connected via Access Connector.
A PySpark cluster processes the data using seven notebooks:

#### 📒 Notebooks Overview

##### 📘 1_autoloader
- Reads raw CSVs incrementally with Auto Loader
- Uses checkpoints to enable streaming updates
- Writes data continuously to `bronze/` every 10s

##### 📘 2_silver
- Reads from `bronze/` using parameterized paths
- Converts CSV to Delta format
- Writes to `silver/` layer

##### 📘 3_lookupNotebook
- Defines an array of dataset folders as dictionaries
- Stores them with `dbutils.jobs.taskValues.set()`

#### 🔄 Databricks Workflow: Silver Transformation
✅ **Task 1: Lookup_Locations**
- Runs 3_lookupNotebook
- Produces `my_arr`, a list of dataset path pairs

🔁 **Task 2: SilverNotebook (with for_each)**
- Runs 2_silver notebook once per dataset in `my_arr`
- Transforms CSV → Delta and stores in `silver/`

![image](https://github.com/user-attachments/assets/abff377c-ffac-4cbf-a071-e371cd3b037e)

##### 📘 4_silver
Transforms netflix_titles in the silver layer:
- Cleans nulls and inconsistent types
- Adds new columns: ShortTitle, type_flag, duration_ranking
- Creates temporary views
- Saves as Delta format back to `silver/`

##### 📘 5_lookupNotebook
- Receives the current weekday (1=Monday to 7=Sunday)
- Converts it to integer
- Stores it with `dbutils.jobs.taskValues.set('weekoutput', weekday)`

##### 📘 6_false
- Fetches weekoutput using `dbutils.jobs.taskValues.get`
- Stores it in a variable for conditional logic inside the notebook

#### 🧠 Workflow: Silver_Cleaning
##### 🗓️ Conditional Workflow based on Weekday
1. **Task: WeekdayLookup**
   - Executes 5_lookupNotebook
   - Stores current day of week as `weekoutput`

2. **Task: IfWeekDay**
   - Checks if `weekoutput == 7` (Sunday)

3. **Conditional Execution**
   - If TRUE (Sunday) → Run 4_silver for weekly cleanup
   - If FALSE → Run 6_false (no transformation)

![image](https://github.com/user-attachments/assets/8f888526-fc6c-4770-a0bb-94408b8ddc2d)


### 🟡 Notebook 7_gold: Delta Live Tables (DLT)
#### 🔄 From Silver → Gold
Creates validated and clean tables for analytics using Delta Live Tables:

**Source folders (Silver):**
- netflix_directors
- netflix_cast
- netflix_countries
- netflix_category

**Validation rule:**
```python
@dlt.expect_all_or_drop({"not_null_id": "show_id IS NOT NULL"})
```

#### 🧱 Table Pipeline: netflix_titles
- **gold_stg_netflixtitles:** raw read from Silver
- **gold_trns_netflixtitles:** adds newflag = 1
- **gold_netflixtitles:** final table with:
  - newflag IS NOT NULL
  - show_id IS NOT NULL

![image](https://github.com/user-attachments/assets/01a77d1b-e7ce-492f-adfa-383a234e83d3)


![image](https://github.com/user-attachments/assets/48d01a0e-b937-4848-b945-bccea46b2187)


#### 🔧 DLT_GOLD Pipeline Configuration

| Setting | Value |
|--------|-------|
| Pipeline Name | DLT_GOLD |
| Source Notebook | /Netflix_project/7_gold |
| Execution Mode | development: true |
| Cluster Type | 1 worker (Standard_D4s_v3) |
| Execution Trigger | Manual / Scheduled |
| Photon Engine | ❌ Disabled |
| Channel | CURRENT (stable) |
| Destination Catalog | netflix_catalog |
| Schema | dlt_schema |

## 🏁 Final Outcome

🎯 An automated, parameterized and secure data pipeline that:

- Ingests raw data from GitHub
- Orchestrates with ADF
- Transforms with PySpark in Databricks
- Cleans and validates with DLT
- Prepares data for downstream analytics in a gold Delta Lake
