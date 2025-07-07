# azure-data-engineering-project
End-to-end Azure data pipeline using Data Factory, Data Lake, Databricks, and Synapse

---

## 🔄 Data Flow Description

### 🔹 1. Ingestion (Bronze Layer)
- Azure Data Factory ingests raw CSV files from a public HTTP source (e.g., GitHub)  
- Files are stored in the **Bronze container** in Azure Data Lake

### 🔹 2. Transformation (Silver Layer)
- Azure Databricks reads the raw files from Bronze
- Cleans data: removes nulls, fixes formats, calculates new columns
- Writes structured data to the **Silver container** in ADLS

### 🔹 3. Aggregation & Analytics (Gold Layer)
- Azure Synapse Analytics creates **external tables** on Silver Parquet files
- SQL queries perform aggregation and summarization
- Final outputs are saved to the **Gold container** in ADLS
- External tables are created on Gold data too

### 🔹 4. Visualization (Power BI)
- Power BI connects to Synapse **via Serverless SQL Endpoint**
- Builds interactive dashboards using **DirectQuery** or **Import Mode**
- Displays KPIs like total sales, trends, and top products

---

## 📊 Example Power BI Dashboards

- Sales by Region  
- Monthly Revenue Trends  
- Top Performing Products  
- Profit Margin by Category

(Screenshots or `.pbix` file can be added)

---

## 📁 Sample SQL Query (Synapse External Table)

```sql
Create Schema gold

CREATE VIEW gold.sales
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_Sales/',
            FORMAT = 'PARQUET'
        ) AS query1
)

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales;
