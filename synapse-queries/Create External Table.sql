CREATE DATABASE SCOPED CREDENTIAL credAbhi
WITH 
    IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE source_silver 
WITH
(
    LOCATION = 'https://abhiawstoragedl.blob.core.windows.net/silver',
    CREDENTIAL = credAbhi
)

CREATE EXTERNAL DATA SOURCE source_gold 
WITH
(
    LOCATION = 'https://abhiawstoragedl.blob.core.windows.net/gold',
    CREDENTIAL = credAbhi
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


-- CREATE EXTERNAL TABLE

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales

SELECT * FROM gold.extsales

CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION = 'extproducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.products

CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION = 'extcustomers',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.customers

CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION = 'extcalendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.calendar

CREATE EXTERNAL TABLE gold.extreturns
WITH
(
    LOCATION = 'extreturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.returns

CREATE EXTERNAL TABLE gold.extterittories
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.territories

CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
    LOCATION = 'extsubcat',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.subcat



