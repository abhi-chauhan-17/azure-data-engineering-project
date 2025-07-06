CREATE VIEW gold.calendar
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
            FORMAT = 'PARQUET'
        ) AS query1
)

CREATE VIEW gold.customers
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_Customers/',
            FORMAT = 'PARQUET'
        ) AS query2
)

CREATE VIEW gold.products
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_Products/',
            FORMAT = 'PARQUET'
        ) AS query3
)

CREATE VIEW gold.returns
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_Returns/',
            FORMAT = 'PARQUET'
        ) AS query1
)

CREATE VIEW gold.territories
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_Territories/',
            FORMAT = 'PARQUET'
        ) AS query1
)

CREATE VIEW gold.subcat
AS
(
    SELECT 
    * 
    FROM 
        OPENROWSET(
            BULK 'https://abhiawstoragedl.dfs.core.windows.net/silver/AdventureWorks_SubCategories/',
            FORMAT = 'PARQUET'
        ) AS query1
)

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


