-- =========================================================
-- Synapse Serverless SQL: External Tables over Gold Delta
-- No data duplication — queries read Delta files directly from ADLS Gen2.
-- =========================================================

CREATE EXTERNAL DATA SOURCE GoldLakehouseSource
WITH (
    LOCATION = 'https://<storageaccount>.dfs.core.windows.net/lakehouse/gold/'
);

CREATE EXTERNAL FILE FORMAT DeltaFileFormat
WITH (
    FORMAT_TYPE = DELTA
);

CREATE EXTERNAL TABLE dbo.sales_summary (
    sale_date         DATE,
    region            VARCHAR(64),
    order_count       INT,
    total_revenue     DECIMAL(14,2),
    unique_customers  INT
)
WITH (
    LOCATION = 'sales_summary/',
    DATA_SOURCE = GoldLakehouseSource,
    FILE_FORMAT = DeltaFileFormat
);

-- Example query a Power BI DirectQuery report would run
SELECT
    region,
    SUM(total_revenue) AS revenue_last_30_days
FROM dbo.sales_summary
WHERE sale_date >= DATEADD(day, -30, GETDATE())
GROUP BY region
ORDER BY revenue_last_30_days DESC;
