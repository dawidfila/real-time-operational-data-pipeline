-- AZURE SYNAPSE ANALYTICS 

-- Create Master Key 
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<<Password>>';

-- Create Database Scoped Credential using Managed Identity
CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';

-- Define External Data Source pointing to your storage account
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://gold@<YOUR_STORAGE_ACCOUNT>.dfs.core.windows.net/',
    CREDENTIAL = storage_credential
);

-- Define Parquet File Format
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);

-- DIMENSION TABLES

-- Dimension 1: Process Dimension (SCD Type 2)
CREATE EXTERNAL TABLE dbo.dim_process (
    surrogate_key NVARCHAR(50),
    process_id NVARCHAR(50),
    process_stage NVARCHAR(50),
    priority NVARCHAR(20),
    effective_from DATETIME2,
    effective_to DATETIME2,
    is_current BIT
)
WITH (
    LOCATION = 'dim_process/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

-- Dimension 2: Region Dimension
CREATE EXTERNAL TABLE dbo.dim_region (
    surrogate_key NVARCHAR(50),
    region NVARCHAR(50)
)
WITH (
    LOCATION = 'dim_region/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

-- Dimension 3: Source System Dimension
CREATE EXTERNAL TABLE dbo.dim_source_system (
    surrogate_key NVARCHAR(50),
    source_system NVARCHAR(100)
)
WITH (
    LOCATION = 'dim_source_system/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

-- FACT TABLE

CREATE EXTERNAL TABLE dbo.fact_operational_events (
    fact_id NVARCHAR(50),
    process_sk NVARCHAR(50),
    region_sk NVARCHAR(50),
    source_sk NVARCHAR(50),
    event_status NVARCHAR(50),
    event_start_time DATETIME2,
    event_end_time DATETIME2,
    event_date DATE,
    processing_time_sec INT,
    is_success INT,
    is_long_running INT,
    event_ingestion_time DATETIME2
)
WITH (
    LOCATION = 'fact_operational_events/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

-- VERIFICATION QUERIES

-- Check dimension data
SELECT COUNT(*) as total_processes FROM dbo.dim_process;
SELECT COUNT(*) as total_regions FROM dbo.dim_region;
SELECT COUNT(*) as total_sources FROM dbo.dim_source_system;

-- Check fact data
SELECT COUNT(*) as total_events FROM dbo.fact_operational_events;

-- ANALYTICAL QUERIES - EXAMPLES

-- Query 1: Current active processes
SELECT 
    p.process_id,
    p.process_stage,
    p.priority,
    p.effective_from
FROM dbo.dim_process p
WHERE p.is_current = 1
ORDER BY p.priority DESC;

-- Query 2: Events summary by region and status
SELECT 
    r.region,
    f.event_status,
    COUNT(*) as event_count,
    AVG(f.processing_time_sec) as avg_processing_time,
    SUM(f.is_success) as successful_events,
    SUM(f.is_long_running) as long_running_events
FROM dbo.fact_operational_events f
JOIN dbo.dim_region r ON f.region_sk = r.surrogate_key
GROUP BY r.region, f.event_status
ORDER BY r.region, f.event_status;

-- Query 3: Process performance by source system
SELECT 
    s.source_system,
    p.process_stage,
    COUNT(f.fact_id) as total_events,
    AVG(f.processing_time_sec) as avg_time_sec,
    CAST(SUM(f.is_success) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 as success_rate_pct
FROM dbo.fact_operational_events f
JOIN dbo.dim_source_system s ON f.source_sk = s.surrogate_key
JOIN dbo.dim_process p ON f.process_sk = p.surrogate_key
WHERE p.is_current = 1
GROUP BY s.source_system, p.process_stage
ORDER BY s.source_system, avg_time_sec DESC;

-- Query 4: Daily trends
SELECT 
    f.event_date,
    COUNT(*) as total_events,
    SUM(f.is_success) as successful_events,
    SUM(f.is_long_running) as long_running_events,
    AVG(f.processing_time_sec) as avg_processing_time
FROM dbo.fact_operational_events f
GROUP BY f.event_date
ORDER BY f.event_date DESC;

-- Query 5: Top failing processes
SELECT TOP 10
    p.process_id,
    p.process_stage,
    p.priority,
    COUNT(*) as failure_count,
    AVG(f.processing_time_sec) as avg_processing_time
FROM dbo.fact_operational_events f
JOIN dbo.dim_process p ON f.process_sk = p.surrogate_key
WHERE f.event_status = 'Failed' 
  AND p.is_current = 1
GROUP BY p.process_id, p.process_stage, p.priority
ORDER BY failure_count DESC;