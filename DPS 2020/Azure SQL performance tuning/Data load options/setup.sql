/*
This script sets up a demo to show differences in the duration of loading transient (staged) data in Azure SQL DB when using the following targets:
1. Heap table in a user database
2. Clustered columnstore index table in a user database
3. Heap table in tempdb, implemented as a global temporary table
4. A non-durable memory-optimized table in a Business Critical database

Steps:
1. Create a storage account.
2. Upload the four data file chunks and the BCP format file to a container named "bcp" in the storage account.
3. Install RML Utilities (https://support.microsoft.com/en-us/help/944837/description-of-the-replay-markup-language-rml-utilities-for-sql-server).
4. Execute the script below as noted.
5. Open RML Utilities command prompt, execute Load-all.cmd. 
6. Observe differences in load times and differences in resource consumption, specifically log rate and CPU utilization for each database.
*/

-- Create four test databases on the same logical server
-- Adjust service objectives as desired
CREATE DATABASE DataLoadHeap (SERVICE_OBJECTIVE = 'GP_Gen5_4');
CREATE DATABASE DataLoadCCI (SERVICE_OBJECTIVE = 'GP_Gen5_4');
CREATE DATABASE DataLoadHeapInTempdb (SERVICE_OBJECTIVE = 'GP_Gen5_4');
CREATE DATABASE DataLoadMO (SERVICE_OBJECTIVE = 'BC_Gen5_4');

-- Replace placeholders as noted and execute the remainder of the script in each test database
CREATE USER bulkloader WITH PASSWORD = 'strong-password-here';
GRANT INSERT, DELETE, EXECUTE, ADMINISTER DATABASE BULK OPERATIONS TO bulkloader;

CREATE MASTER KEY;

-- Replace "SAS-token-here" with the SAS token that allows reads from the container with four data file chunks
CREATE DATABASE SCOPED CREDENTIAL bcp_cred
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'SAS-token-here';

-- Replace "storage-account-name-here" with the name of your storage account
CREATE EXTERNAL DATA SOURCE bcp
WITH (TYPE = BLOB_STORAGE, LOCATION = 'https://storage-account-name-here.blob.core.windows.net', CREDENTIAL = bcp_cred);
GO

CREATE SCHEMA stage AUTHORIZATION dbo;
GO

CREATE SCHEMA report AUTHORIZATION dbo;
GO

-- Heap staging table
CREATE TABLE stage.ResourceStatsHeap
(
ElasticPoolID int NOT NULL,
StartDateTime datetime2(2) NOT NULL,
EndDateTime datetime2(2) NOT NULL,
AvgCPUPercent decimal(5,2) NOT NULL,
AvgDataIOPercent decimal(5,2) NOT NULL,
AvgLogWritePercent decimal(5,2) NOT NULL,
AvgDTUPercent decimal(5,2) NOT NULL,
AvgStoragePercent decimal(5,2) NOT NULL,
MaxWorkerPercent decimal(5,2) NOT NULL,
MaxSessionPercent decimal(5,2) NOT NULL,
CollectionDateTime datetime2(2) NOT NULL
);

-- Clustered columnstore staging table
CREATE TABLE stage.ResourceStatsCCI
(
ElasticPoolID int NOT NULL,
StartDateTime datetime2(2) NOT NULL,
EndDateTime datetime2(2) NOT NULL,
AvgCPUPercent decimal(5,2) NOT NULL,
AvgDataIOPercent decimal(5,2) NOT NULL,
AvgLogWritePercent decimal(5,2) NOT NULL,
AvgDTUPercent decimal(5,2) NOT NULL,
AvgStoragePercent decimal(5,2) NOT NULL,
MaxWorkerPercent decimal(5,2) NOT NULL,
MaxSessionPercent decimal(5,2) NOT NULL,
CollectionDateTime datetime2(2) NOT NULL,
INDEX IX_ElasticPoolResourceStats1 CLUSTERED COLUMNSTORE
);

-- Heap staging table in tempdb

-- Execute as server admin
-- Required for BULK INSERT but not for bulk load APIs (BCP, SqlBulkCopy, SqlServerBulkCopy)
EXEC tempdb.sys.sp_executesql @statement = N'GRANT ADMINISTER DATABASE BULK OPERATIONS TO public;';

-- Keep global temp tables until explicitly dropped, or until database engine restarts
ALTER DATABASE SCOPED CONFIGURATION SET GLOBAL_TEMPORARY_TABLE_AUTO_DROP = OFF;

CREATE TABLE ##ResourceStatsHeap
(
ElasticPoolID int NOT NULL,
StartDateTime datetime2(2) NOT NULL,
EndDateTime datetime2(2) NOT NULL,
AvgCPUPercent decimal(5,2) NOT NULL,
AvgDataIOPercent decimal(5,2) NOT NULL,
AvgLogWritePercent decimal(5,2) NOT NULL,
AvgDTUPercent decimal(5,2) NOT NULL,
AvgStoragePercent decimal(5,2) NOT NULL,
MaxWorkerPercent decimal(5,2) NOT NULL,
MaxSessionPercent decimal(5,2) NOT NULL,
CollectionDateTime datetime2(2) NOT NULL
);

-- Memory-optimized non-durable staging table
IF DATABASEPROPERTYEX(DB_NAME(), 'Edition') IN ('Premium','BusinessCritical')
EXEC('
     CREATE TABLE stage.ResourceStatsMO
     (
     ElasticPoolID int NOT NULL,
     StartDateTime datetime2(2) NOT NULL,
     EndDateTime datetime2(2) NOT NULL,
     AvgCPUPercent decimal(5,2) NOT NULL,
     AvgDataIOPercent decimal(5,2) NOT NULL,
     AvgLogWritePercent decimal(5,2) NOT NULL,
     AvgDTUPercent decimal(5,2) NOT NULL,
     AvgStoragePercent decimal(5,2) NOT NULL,
     MaxWorkerPercent decimal(5,2) NOT NULL,
     MaxSessionPercent decimal(5,2) NOT NULL,
     CollectionDateTime datetime2(2) NOT NULL,
     CONSTRAINT PK_ResourceStatsMO PRIMARY KEY NONCLUSTERED HASH (ElasticPoolID, StartDateTime) WITH (BUCKET_COUNT = 10000000)
     )
     WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);
     ');

-- Aggregated summary table
CREATE TABLE report.ResourceStatsSummary
(
ElasticPoolID int NOT NULL,
MinStartDateTime datetime2(2) NOT NULL,
MaxEndDateTime datetime2(2) NOT NULL,
MaxCPUPercent decimal(5,2) NOT NULL,
MaxDataIOPercent decimal(5,2) NOT NULL,
MaxLogWritePercent decimal(5,2) NOT NULL,
MaxDTUPercent decimal(5,2) NOT NULL,
MaxStoragePercent decimal(5,2) NOT NULL,
MaxWorkerPercent decimal(5,2) NOT NULL,
MaxSessionPercent decimal(5,2) NOT NULL,
CONSTRAINT PK_ResourceStatsSummary PRIMARY KEY (ElasticPoolID)
);
GO

CREATE SEQUENCE stage.seqChunkNumber
AS tinyint
START WITH 0
INCREMENT BY 1;
GO

CREATE OR ALTER PROCEDURE stage.spLoadHeap
AS
DECLARE @ChunkNumber tinyint;

-- Pick a chunk to load
SELECT @ChunkNumber = (NEXT VALUE FOR stage.seqChunkNumber) % 4 + 1;

-- Note the presence of the TABLOCK hint to enable concurrent data loads into a heap

IF @ChunkNumber = 1
    BULK INSERT stage.ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk1.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 2
    BULK INSERT stage.ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk2.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 3
    BULK INSERT stage.ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk3.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 4
    BULK INSERT stage.ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk4.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');
GO

CREATE OR ALTER PROCEDURE stage.spLoadCCI
AS
DECLARE @ChunkNumber tinyint;

-- Pick a chunk to load
SELECT @ChunkNumber = (NEXT VALUE FOR stage.seqChunkNumber) % 4 + 1;

-- Note the absence of the TABLOCK hint to enable concurrent data loads into clustered columnstore index

IF @ChunkNumber = 1
    BULK INSERT stage.ResourceStatsCCI
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk1.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 2
    BULK INSERT stage.ResourceStatsCCI
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk2.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 3
    BULK INSERT stage.ResourceStatsCCI
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk3.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 4
    BULK INSERT stage.ResourceStatsCCI
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk4.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');
GO

CREATE OR ALTER PROCEDURE stage.spLoadHeapInTempdb
AS
DECLARE @ChunkNumber tinyint;

-- Pick a chunk to load
SELECT @ChunkNumber = (NEXT VALUE FOR stage.seqChunkNumber) % 4 + 1;

-- Note the presence of the TABLOCK hint to enable concurrent data loads into a heap

IF @ChunkNumber = 1
    BULK INSERT ##ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk1.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 2
    BULK INSERT ##ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk2.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 3
    BULK INSERT ##ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk3.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 4
    BULK INSERT ##ResourceStatsHeap
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk4.bcp'
    WITH (TABLOCK, DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');
GO

CREATE OR ALTER PROCEDURE stage.spLoadMO
AS
DECLARE @ChunkNumber tinyint;

-- Pick a chunk to load
SELECT @ChunkNumber = (NEXT VALUE FOR stage.seqChunkNumber) % 4 + 1;

IF @ChunkNumber = 1
    BULK INSERT stage.ResourceStatsMO
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk1.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 2
    BULK INSERT stage.ResourceStatsMO
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk2.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 3
    BULK INSERT stage.ResourceStatsMO
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk3.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');

IF @ChunkNumber = 4
    BULK INSERT stage.ResourceStatsMO
    FROM 'bcp/ElasticPoolResourceStats10mil_chunk4.bcp'
    WITH (DATA_SOURCE = 'bcp', FORMATFILE='bcp/ElasticPoolResourceStats10mil_chunked.fmt', FORMATFILE_DATA_SOURCE = 'bcp');
GO

-- Optionally, load the summary table using staged data
-- This executes very quickly compared to loads into staging tables
TRUNCATE TABLE report.ResourceStatsSummary;

INSERT INTO report.ResourceStatsSummary
(
ElasticPoolID,
MinStartDateTime,
MaxEndDateTime,
MaxCPUPercent,
MaxDataIOPercent,
MaxLogWritePercent,
MaxDTUPercent,
MaxStoragePercent,
MaxWorkerPercent,
MaxSessionPercent
)
SELECT ElasticPoolID,
       MIN(StartDateTime) AS MinStartDateTime,
       MAX(EndDateTime) AS MaxEndDateTime,
       MAX(AvgCPUPercent) AS MaxCPUPercent,
       MAX(AvgDataIOPercent) AS MaxDataIOPercent,
       MAX(AvgLogWritePercent) AS MaxLogWritePercent,
       MAX(AvgDTUPercent) AS MaxDTUPercent,
       MAX(AvgStoragePercent) AS MaxStoragePercent,
       MAX(MaxWorkerPercent) AS MaxWorkerPercent,
       MAX(MaxSessionPercent) AS MaxSessionPercent
FROM stage.ResourceStatsCCI
GROUP BY ElasticPoolID;
