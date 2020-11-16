/*
This script sets up a demo to show high CPU utilization and poor performance due to compilation of every single-row insert, 
which happens because of varying parameter data type lengths.

Use a Premium, Business Critical, or Hyperscale Azure SQL database.
*/

-- Create a table with many varchar columns
CREATE TABLE dbo.Wide
(
WideId int NOT NULL IDENTITY(1,1),
Col0 varchar(30) NOT NULL,
Col1 varchar(30) NOT NULL,
Col2 varchar(30) NOT NULL,
Col3 varchar(30) NOT NULL,
Col4 varchar(30) NOT NULL,
Col5 varchar(30) NOT NULL,
Col6 varchar(30) NOT NULL,
Col7 varchar(30) NOT NULL,
Col8 varchar(30) NOT NULL,
Col9 varchar(30) NOT NULL,
Col10 varchar(30) NOT NULL,
Col11 varchar(30) NOT NULL,
Col12 varchar(30) NOT NULL,
Col13 varchar(30) NOT NULL,
Col14 varchar(30) NOT NULL,
Col15 varchar(30) NOT NULL,
Col16 varchar(30) NOT NULL,
Col17 varchar(30) NOT NULL,
Col18 varchar(30) NOT NULL,
Col19 varchar(30) NOT NULL,
Col20 varchar(30) NOT NULL,
Col21 varchar(30) NOT NULL,
Col22 varchar(30) NOT NULL,
Col23 varchar(30) NOT NULL,
Col24 varchar(30) NOT NULL,
Col25 varchar(30) NOT NULL,
Col26 varchar(30) NOT NULL,
Col27 varchar(30) NOT NULL,
Col28 varchar(30) NOT NULL,
Col29 varchar(30) NOT NULL,
Col30 varchar(30) NOT NULL,
Col31 varchar(30) NOT NULL,
Col32 varchar(30) NOT NULL,
Col33 varchar(30) NOT NULL,
Col34 varchar(30) NOT NULL,
Col35 varchar(30) NOT NULL,
Col36 varchar(30) NOT NULL,
Col37 varchar(30) NOT NULL,
Col38 varchar(30) NOT NULL,
Col39 varchar(30) NOT NULL,
Col40 varchar(30) NOT NULL,
Col41 varchar(30) NOT NULL,
Col42 varchar(30) NOT NULL,
Col43 varchar(30) NOT NULL,
Col44 varchar(30) NOT NULL,
Col45 varchar(30) NOT NULL,
Col46 varchar(30) NOT NULL,
Col47 varchar(30) NOT NULL,
Col48 varchar(30) NOT NULL,
Col49 varchar(30) NOT NULL,
CONSTRAINT pkWide PRIMARY KEY (WideId)
);
GO

-- Create a helper natively compiled procedure to generate sp_executesql parameters with either fixed or variable varchar parameter length
-- This takes advantage of efficient execution of procedural code in a natively compiled module
CREATE OR ALTER PROCEDURE dbo.spGetInsertParameters
    @RandomParameterLength bit,
    @ParameterDeclarationString nvarchar(4000) = '' OUTPUT,
    @ParameterString nvarchar(4000) = '' OUTPUT
WITH NATIVE_COMPILATION, SCHEMABINDING
AS

BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL=SNAPSHOT, LANGUAGE=N'English');

DECLARE @i int = 0,
        @ParameterLength smallint,
        @DataLength smallint,
        @ColumnData varchar(30),
        @FullDataString varchar(30) = 'DPS-DPS-DPS-DPS-DPS-DPS-DPS-DP';

WHILE @i < 50 -- For each column in dbo.Wide table
BEGIN
    SELECT @DataLength = CAST(1 + 29. * RAND() AS smallint);

    IF @RandomParameterLength = 1
        SELECT @ParameterLength = @DataLength
    ELSE
        SELECT @ParameterLength = 30;

    SELECT @ColumnData = SUBSTRING(@FullDataString, 1, @DataLength);

    SELECT @ParameterDeclarationString = CONCAT_WS(',', @ParameterDeclarationString, CONCAT_WS('', '@Col', CAST(@i AS varchar(2)), ' varchar(', CAST(@ParameterLength AS varchar(2)), ')')),
           @ParameterString = CONCAT_WS(',', @ParameterString, CONCAT_WS('', '@Col', CAST(@i AS varchar(2)), '=''', @ColumnData, ''''));

    SELECT @i += 1;
END;

END;
GO

-- Create a procedure to insert a row using sp_executesql
CREATE OR ALTER PROCEDURE dbo.spInsertRow
    @RandomParameterLength bit = 1
AS
DECLARE @ParameterDeclarationString nvarchar(4000),
        @ParameterString nvarchar(4000),
        @SQLStmt nvarchar(max);

SET NOCOUNT ON;

-- Get sp_executesql parameters
EXEC dbo.spGetInsertParameters @RandomParameterLength, 
                               @ParameterDeclarationString OUTPUT, 
                               @ParameterString OUTPUT;

-- Build the statement to execute sp_executesql
SELECT @SQLStmt = 
CONCAT
(
'EXEC sys.sp_executesql @stmt = N''INSERT INTO dbo.Wide (Col0,Col1,Col2,Col3,Col4,Col5,Col6,Col7,Col8,Col9,Col10,Col11,Col12,Col13,Col14,Col15,Col16,Col17,Col18,Col19,Col20,Col21,Col22,Col23,Col24,Col25,Col26,Col27,Col28,Col29,Col30,Col31,Col32,Col33,Col34,Col35,Col36,Col37,Col38,Col39,Col40,Col41,Col42,Col43,Col44,Col45,Col46,Col47,Col48,Col49)
VALUES (@Col0,@Col1,@Col2,@Col3,@Col4,@Col5,@Col6,@Col7,@Col8,@Col9,@Col10,@Col11,@Col12,@Col13,@Col14,@Col15,@Col16,@Col17,@Col18,@Col19,@Col20,@Col21,@Col22,@Col23,@Col24,@Col25,@Col26,@Col27,@Col28,@Col29,@Col30,@Col31,@Col32,@Col33,@Col34,@Col35,@Col36,@Col37,@Col38,@Col39,@Col40,@Col41,@Col42,@Col43,@Col44,@Col45,@Col46,@Col47,@Col48,@Col49)'',
@params = N''', @ParameterDeclarationString, ''',', @ParameterString
);

-- Insert a row in dbo.Wide
EXEC sys.sp_executesql @SQLStmt;
GO

-- Set up Query Store: clear old queries, make sure all queries are captured, use short interval
ALTER DATABASE CURRENT SET QUERY_STORE CLEAR;
ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE, INTERVAL_LENGTH_MINUTES = 1, QUERY_CAPTURE_MODE = ALL);

-- Create XE session to trace inserts
CREATE EVENT SESSION insert_statements_trace ON DATABASE 
ADD EVENT sqlserver.sp_statement_starting
(
SET collect_statement=1 
WHERE sqlserver.like_i_sql_unicode_string(statement,N'EXEC sys.sp_executesql @stmt = N''INSERT INTO dbo.Wide%')
)
ADD TARGET package0.ring_buffer (SET max_events_limit=1) -- only one event is needed
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF);
