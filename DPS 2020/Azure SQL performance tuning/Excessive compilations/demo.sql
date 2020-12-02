/*
This script executes a demo to show high CPU utilization and poor performance due to compilation of every single-row insert due to varying parameter data type lengths.
The demo then shows a major improvement from using fixed parameter data type lengths that match column lengths, and an additional improvement from using a single transaction for all inserts.

Execute one statement at a time unless noted otherwise.
*/

-- Start XE
ALTER EVENT SESSION insert_statements_trace ON DATABASE STATE = START;

-- Clear plan cache
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;

-- Start initial run using varying parameter data type lengths. Execute next two lines together to run the procedure 5000 times.
EXEC dbo.spInsertRow;
GO 5000

-- Once completed, look at recent resource usage for user workload group
-- Observe high number of query optimizations (optimization is a part of compilation) during the time when workload ran
SELECT snapshot_time, duration_ms, delta_request_count, delta_cpu_usage_ms, delta_query_optimizations
FROM sys.dm_resource_governor_workload_groups_history_ex
WHERE name like 'UserPrimaryGroup.DBId%'
ORDER BY snapshot_time DESC;

-- In SSMS (Extended Events, Sessions, insert_statements_trace, package0.ring_buffer), open the XML data for the event in ring buffer.
-- Observe the captured sp_executesql INSERT statement and pay attention to parameter lengths. Note that they are set to match data length
-- instead of column length, which is an anti-pattern.

-- In the Grafana dashboard, or using sys.dm_os_memory_clerks, observe a spike in plan cache memory (SQL Plans/CACHESTORE_SQLCP memory clerk).
-- Also observe high WRITELOG and HADR_SYNC_COMMIT waits.

-- Clear plan cache
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;

-- Restart XE
ALTER EVENT SESSION insert_statements_trace ON DATABASE STATE = STOP;
ALTER EVENT SESSION insert_statements_trace ON DATABASE STATE = START;

-- Second run. This uses fixed length parameter data types that match column lengths.
-- Execute next two lines together to run the procedure 5000 times.
EXEC dbo.spInsertRow 0;
GO 5000

-- Observe much faster execution and much lower plan cache memory usage.
-- In the insert_statements_trace, observe that parameter data type lengths are all the same (30).

-- Stop XE
ALTER EVENT SESSION insert_statements_trace ON DATABASE STATE = STOP;

-- Third run. This additionally executes all inserts in a transaction to reduce WRITELOG and HADR_SYNC_COMMIT waits.
BEGIN TRANSACTION;

-- Execute next two lines together to run the procedure 5000 times.
EXEC dbo.spInsertRow 0;
GO 5000

COMMIT TRANSACTION;

-- Observe a much shorter execution and no WRITELOG and HADR_SYNC_COMMIT waits.

-- Find all recently executed queries in Query Store, using the Top Resource Consuming Queries report in SSMS.
-- Use CPU Time as the metric, and adjust time interval to the time when the workload ran. Increase the number of queries from the default 25.
-- Note that there is only one query_id/plan_id for the query with fixed parameter data type lengths,
-- and 5000 distinct query_id/plan_id for the initial queries with varying parameter data type lengths.
