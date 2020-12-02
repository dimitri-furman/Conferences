@echo off

REM Truncate staging tables

ostress -Sserver-name-here.database.windows.net -Ubulkloader -Ppassword-here -dDataLoadCCI -Q"TRUNCATE TABLE stage.ResourceStatsCCI;"
ostress -Sserver-name-here.database.windows.net -Ubulkloader -Ppassword-here -dDataLoadHeap -Q"TRUNCATE TABLE stage.ResourceStatsHeap;"
ostress -Sserver-name-here.database.windows.net -Ubulkloader -Ppassword-here -dDataLoadHeapInTempdb -Q"TRUNCATE TABLE ##ResourceStatsHeap;"
ostress -Sserver-name-here.database.windows.net -Ubulkloader -Ppassword-here -dDataLoadMO -Q"DELETE stage.ResourceStatsMO;"

REM Start four concurrent loads, each using four sessions to load data

start "Memory-optimized" cmd /c "ostress -Sserver-name-here.database.windows.net -n4 -Ubulkloader -Ppassword-here -dDataLoadMO -Q"EXEC stage.spLoadMO;" -o%temp%\DataLoadMO_output.txt&&pause"
start "Heap" cmd /c "ostress -Sserver-name-here.database.windows.net -n4 -Ubulkloader -Ppassword-here -dDataLoadHeap -Q"EXEC stage.spLoadHeap;"  -o%temp%\DataLoadHeap_output.txt&&pause"
start "Clustered columnstore" cmd /c "ostress -Sserver-name-here.database.windows.net -n4 -Ubulkloader -Ppassword-here -dDataLoadCCI -Q"EXEC stage.spLoadCCI;" -o%temp%\DataLoadCCI_output.txt&&pause"
start "Heap in tempdb" cmd /c "ostress -Sserver-name-here.database.windows.net -n4 -Ubulkloader -Ppassword-here -dDataLoadHeapInTempdb -Q"EXEC stage.spLoadHeapInTempdb;" -o%temp%\DataLoadHeapInTempdb_output.txt&&pause"
