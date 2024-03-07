PROCESSOR
---------------------

 * Introduction
 * Daily Processor
 * Historical Processor
 * NLMS Processor
 * SQL Processor  
 * External dependencies
 * Configuration
 * Troubleshooting

INTRODUCTION
------------
This package contains submodules that serves different purposes.
But all of them is doing 'stuff' on the data that is already landed in the
staging layers of the DPS Data Warehouse.

DAILY PROCESSOR
-------------
Daily processor responsible for splitting each record of the time report data evenly,
on a daily basis.

The ETS gives an ability to submit time reports for a period. This means, for instance,
you can have single record with 40-hours reported. This module splits this 40 hours reported
from Monday to Friday to 5 records: Monday-Monday 8 hours, Tuesday-Tuesday 8 hours, and so on.

HISTORICAL PROCESSOR
--------------------

Historical processor helps to keep track of data record changes.
The processor stores all unique records by comparing records hash sum.
>NOTE: this hash sums are not the same as 1C hash sums. The hash sums used here is produced
> by `hash_column_enricher.py`

NLMS PROCESSOR
--------------------

The NLMS processor is responsible for triggering reports initiating through the API, 
extracting data from reports, combining all reports into one Spark dataframe, 
writing data into the stage table in DWH and further decomposition into 
fact and dimensions tables.

SQL PROCESSOR
-------------
SQL processor uses SQL scripts to manipulate data. The common use of SQL processor
is to modify and move data from staging layer to the user layer.


CONFIGURATION
-------------
For available configuration parameters see `commons/configurations` module.

TROUBLESHOOTING
---------------
N/A

