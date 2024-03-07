INGESTION
---------------------

 * Introduction
 * Generic Flow
 * Ingestion Write Strategy
 * Module Description
 * 1C Hash Sum Mechanism
 * Configuration
 * Possible Enhancements
 * Troubleshooting

INTRODUCTION
------------
This module is responsible for _ingesting_ data from APIs into
DPS Data Warehouse.

Despite its name, this module is not only in charge of executing
ingestion functions, but also a little modification at ingestion
step (such as converting dates, dropping unnecessary columns, renaming
columns, etc.)

GENERIC FLOW
------------
There are three main steps:
1. Extract data from the source system
2. Enrich data
3. Store data

INGESTION WRITE STRATEGY
------------------------
By default, before writing data in actual database tables, the data is being written 
into temporary created tables. If there are several partitions - the data is still
being accumulated in the temporal tables and once all partition where fetched - 
the data is being copied from the temporal to actual tables.

This gives us the ability to cancel ingestion anytime if any error occurred, preventing
inconsistent writes. The data is being copied at the end of the ingestion job.

MODULE DESCRIPTION
------------------
* `authorization` - responsible for providing convenient way to retrieve authorization tokens
* `data_sources` -  define vendor specific datasources with their own fetch behaviour
  Provides factory to build specific datasource by configuring it with environment variables
* `date_range_builders` - defines builders and helper classes for _incremental_ data
* `query_params_builders` - defines vendor-specific query parameters builder to be used when extracting data from
  the source systems
* `enrichers` - provides different enriches that can be configured with environment variables 
* `exec` - provides runtime execution mapping-like containers to be used per single partition ingestion
* `pipeline` - declarative framework for building pipeline flow
* `hash_sum_utils` - contain hash sum manipulation related classes (used in conjunction with 1C (vendor) hashsum mechanism)

1C HASH SUM MECHANISM
---------------------
Our data vendor (1C) gives us the ability to capture the hashsum of the fetched batch of data
and store it in our metainformation tables. 1C also gives us the ability to check whether
hash sum of the batch of data has changed or not for the provided request parameters.

This gives us an ability to conveniently re-ingest our data that might be changed.
We can use `datamanager.py` to turn the hashsum reprocess behaviour - before ingesting 
next incremental partition, we check hashsum of already fetched batches of data against 
new hash sum, provided by special API.



CONFIGURATION 
-------------
For available configuration parameters see `commons/configurations` module. 

POSSIBLE ENHANCEMENTS
---------------
N/A

TROUBLESHOOTING
---------------
N/A

