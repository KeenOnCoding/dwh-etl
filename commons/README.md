COMMONS
-------------

 * Introduction
 * Configuration Module
 * Possible Enhancements
 * Troubleshooting

INTRODUCTION
------------
This package is a servant for other packages in the project such as
`ingestion` or `processor`. The package contains common 
utilities and Python modules that are used by the dependent modules.

This package is not intended to be run on its own.

CONFIGURATIONS MODULE
--------------------
This module is responsible for configuring job parameters.
For configuration setup we use nested json files structured according to use nested model.
There are 3 type of configs: **environment**, **job** and **utility**.

Each config type expected from a specific file:
- environment: `"/config/common_configuration.json"`
- job: `"/config/job_configuration.json"`
- utility: `"/config/utility_configuration.json"`

To use configuration, import Configurator, then create object and call _get_configurator_ method
```python
from commons.configurations.configurator import Configurator
config = Configurator().get_configurator()
```
In this stage _get_configurator_ reads configuration values from available files
and returns object with initialize relevant nested object and them fields.

Now you can use configuration values.

**Environment** configs are commons and need for all job, consequently use in pair with job/utility config, and have 3 config groups:
   - shared
   - ingestion
   - processing

To get access to some parameter inside you need call it by next way:
```python
config.environment.ingestion.failed_job_metric
```
**Job** configs have 2 base stage: `ingestion` and `processing`, which have own nested configuration groups.

_Ingestion_ has next groups: 

  - general
  - auth
  - fetching_strategy
  - enrichers
  - data_manager
  - data_source

To get access to some parameter inside you need call it by next way:
```python
config.job.ingestion.auth.auth_strategy
```
_Processing_ has configs for all processors that we have now:
  - sql_processor
  - daily_processor
  - history_processor
  - spreadsheets_processor
  - work_experience_processor
  
To get access to some parameter inside you need call it by next way:
```python
config.job.processing.sql_processor.min_batches_to_process
```
**Utility** have several nested groups by kind of utility:
   - data_lag
   - dimension_cleanup
   - count_check

To get access to some parameter inside you need call it by next way:
```python
config.utility.dimension_cleanup.retention_time
```
If we need some of them you should describe only them in configuration json file.
The presence of unused configurations groups - not required.

POSSIBLE ENHANCEMENTS
---------------------

TROUBLESHOOTING
---------------
Because this package modules used in other packages, you should be careful when 
modifying function/classes.

The most common problem is reworking some functionality used in one package, thereby breaking its behavior in another.