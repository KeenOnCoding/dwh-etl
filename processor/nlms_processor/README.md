NLMS EXTENDED GRADE REPORT
---------------------

Description
------------
The NLMS processor receives data via the API on a daily basis


Project files
-------------
* processor/nlms_processor/nlms_report_generator.py
* processor/nlms_processor/stage_table_processor.py
* processor/nlms_processor/main.py
* processor/nlms_processor/start.py
* jenkins/containers/nlms-extended-grade-report.xml
* jenkins/lib/resources/etl-configs/nlms-reports.json
* jenkins/lib/vars/runNLMSProcessor.groovy
* jenkins/pipelines/nlms-reports-pipeline.groovy
* run-scripts/nlms-processor/start.sh
* configs/sql-processor-configs/production/nlms_extended_grade_report.yaml

Configuration
-------------
Available configuration parameters:

* **nlms_report_target_table** – name of the stage table in the database where received data.

* **trigger_url** – url used to trigger report generation.

* **check_status_url** - url used to check report generation status and get task_id.

* **on_prem_report_path** – the path on the local machine to ready-made reports.

* **http_report_url** - url, in which we substitute the name of the report in order to take data from it.

* **header_key** - access token key

* **plan_id** - name of the stage table for sql_processor