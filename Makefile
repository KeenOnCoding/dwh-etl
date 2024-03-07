.PHONY: init lint test
.PHONY: test-ingestion test-data-lag
.PHONY: test-daily-processor-unit test-daily-processor-integration
.PHONY: test-historical-processor-unit test-daily-processor-integration
.PHONY: test-sql-processor
.PHONY: test-integration test-unit
.PHONY: test-all

TEST_ROOT_FOLDER := ./test
COVERAGERC := ./coveragerc

INCLUDE_HTML ?= false
ifeq (${INCLUDE_HTML}, true)
	COVERAGE_HTML_REPORT=--cov-report=html
endif

test-commons:
	echo "----------------- RUNNING COMMON UNIT TESTS -----------------";\
	export PYTHONPATH="${PYTHONPATH}:./commons";\
 	python3 -m pytest  --cov=commons --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet  ${TEST_ROOT_FOLDER}/commons/;\
 	TEST_EXIT_CODE=$$?;\
 	exit $${TEST_EXIT_CODE}; \

test-ingestion: ;\
 	echo "----------------- RUNNING INGESTION UNIT TESTS -----------------";\
	export PYTHONPATH="${PYTHONPATH}:./ingestion:./commons";\
 	python3 -m pytest  --cov=ingestion --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet  ${TEST_ROOT_FOLDER}/ingestion/;\
 	TEST_EXIT_CODE=$$?;\
 	exit $${TEST_EXIT_CODE}; \

test-data-lag:
	echo "----------------- RUNNING DATA LAG UNIT TESTS -----------------";\
	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./utility/data-lag:./commons";\
 	python3 -m pytest --cov=utility/data-lag --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet ${TEST_ROOT_FOLDER}/utility/data-lag ;\
 	TEST_EXIT_CODE=$$?;\
 	exit $${TEST_EXIT_CODE}; \

test-daily-processor-unit:
	echo "----------------- RUNNING DAILY PROCESSOR UNIT TESTS -----------------";\
   	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./processor/daily_processor:./commons";\
 	python3 -m pytest --cov=processor/daily_processor --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet ${TEST_ROOT_FOLDER}/processor/daily_processor;\
 	TEST_EXIT_CODE=$$?;\
	exit $${TEST_EXIT_CODE};\

test-daily-processor-integration: ;\
 	echo "----------------- RUNNING DAILY PROCESSOR INTEGRATION TESTS -----------------";\
 	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./processor/daily_processor:./commons";\
 	# sleep should be set to let database successfully initialize;\
    # otherwise you'll get connection error;\
	# docker-compose -f ${TEST_ROOT_FOLDER}/processor/daily_processor/docker-compose.yaml up -d && sleep 30 ;\
	behave ${TEST_ROOT_FOLDER}/processor/daily_processor/features/;\
	TEST_EXIT_CODE=$$?;\
	exit $${TEST_EXIT_CODE};\

# TODO pytest returns status code 5 when no test found
# don't forget to include it in test-unit when unit test will be introduced
test-historical-processor-unit:
	echo "----------------- RUNNING HISTORICAL PROCESSOR UNIT TESTS -----------------";\
   	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./processor/historical_processor:./commons";\
 	python3 -m pytest --cov=processor/historical_processor --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet ${TEST_ROOT_FOLDER}/processor/historical_processor;\
 	TEST_EXIT_CODE=$$?;\
	exit $${TEST_EXIT_CODE};\


test-historical-processor-integration: ;\
 	echo "----------------- RUNNING HISTORICAL PROCESSOR INTEGRATION TESTS -----------------";\
 	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./processor/historical_processor:./commons";\
	behave --tags=~@skip ${TEST_ROOT_FOLDER}/processor/historical_processor/features/;\
	TEST_EXIT_CODE=$$?;\
    exit $${TEST_EXIT_CODE};\

test-sql-processor:
	echo "----------------- RUNNING SQL PROCESSOR UNIT TESTS -----------------";\
	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./processor/sql_processor:./commons";\
 	python3 -m pytest --cov=processor/sql_processor --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet ${TEST_ROOT_FOLDER}/processor/sql_processor;\
	TEST_EXIT_CODE=$$?;\
    exit $${TEST_EXIT_CODE};\

test-work-experience-processor:
	echo "----------------- RUNNING WORK-EXPERIENCE PROCESSOR UNIT TESTS -----------------";\
	export PYTHONPATH="${DEFAULT_PYTHONPATH}:./processor/work_experience_processor:./commons";\
	python3 -m pytest --cov=processor/work_experience_processor --cov-config=${COVERAGERC} ${COVERAGE_HTML_REPORT} --quiet ${TEST_ROOT_FOLDER}/processor/work_experience_processor;\
	TEST_EXIT_CODE=$$?;\
    exit $${TEST_EXIT_CODE};\


test-integration: test-daily-processor-integration test-historical-processor-integration
test-unit: test-commons test-ingestion test-data-lag test-sql-processor test-daily-processor-unit test-work-experience-processor

test-all: test-unit test-integration

lint:
	python -m pylint --rcfile=./pylintrc ./commons ./ingestion ./processor ./utility ./test/**

