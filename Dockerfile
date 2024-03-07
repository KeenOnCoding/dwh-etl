FROM openjdk:11
COPY --from=python:3.10-slim / /

ENV TZ=Europe/Kiev
RUN groupadd --gid 2000 docker && \
    useradd --uid 2000 --gid docker --shell /bin/bash docker

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get install -y --reinstall build-essential && \
    apt-get install -y unixodbc-dev python3-dev && \
    pip install --upgrade pip


WORKDIR /dwh
COPY requirements-image.txt requirements-image.txt
RUN pip install -r requirements-image.txt

RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/spark-mssql-connector/1.0.0/spark-mssql-connector-1.0.0.jar && \
    wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/7.2.1.jre11/mssql-jdbc-7.2.1.jre11.jar && \
    mv *.jar $(pip show pyspark | grep Location | cut -c 11-)/pyspark/jars

COPY . /dwh

RUN echo '#!/bin/bash\n . /dwh/run-scripts/ingestion/start.sh' \
    > /usr/bin/general-etl-start && chmod +x /usr/bin/general-etl-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/daily-processor/start.sh' \
    > /usr/bin/daily-processor-start && chmod +x /usr/bin/daily-processor-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/sql-processor/start.sh' \
    > /usr/bin/sql-processor-start && chmod +x /usr/bin/sql-processor-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/data-lag/start.sh' \
    > /usr/bin/data-lag-start && chmod +x /usr/bin/data-lag-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/dimension-cleanup/start.sh' \
    > /usr/bin/dimension-cleanup-start && chmod +x /usr/bin/dimension-cleanup-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/historical-processor/start.sh' \
    > /usr/bin/dim-historical-copy && chmod +x /usr/bin/dim-historical-copy

RUN echo '#!/bin/bash\n . /dwh/run-scripts/errcheck/count-check.sh' \
    > /usr/bin/count-check && chmod +x /usr/bin/count-check

RUN echo '#!/bin/bash\n . /dwh/run-scripts/spreadsheet-processor/start.sh' \
    > /usr/bin/spreadsheet-processor-start && chmod +x /usr/bin/spreadsheet-processor-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/work-experience-processor/start.sh' \
    > /usr/bin/work-experience-processor-start && chmod +x /usr/bin/work-experience-processor-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/nlms-processor/start.sh' \
    > /usr/bin/nlms-processor-start && chmod +x /usr/bin/nlms-processor-start

RUN echo '#!/bin/bash\n . /dwh/run-scripts/riksbank-processor/start.sh' \
    > /usr/bin/riksbank-processor-start && chmod +x /usr/bin/riksbank-processor-start
USER docker

CMD echo 'USAGE: docker run [OPTIONS] IMAGE[:TAG] [COMMAND].\n\
Available commands:\n\
    - general-etl-start\n\
    - daily-processor-start\n\
    - sql-processor-start\n\
    - data-lag-start\n\
    - dimension-cleanup-start\n\
    - dim-historical-copy\n\
    - count-check\n\
    - spreadsheet-processor-start\n\
    - work-experience-processor\n\
\n\
To read more about available commands, their parameters,\
please, reference the DPS project \
documentation or check out sources.\
';\
exit 1