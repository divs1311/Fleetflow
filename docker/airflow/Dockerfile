# Airflow 2.10.5 + the PorterFlow pipeline: Kafka client, DuckDB, PyArrow, PySpark (needs a JRE),
# and dbt in its own virtualenv so its dependencies can never conflict with Airflow's.
FROM apache/airflow:2.10.5-python3.11

USER root
# Slim Debian images ship without man dirs; the JRE's post-install step needs man1 to exist.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY --chown=airflow:root docker/airflow/constraints-2.10.5-3.11.txt /tmp/constraints.txt
COPY --chown=airflow:root requirements/pipeline.txt /tmp/pipeline.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/pipeline.txt --constraint /tmp/constraints.txt

COPY --chown=airflow:root requirements/dbt.lock.txt /tmp/dbt.lock.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt.lock.txt

ENV PYTHONPATH=/opt/porterflow/src \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_EXECUTABLE=/home/airflow/dbt-venv/bin/dbt \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
