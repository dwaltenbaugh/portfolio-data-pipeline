FROM apache/airflow:3.3.0

# Airflow's AWS integration, including CloudWatch remote logging.
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==9.35.0" \
    "botocore[crt]"

COPY --chown=airflow:root \
    pyproject.toml \
    /opt/airflow/project/pyproject.toml

COPY --chown=airflow:root \
    src \
    /opt/airflow/project/src

RUN pip install --no-cache-dir /opt/airflow/project
