FROM apache/airflow:3.3.0


# ------------------------------------------------------------
# COPY THE PYTHON PROJECT INTO THE IMAGE
# ------------------------------------------------------------

COPY --chown=airflow:root \
    pyproject.toml \
    /opt/airflow/project/pyproject.toml

COPY --chown=airflow:root \
    src \
    /opt/airflow/project/src


# ------------------------------------------------------------
# INSTALL OUR PIPELINE APPLICATION
# ------------------------------------------------------------

RUN pip install --no-cache-dir /opt/airflow/project