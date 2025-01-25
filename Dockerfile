FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set AWS region environment variables
ENV AWS_DEFAULT_REGION=il-central-1
ENV AWS_REGION=il-central-1

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src_scripts/ /opt/airflow/dags/src_scripts/
COPY data/ /opt/airflow/dags/data/
COPY orchestration-airflow.py /opt/airflow/dags/orchestration-airflow.py
