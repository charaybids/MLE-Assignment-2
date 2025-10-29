# Use official Apache Airflow 2.10.3 image as base (more stable than 3.x)
FROM apache/airflow:2.10.3-python3.11

# Switch to root to install system dependencies
USER root

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Install Java for PySpark and other utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        bash \
    && rm -rf /var/lib/apt/lists/* && \
    # Ensure bash is default shell
    ln -sf /bin/bash /bin/sh

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Create directories for pipeline data
RUN mkdir -p /opt/airflow/data \
             /opt/airflow/datamart/bronze \
             /opt/airflow/datamart/silver \
             /opt/airflow/datamart/gold \
             /opt/airflow/model_store \
             /opt/airflow/reports \
             /opt/airflow/diagrams && \
    chown -R airflow:root /opt/airflow

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY --chown=airflow:root requirements.txt /opt/airflow/

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Set working directory
WORKDIR /opt/airflow
