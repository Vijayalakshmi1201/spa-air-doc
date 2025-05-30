FROM apache/airflow:2.10.0-python3.10
 
# Switch to root user to install system dependencies
USER root
 
# Install OpenJDK 17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
 
# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
 
# Switch back to the airflow user
USER airflow
 
# Install the Apache Spark provider and pin PySpark version
RUN pip install apache-airflow-providers-apache-spark==4.8.2 pyspark==3.5.0