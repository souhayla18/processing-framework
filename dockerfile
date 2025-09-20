
FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*
# Set environment variables for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


WORKDIR /app


COPY . /app


RUN pip install --no-cache-dir -r requirements.txt

