FROM python:3.11-slim

# OpenJDK 17 is required by PySpark; this also sidesteps the winutils.exe pain
# of running Spark natively on Windows — the whole point of using Docker here.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY . .

CMD ["bash"]
