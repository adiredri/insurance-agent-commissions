FROM python:3.11-slim-bookworm
# Pinned to bookworm (Debian 12) rather than the floating "3.11-slim" tag —
# once that tag rolls to trixie (Debian 13), openjdk-17-jre-headless is gone
# in favor of openjdk-21, which is untested against this project's Spark version.

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
