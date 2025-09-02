# Base: Python 3.11 with Java 11
FROM openjdk:11-jre-slim AS base

# Install Python 3.11 and system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-venv python3-pip curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Workdir
WORKDIR /app

# Copy and install Python deps
COPY requirements.txt ./
RUN python3.11 -m pip install --upgrade pip \
 && pip install -r requirements.txt

# Copy code
COPY scripts/ ./scripts/

# Default bucket can be overridden via --env BUCKET=...
ENV BUCKET="morningflow"

# Run the job (override args at runtime if desired)
# Example: docker run --rm -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
#          -e AWS_REGION=us-west-2 -e BUCKET=morningflow image:tag
ENTRYPOINT ["python3.11", "scripts/flight_delays_s3.py"]
CMD ["--bucket", "${BUCKET}"]
