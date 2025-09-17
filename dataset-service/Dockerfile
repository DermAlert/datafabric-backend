FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install system dependencies 
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install poetry
RUN pip install poetry

# Configure poetry
RUN poetry config virtualenvs.create false

# Install basic dependencies
RUN pip install fastapi uvicorn sqlalchemy asyncpg httpx pandas minio

# Install additional Spark dependencies
RUN pip install pyspark==4.0.0 delta-spark==4.0.0

# Copy service code
COPY . .

# Set Python path to include shared modules (mounted via docker-compose)
ENV PYTHONPATH="/shared/project:$PYTHONPATH"

EXPOSE 8006

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8006", "--workers", "1"]
