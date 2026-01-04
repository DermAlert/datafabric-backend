FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYSPARK_HADOOP_VERSION 3
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

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

# Install poetry
RUN pip install poetry

# Configure poetry
RUN poetry config virtualenvs.create false

# Install basic dependencies
RUN pip install fastapi uvicorn sqlalchemy asyncpg httpx pandas minio

# Install additional Spark dependencies
RUN pip install pyspark==4.0.0 delta-spark==4.0.0 aiotrino>=0.3.0

# Copy service code
COPY . .

# Set Python path to include shared modules (mounted via docker-compose)
ENV PYTHONPATH="/shared/project:$PYTHONPATH"

EXPOSE 8006

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8006", "--workers", "1"]
