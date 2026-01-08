FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH" 

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN pip install poetry

# Configure poetry
RUN poetry config virtualenvs.create false

# Install basic dependencies first
RUN pip install fastapi uvicorn sqlalchemy asyncpg httpx pydantic pyspark==4.0.0 delta-spark==4.0.0 aiotrino>=0.3.0 minio==7.2.15

# Copy service code
COPY . .

# Set Python path to include shared modules (mounted via docker-compose)
ENV PYTHONPATH="/shared/project:$PYTHONPATH"

EXPOSE 8007

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8007", "--workers", "1"]
