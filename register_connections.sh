#!/bin/bash

set -e

API_URL="http://localhost:8004/api"

echo "📡 Criando tipos de conexão..."

# Delta Lake
curl -s -X POST "$API_URL/connection/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "deltalake",
  "description": "Delta Lake connection for reading/writing Delta tables on S3-compatible storage",
  "icon": "delta",
  "color_hex": "#FF6B35",
  "connection_params_schema": {
    "type": "object",
    "properties": {
      "s3a_endpoint": {
        "type": "string",
        "title": "S3A Endpoint",
        "description": "S3-compatible storage endpoint (e.g., http://localhost:9000)",
        "default": "http://localhost:9000"
      },
      "s3a_access_key": {
        "type": "string",
        "title": "Access Key",
        "description": "S3 access key for authentication"
      },
      "s3a_secret_key": {
        "type": "string",
        "title": "Secret Key",
        "description": "S3 secret key for authentication",
        "format": "password"
      },
      "app_name": {
        "type": "string",
        "title": "Application Name",
        "description": "Spark application name",
        "default": "DeltaLakeConnector"
      },
      "bucket_name": {
        "type": "string",
        "title": "Delta Path",
        "description": "Base path for Delta tables (e.g., bucket-name)",
        "default": "isic-delta"
      }
    },
    "required": ["s3a_endpoint", "s3a_access_key", "s3a_secret_key"],
    "additionalProperties": false
  },
  "metadata_extraction_method": "direct_spark"
}'

# PostgreSQL
curl -s -X POST "$API_URL/connection/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "postgresql",
  "description": "postgresql",
  "icon": "string",
  "color_hex": "#123123",
  "connection_params_schema": {
    "host": {
      "type": "string",
      "description": "Endereço do servidor PostgreSQL"
    },
    "port": {
      "type": "integer",
      "description": "Porta do servidor PostgreSQL",
      "default": 5432
    },
    "database": {
      "type": "string",
      "description": "Nome do banco de dados"
    },
    "username": {
      "type": "string",
      "description": "Usuário para autenticação"
    },
    "password": {
      "type": "string",
      "description": "Senha para autenticação"
    },
    "sslmode": {
      "type": "string",
      "description": "Modo SSL (disable, require, verify-ca, verify-full)",
      "default": "prefer"
    }
  },
  "metadata_extraction_method": "direct_query"
}'

# MinIO para imagens
curl -s -X POST "$API_URL/connection/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "minio",
  "description": "MinIO S3-compatible object storage for images",
  "icon": "minio",
  "color_hex": "#C72E29",
  "connection_params_schema": {
    "type": "object",
    "properties": {
      "endpoint": {
        "type": "string",
        "title": "MinIO Endpoint",
        "description": "MinIO server endpoint (e.g., http://localhost:9000)",
        "default": "http://localhost:9000"
      },
      "access_key": {
        "type": "string",
        "title": "Access Key",
        "description": "MinIO access key for authentication"
      },
      "secret_key": {
        "type": "string",
        "title": "Secret Key",
        "description": "MinIO secret key for authentication",
        "format": "password"
      },
      "bucket_name": {
        "type": "string",
        "title": "Bucket Name",
        "description": "Default bucket name for image storage",
        "default": "images"
      },
      "region": {
        "type": "string",
        "title": "Region",
        "description": "MinIO region (usually us-east-1 for local MinIO)",
        "default": "us-east-1"
      },
      "secure": {
        "type": "boolean",
        "title": "Use HTTPS",
        "description": "Whether to use HTTPS for connections",
        "default": false
      }
    },
    "required": ["endpoint", "access_key", "secret_key", "bucket_name"],
    "additionalProperties": false
  },
  "metadata_extraction_method": "none"
}'

echo "✅ Tipos de conexão registrados!"

echo "🔌 Criando conexões específicas..."

# Conexão PostgreSQL
curl -s -X POST "$API_URL/data-connections/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "dermalert App",
  "description": "dermalert App",
  "connection_type_id": 2,
  "content_type": "metadata",
  "connection_params": {
    "host": "db",
    "port": 5432,
    "database": "dermalert",
    "username": "postgres",
    "password": "postgres",
    "sslmode": "disable"
  },
  "cron_expression": "string",
  "sync_settings": {},
  "organization_id": 1
}'

# Conexão Delta Lake
curl -s -X POST "$API_URL/data-connections/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "Isic-archive delta",
  "description": "Isic-archive delta",
  "connection_type_id": 1,
  "content_type": "metadata",
  "connection_params": {
    "s3a_endpoint": "localhost:9000",
    "s3a_access_key": "EmbXErTVc1m5sQ1zMtzH",
    "s3a_secret_key": "pvDz556mJs8a7zFtvP0ATNB9tVnbJNoFXshwCRaP",
    "bucket_name": "isic-delta",
    "app_name": "DeltaLakeTest"
  },
  "cron_expression": "string",
  "sync_settings": {},
  "organization_id": 1
}'

# Conexão de imagens (MinIO)
curl -s -X POST "$API_URL/data-connections/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "ISIC Images Storage",
  "description": "MinIO storage for ISIC dataset images",
  "connection_type_id": 3,
  "content_type": "image",
  "connection_params": {
    "endpoint": "http://localhost:9000",
    "access_key": "EmbXErTVc1m5sQ1zMtzH",
    "secret_key": "pvDz556mJs8a7zFtvP0ATNB9tVnbJNoFXshwCRaP",
    "bucket_name": "isic-images",
    "region": "us-east-1",
    "secure": false
  },
  "organization_id": 1
}'

curl -s -X POST "$API_URL/data-connections/" -H 'accept: application/json' -H 'Content-Type: application/json' -d '{
  "name": "Dermalert Images Storage",
  "description": "Dermalert",
  "connection_type_id": 3,
  "content_type": "image",
  "connection_params": {
    "endpoint": "http://localhost:9000",
    "access_key": "EmbXErTVc1m5sQ1zMtzH",
    "secret_key": "pvDz556mJs8a7zFtvP0ATNB9tVnbJNoFXshwCRaP",
    "bucket_name": "dermalert-backend",
    "region": "us-east-1",
    "secure": false
  },
  "organization_id": 1
}'

echo "✅ Conexões criadas com sucesso!"
