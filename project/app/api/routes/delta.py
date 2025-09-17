from fastapi import APIRouter
from app.api.routes import delta

import pyspark
from delta import *

s3a_endpoint = "http://localhost:9000"
s3a_access_key = "EmbXErTVc1m5sQ1zMtzH"
s3a_secret_key = "pvDz556mJs8a7zFtvP0ATNB9tVnbJNoFXshwCRaP"

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", s3a_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", s3a_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", s3a_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.delta.enableFastS3AListFrom", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \

packages = [
    "org.apache.hadoop:hadoop-aws:3.4.0",
    "io.delta:delta-spark_2.13:4.0.0",
]
spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()

# Your DataFrame operations would follow here
# df.write.format("delta").mode("overwrite").save("s3a://isic-delta/")


router = APIRouter()


@router.get("/test")
async def test_delta():
    """
    Test Delta Lake functionality.
    """
    # Create a simple DataFrame
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "id"])

    # Write to Delta table
    df.write.format("delta").mode("overwrite").save("/tmp/delta_table")

    # Read from Delta table
    delta_df = spark.read.format("delta").load("/tmp/delta_table")
    
    return delta_df.collect()

@router.get("/version")
async def get_delta_version():
    """
    Get the Delta Lake version.
    """
    return {"delta_version": spark.conf.get("spark.sql.extensions")}

@router.get("/spark_version")
async def get_spark_version():
    """
    Get the Spark version.
    """
    return {"spark_version": spark.version}

# create example on minio
@router.get("/create_example_minio")
async def create_delta_on_minio():

    # # Exemplo de gravação no bucket MinIO
    # data = [("Carol", 3), ("Dan", 4)]
    # df = spark.createDataFrame(data, ["name", "id"])
    # df.write.format("delta").mode("overwrite").save("s3a://isic-delta/")

    # # Ler do Delta
    # delta_df = spark.read.format("delta").load("s3a://isic-delta/")
    # return [row.asDict() for row in delta_df.collect()]

    path = "s3a://isic-delta/images_metadata"
    df = spark.read.format("delta").load(path)
    df_male = df.filter(df.clinical_diagnosis_4 == "Basal cell carcinoma, Infiltrating").limit(10)
    return [row.asDict() for row in df_male.collect()]

#general minio read delta bucket, input bucket path
#general minio read delta bucket, input bucket path
@router.get("/read_delta_minio/{path:path}")
async def read_delta_minio(path: str):
    """
    Read Delta table from MinIO bucket.
    """
    print(f"Reading Delta table from path: s3a://{path}")
    try:
        delta_df = spark.read.format("delta").load(f"s3a://{path}")
        return [row.asDict() for row in delta_df.collect()]
    except Exception as e:
        return {"error": str(e)}
    