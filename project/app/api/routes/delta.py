from fastapi import APIRouter
from app.services.infrastructure.spark_manager import SparkManager

router = APIRouter()


def _get_spark():
    """Lazy Spark session — uses the shared SparkManager (local or standalone)."""
    return SparkManager().get_or_create_session()


@router.get("/test")
async def test_delta():
    """Test Delta Lake functionality."""
    spark = _get_spark()
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "id"])
    df.write.format("delta").mode("overwrite").save("/tmp/delta_table")
    delta_df = spark.read.format("delta").load("/tmp/delta_table")
    return delta_df.collect()


@router.get("/version")
async def get_delta_version():
    """Get the Delta Lake version."""
    spark = _get_spark()
    return {"delta_version": spark.conf.get("spark.sql.extensions")}


@router.get("/spark_version")
async def get_spark_version():
    """Get the Spark version."""
    spark = _get_spark()
    return {"spark_version": spark.version}


@router.get("/create_example_minio")
async def create_delta_on_minio():
    spark = _get_spark()
    path = "s3a://isic-delta/images_metadata"
    df = spark.read.format("delta").load(path)
    df_male = df.filter(
        df.clinical_diagnosis_4 == "Basal cell carcinoma, Infiltrating"
    ).limit(10)
    return [row.asDict() for row in df_male.collect()]


@router.get("/read_delta_minio/{path:path}")
async def read_delta_minio(path: str, limit: int = 100, offset: int = 0):
    """Read Delta table from MinIO bucket."""
    limit = min(limit, 10000)
    spark = _get_spark()
    try:
        delta_df = spark.read.format("delta").load(f"s3a://{path}")
        total_count = delta_df.count()
        if offset > 0:
            from pyspark.sql.functions import monotonically_increasing_id
            delta_df = delta_df.withColumn("_row_num", monotonically_increasing_id())
            delta_df = delta_df.filter(delta_df._row_num >= offset).drop("_row_num")
        rows = [row.asDict() for row in delta_df.limit(limit).collect()]
        return {
            "total_count": total_count,
            "returned_count": len(rows),
            "limit": limit,
            "offset": offset,
            "data": rows,
        }
    except Exception as e:
        return {"error": str(e)}
