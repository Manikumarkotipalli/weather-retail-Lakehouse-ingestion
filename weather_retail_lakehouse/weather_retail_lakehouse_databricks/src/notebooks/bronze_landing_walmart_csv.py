
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F



from pyspark.sql.types import StructType, StructField, StringType, BooleanType

schema = StructType([
    StructField("source_file", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("active", BooleanType(), False),
])

seed_data = [
    ("features.csv", "workspace.bronze.features", True),
    ("stores.csv",   "workspace.bronze.stores",   True),
    ("test.csv",     "workspace.bronze.test",     True),
    ("train.csv",    "workspace.bronze.train",    True),
]

spark.createDataFrame(seed_data, schema) \
     .write.mode("overwrite") \
     .saveAsTable("workspace.bronze.ingestion_config")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bronze_landing")

# ---- Parameters (falls back to defaults if not passed as job params) ----
dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/landing_walmart/archive/")
dbutils.widgets.text("config_table", "workspace.bronze.ingestion_config")
dbutils.widgets.text("max_workers", "4")

source_path = dbutils.widgets.get("source_path")
config_table = dbutils.widgets.get("config_table")
max_workers = int(dbutils.widgets.get("max_workers"))


# ---- Core functions ----

def read_source(path: str, file_name: str):
    return (
        spark.read
             .option("header", True)
             .option("inferSchema", True)
             .csv(f"{path}/{file_name}")
    )


def add_ingestion_metadata(df, file_name: str):
    return (
        df.withColumn("_source_file", F.lit(file_name))
          .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze(df, target_table: str):
    (
        df.write
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(target_table)
    )


def process_source(item: dict) -> dict:
    file_name = item["source_file"]
    target_table = item["target_table"]
    try:
        df = read_source(source_path, file_name)
        df = add_ingestion_metadata(df, file_name)
        row_count = df.count()
        write_bronze(df, target_table)
        logger.info(f"SUCCESS: {file_name} -> {target_table} ({row_count} rows)")
        return {"file": file_name, "table": target_table, "status": "success", "rows": row_count}
    except Exception as e:
        logger.error(f"FAILED: {file_name} -> {target_table}: {e}")
        return {"file": file_name, "table": target_table, "status": "failed", "error": str(e)}


# ---- Driver ----

def main():
    config_df = spark.table(config_table).filter("active = true")
    config = [row.asDict() for row in config_df.collect()]

    if not config:
        logger.warning("No active entries found in config table. Exiting.")
        return

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_source, item): item for item in config}
        for future in as_completed(futures):
            results.append(future.result())

    failures = [r for r in results if r["status"] == "failed"]

    logger.info(f"Run complete: {len(results) - len(failures)} succeeded, {len(failures)} failed")

    if failures:
        failed_files = [f["file"] for f in failures]
        raise RuntimeError(f"Ingestion failed for: {failed_files}")


main()