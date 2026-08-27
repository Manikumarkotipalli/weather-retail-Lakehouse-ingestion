from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, ArrayType
)
import json

schema = StructType([
    StructField("source_file", StringType(), False),
    StructField("bronze_table", StringType(), False),
    StructField("silver_table", StringType(), False),
    StructField("pk_columns", ArrayType(StringType()), False),
    StructField("schema_json", StringType(), False),  # JSON string: {"col": "type"}
    StructField("active", BooleanType(), False),
])

seed_data = [
    (
        "features.csv", "features_bronze", "features_silver",
        ["Store", "Date"],
        json.dumps({
            "Store": "int", "Date": "date", "Temperature": "double",
            "Fuel_Price": "double", "CPI": "double", "Unemployment": "double",
            "IsHoliday": "boolean",
        }),
        True,
    ),
    (
        "stores.csv", "stores_bronze", "stores_silver",
        ["Store"],
        json.dumps({"Store": "int", "Type": "string", "Size": "int"}),
        True,
    ),
    (
        "train.csv", "train_bronze", "train_silver",
        ["Store", "Dept", "Date"],
        json.dumps({
            "Store": "int", "Dept": "int", "Date": "date",
            "Weekly_Sales": "double", "IsHoliday": "boolean",
        }),
        True,
    ),
    (
        "test.csv", "test_bronze", "test_silver",
        ["Store", "Dept", "Date"],
        json.dumps({
            "Store": "int", "Dept": "int", "Date": "date", "IsHoliday": "boolean",
        }),
        True,
    ),
]

spark.createDataFrame(seed_data, schema) \
     .write.mode("overwrite") \
     .saveAsTable("workspace.bronze.ingestion_config")






#Silver MV code



from pyspark import pipelines as dp
from pyspark.sql import functions as F
import json

SOURCE_PATH = "/Volumes/workspace/bronze/landing_walmart/archive/"

config = spark.table("workspace.bronze.ingestion_config").filter("active = true").collect()


def generate_bronze_table(row):
    @dp.table(
        name=row["bronze_table"],
        comment=f"Bronze streaming ingestion of {row['source_file']}"
    )
    def _bronze():
        return (
            spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "csv")
                 .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/_schemas/{row['bronze_table']}")
                 .option("header", "true")
                 .load(f"{SOURCE_PATH}/{row['source_file']}")
                 .withColumn("_source_file", F.lit(row["source_file"]))
                 .withColumn("_ingested_at", F.current_timestamp())
        )
    return _bronze


def generate_silver_mv(row):
    schema_map = json.loads(row["schema_json"])
    pk_cols = row["pk_columns"]
    pk_condition = " AND ".join(f"{c} IS NOT NULL" for c in pk_cols)

    @dp.table(
        name=row["silver_table"],
        comment=f"Typed, deduplicated silver materialized view for {row['bronze_table']}"
    )
    @dp.expect_or_drop(f"valid_pk_{row['silver_table']}", pk_condition)
    def _silver():
        df = dp.read(row["bronze_table"])
        for col, dtype in schema_map.items():
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(dtype))
        return df.dropDuplicates(pk_cols).drop("_source_file", "_ingested_at")

    return _silver


for row in config:
    generate_bronze_table(row)
    generate_silver_mv(row)