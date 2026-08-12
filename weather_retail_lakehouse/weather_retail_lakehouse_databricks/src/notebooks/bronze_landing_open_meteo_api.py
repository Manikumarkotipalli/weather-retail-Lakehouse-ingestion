from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

schema = StructType([
    StructField("location_name", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("timezone", StringType(), False),
    StructField("active", BooleanType(), False),
])

seed_data = [
    ("bentonville_ar", 36.3729, -94.2088, "America/Chicago", True),
]

spark.createDataFrame(seed_data, schema) \
     .write.mode("overwrite") \
     .saveAsTable("workspace.bronze.weather_locations_config")






import requests
import logging
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bronze_weather")

BASE_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"

target_table = "workspace.bronze.weather_forecast_raw"
config_table = "workspace.bronze.weather_locations_config"


def fetch_forecast(lat: float, lon: float, tz: str) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": DAILY_VARS,
        "forecast_days": 16,
        "timezone": tz,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def process_location(row) -> dict:
    try:
        payload = fetch_forecast(row["latitude"], row["longitude"], row["timezone"])
        return {
            "location_name": row["location_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "raw_response": payload,   # kept as struct/json, parsed in silver
            "status": "success",
        }
    except Exception as e:
        logger.error(f"FAILED for {row['location_name']}: {e}")
        return {
            "location_name": row["location_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "raw_response": None,
            "status": f"failed: {e}",
        }


def main():
    locations = spark.table(config_table).filter("active = true").collect()

    if not locations:
        logger.warning("No active locations configured. Exiting.")
        return

    results = [process_location(row) for row in locations]
    failures = [r for r in results if r["status"] != "success"]
    successes = [r for r in results if r["status"] == "success"]

    if not successes:
        raise RuntimeError(f"All location fetches failed: {failures}")

    ingested_at = datetime.now(timezone.utc)

    rows = [
        {
            "location_name": r["location_name"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "daily_dates": r["raw_response"]["daily"]["time"],
            "temp_max": r["raw_response"]["daily"]["temperature_2m_max"],
            "temp_min": r["raw_response"]["daily"]["temperature_2m_min"],
            "precipitation_sum": r["raw_response"]["daily"]["precipitation_sum"],
            "windspeed_max": r["raw_response"]["daily"]["windspeed_10m_max"],
            "_ingested_at": ingested_at,
        }
        for r in successes
    ]

    df = spark.createDataFrame(rows)

    (
        df.write
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(target_table)
    )

    logger.info(f"Landed {len(successes)} location snapshots. {len(failures)} failed.")
    if failures:
        logger.warning(f"Failures: {failures}")


main()