from pyspark import pipelines as dp
from pyspark.sql import functions as F
import requests
from datetime import datetime, timezone

BASE_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"


@dp.table(
    name="weather_forecast_raw",
    comment="Raw daily forecast snapshots landed via Open-Meteo API"
)
def weather_forecast_raw():
    locations = spark.table("workspace.bronze.weather_locations_config") \
                      .filter("active = true").collect()

    rows = []
    ingested_at = datetime.now(timezone.utc)
    for loc in locations:
        resp = requests.get(BASE_URL, params={
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "daily": DAILY_VARS,
            "forecast_days": 16,
            "timezone": loc["timezone"],
        }, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        rows.append({
            "location_name": loc["location_name"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "daily_dates": payload["daily"]["time"],
            "temp_max": payload["daily"]["temperature_2m_max"],
            "temp_min": payload["daily"]["temperature_2m_min"],
            "precipitation_sum": payload["daily"]["precipitation_sum"],
            "windspeed_max": payload["daily"]["windspeed_10m_max"],
            "_ingested_at": ingested_at,
        })
    return spark.createDataFrame(rows)


@dp.table(
    name="weather_forecast_daily",
    comment="Exploded, typed daily forecast rows"
)
@dp.expect_or_drop("valid_forecast_date", "forecast_date IS NOT NULL")
@dp.expect("reasonable_temp", "temp_max_c BETWEEN -60 AND 60")
def weather_forecast_daily():
    df = dp.read("weather_forecast_raw")
    return (
        df.withColumn(
            "daily_zipped",
            F.arrays_zip("daily_dates", "temp_max", "temp_min", "precipitation_sum", "windspeed_max")
        )
        .withColumn("daily_exploded", F.explode("daily_zipped"))
        .select(
            "location_name", "latitude", "longitude",
            F.col("daily_exploded.daily_dates").cast("date").alias("forecast_date"),
            F.col("daily_exploded.temp_max").cast("double").alias("temp_max_c"),
            F.col("daily_exploded.temp_min").cast("double").alias("temp_min_c"),
            F.col("daily_exploded.precipitation_sum").cast("double").alias("precipitation_mm"),
            F.col("daily_exploded.windspeed_max").cast("double").alias("windspeed_max_kmh"),
            "_ingested_at",
        )
    )