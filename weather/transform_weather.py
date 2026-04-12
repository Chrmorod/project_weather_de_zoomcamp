import os
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


PG_HOST = get_required_env("PG_HOST")
PG_PORT = get_required_env("PG_PORT")
PG_DB = get_required_env("PG_DB")
PG_USER = get_required_env("PG_USER")
PG_PASSWORD = get_required_env("PG_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPERTIES: Dict[str, str] = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

SOURCE_TABLE = "raw.weather_api"
TARGET_TABLE = "int_weather_spark"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("weather-transform")
        .getOrCreate()
    )


def read_raw_weather(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table=SOURCE_TABLE,
            properties=JDBC_PROPERTIES,
        )
    )


def transform_weather(raw_df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy(
        "city_name",
        "api_timestamp"
    ).orderBy(col("ingestion_timestamp").desc())

    return (
        raw_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .drop("raw_payload")
        .withColumn("weather_date", to_date(col("api_timestamp")))
        .select(
            "id",
            "city_name",
            "country_code",
            "api_timestamp",
            "ingestion_timestamp",
            "weather_date",
            "temperature_c",
            "feels_like_c",
            "temp_min_c",
            "temp_max_c",
            "pressure_hpa",
            "humidity_pct",
            "wind_speed_ms",
            "weather_main",
            "weather_description",
            "clouds_pct",
            "sunrise_ts",
            "sunset_ts",
        )
    )


def write_intermediate_weather(df: DataFrame) -> None:
    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=JDBC_URL,
            table=TARGET_TABLE,
            properties=JDBC_PROPERTIES,
        )
    )


def main() -> None:
    spark = build_spark_session()

    try:
        print(f"Reading source table: {SOURCE_TABLE}")
        raw_df = read_raw_weather(spark)

        print("Transforming weather data")
        transformed_df = transform_weather(raw_df)

        print(f"Writing target table: {TARGET_TABLE}")
        write_intermediate_weather(transformed_df)

        print("PySpark transformation completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()