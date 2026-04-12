import json
import os
from datetime import datetime, timezone

import psycopg2
import requests


API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip()
CITY_NAME = os.getenv("CITY_NAME")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.weather_api (
                id BIGSERIAL PRIMARY KEY,
                city_name TEXT NOT NULL,
                country_code TEXT,
                api_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP NOT NULL,
                temperature_c NUMERIC,
                feels_like_c NUMERIC,
                temp_min_c NUMERIC,
                temp_max_c NUMERIC,
                pressure_hpa INTEGER,
                humidity_pct INTEGER,
                wind_speed_ms NUMERIC,
                weather_main TEXT,
                weather_description TEXT,
                clouds_pct INTEGER,
                sunrise_ts TIMESTAMP,
                sunset_ts TIMESTAMP,
                raw_payload JSONB
            );
            """
        )
    conn.commit()


def fetch_weather(city_name: str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": API_KEY,
        "units": "metric",
        "lang": "es",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def to_utc_timestamp(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(tzinfo=None)


def insert_weather(conn, payload: dict):
    weather0 = payload["weather"][0] if payload.get("weather") else {}

    api_ts = to_utc_timestamp(payload.get("dt"))
    sunrise_ts = to_utc_timestamp(payload.get("sys", {}).get("sunrise"))
    sunset_ts = to_utc_timestamp(payload.get("sys", {}).get("sunset"))
    ingestion_ts = datetime.utcnow()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.weather_api (
                city_name,
                country_code,
                api_timestamp,
                ingestion_timestamp,
                temperature_c,
                feels_like_c,
                temp_min_c,
                temp_max_c,
                pressure_hpa,
                humidity_pct,
                wind_speed_ms,
                weather_main,
                weather_description,
                clouds_pct,
                sunrise_ts,
                sunset_ts,
                raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                payload.get("name"),
                payload.get("sys", {}).get("country"),
                api_ts,
                ingestion_ts,
                payload.get("main", {}).get("temp"),
                payload.get("main", {}).get("feels_like"),
                payload.get("main", {}).get("temp_min"),
                payload.get("main", {}).get("temp_max"),
                payload.get("main", {}).get("pressure"),
                payload.get("main", {}).get("humidity"),
                payload.get("wind", {}).get("speed"),
                weather0.get("main"),
                weather0.get("description"),
                payload.get("clouds", {}).get("all"),
                sunrise_ts,
                sunset_ts,
                json.dumps(payload),
            ),
        )
    conn.commit()


def main():
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY is not defined")
    if not CITY_NAME:
        raise ValueError("CITY_NAME is not defined")

    payload = fetch_weather(CITY_NAME)

    conn = get_connection()
    try:
        create_table(conn)
        insert_weather(conn, payload)
        print(f"data ingested correctly for {CITY_NAME}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()