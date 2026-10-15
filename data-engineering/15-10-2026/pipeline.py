"""
Open-Meteo Weather Forecast ETL Pipeline
----------------------------------------
This pipeline extracts weather forecast data from the Open-Meteo API,
cleans and transforms the dataset, applies database migrations,
and loads the data into an SQLite database with full idempotency guarantees.
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from migrate import run_migrations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weather_pipeline")

# Configuration constants
DEFAULT_DB_PATH = Path(__file__).resolve().parent / "weather_data.db"
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Target variables from Open-Meteo Weather Forecast endpoint
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "weather_code",
    "surface_pressure",
    "wind_speed_10m",
]

# Standard default locations to fetch forecasts for
LOCATIONS = [
    {"name": "Baku", "latitude": 40.4093, "longitude": 49.8671},
    {"name": "Berlin", "latitude": 52.5200, "longitude": 13.4050},
]

# WMO Weather interpretation codes (WW) mapping
WMO_WEATHER_CODES: Dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Drizzle: Light",
    53: "Drizzle: Moderate",
    55: "Drizzle: Dense intensity",
    56: "Freezing Drizzle: Light",
    57: "Freezing Drizzle: Dense intensity",
    61: "Rain: Slight",
    63: "Rain: Moderate",
    65: "Rain: Heavy intensity",
    66: "Freezing Rain: Light",
    67: "Freezing Rain: Heavy intensity",
    71: "Snow fall: Slight",
    73: "Snow fall: Moderate",
    75: "Snow fall: Heavy intensity",
    77: "Snow grains",
    80: "Rain showers: Slight",
    81: "Rain showers: Moderate",
    82: "Rain showers: Violent",
    85: "Snow showers: Slight",
    86: "Snow showers: Heavy",
    95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def get_api_client() -> openmeteo_requests.Client:
    """Set up and return the Open-Meteo API client with caching and retries."""
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def extract_weather_data(
    client: openmeteo_requests.Client, location: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract raw forecast data from Open-Meteo API for a specific location.
    """
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": HOURLY_VARIABLES,
    }
    logger.info(
        "Calling Open-Meteo API for %s (lat: %s, lon: %s)",
        location["name"],
        location["latitude"],
        location["longitude"],
    )
    responses = client.weather_api(OPENMETEO_FORECAST_URL, params=params)
    response = responses[0]

    location_metadata = {
        "name": location["name"],
        "latitude": float(response.Latitude()),
        "longitude": float(response.Longitude()),
        "elevation": float(response.Elevation()),
        "timezone": response.Timezone().decode("utf-8") if response.Timezone() else "UTC",
        "utc_offset_seconds": int(response.UtcOffsetSeconds()),
    }

    hourly = response.Hourly()
    timestamps = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    data = {"forecast_time": timestamps}
    for i, var_name in enumerate(HOURLY_VARIABLES):
        data[var_name] = hourly.Variables(i).ValuesAsNumpy()

    df_raw = pd.DataFrame(data=data)
    return {"metadata": location_metadata, "raw_dataframe": df_raw}


def classify_wind(speed_kmh: float) -> str:
    """Classify wind speed into descriptive Beaufort categories."""
    if pd.isna(speed_kmh):
        return "Unknown"
    if speed_kmh < 5.0:
        return "Calm"
    elif speed_kmh < 15.0:
        return "Light Breeze"
    elif speed_kmh < 30.0:
        return "Moderate Breeze"
    elif speed_kmh < 50.0:
        return "Strong Breeze"
    else:
        return "High Wind / Gale"


def clean_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the extracted weather forecast dataframe.
    - Drops invalid timestamps
    - Deduplicates on forecast_time
    - Fills missing numeric values with domain-appropriate fallbacks
    - Bounds physical metrics (non-negative precipitation, humidity in [0, 100])
    """
    df_clean = df.copy()

    # 1. Drop rows missing timestamps
    df_clean = df_clean.dropna(subset=["forecast_time"])

    # 2. Deduplicate records keeping the last entry
    df_clean = df_clean.drop_duplicates(subset=["forecast_time"], keep="last")

    # 3. Numeric bounds & fill missing
    if "precipitation" in df_clean.columns:
        df_clean["precipitation"] = df_clean["precipitation"].fillna(0.0).clip(lower=0.0)
    if "rain" in df_clean.columns:
        df_clean["rain"] = df_clean["rain"].fillna(0.0).clip(lower=0.0)
    if "wind_speed_10m" in df_clean.columns:
        df_clean["wind_speed_10m"] = df_clean["wind_speed_10m"].fillna(0.0).clip(lower=0.0)
    if "relative_humidity_2m" in df_clean.columns:
        df_clean["relative_humidity_2m"] = (
            df_clean["relative_humidity_2m"].ffill().bfill().clip(0.0, 100.0)
        )
    if "temperature_2m" in df_clean.columns:
        df_clean["temperature_2m"] = df_clean["temperature_2m"].ffill().bfill()
    if "apparent_temperature" in df_clean.columns:
        df_clean["apparent_temperature"] = df_clean["apparent_temperature"].ffill().bfill()
    if "surface_pressure" in df_clean.columns:
        df_clean["surface_pressure"] = df_clean["surface_pressure"].ffill().bfill()
    if "weather_code" in df_clean.columns:
        df_clean["weather_code"] = df_clean["weather_code"].fillna(0).astype(int)

    return df_clean


def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply business transformations and feature engineering:
    - Map WMO weather interpretation code to text descriptions
    - Calculate difference between real temperature and apparent temperature
    - Classify wind speed into categorical bands
    - Format datetime to standardized ISO 8601 strings
    - Round numerical columns for consistent persistence
    """
    df_transformed = df.copy()

    # Weather condition description
    df_transformed["weather_condition"] = df_transformed["weather_code"].map(
        lambda code: WMO_WEATHER_CODES.get(int(code), f"Code {code}")
    )

    # Temperature difference (real vs feels-like)
    df_transformed["temp_feels_diff"] = (
        df_transformed["temperature_2m"] - df_transformed["apparent_temperature"]
    ).round(2)

    # Wind category
    df_transformed["wind_category"] = df_transformed["wind_speed_10m"].apply(classify_wind)

    # Format datetime as ISO 8601 string in UTC
    df_transformed["forecast_time"] = df_transformed["forecast_time"].dt.strftime(
        "%Y-%m-%d %H:%M:%S%z"
    )

    # Round floating columns
    float_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "surface_pressure",
        "wind_speed_10m",
    ]
    for col in float_cols:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col].round(2)

    return df_transformed


def upsert_location(conn: sqlite3.Connection, meta: Dict[str, Any]) -> int:
    """
    Idempotently insert or update a location in the database.
    Returns the location's database ID.
    """
    query = """
        INSERT INTO locations (
            name, latitude, longitude, elevation, timezone, utc_offset_seconds, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(name) DO UPDATE SET
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            elevation = excluded.elevation,
            timezone = excluded.timezone,
            utc_offset_seconds = excluded.utc_offset_seconds,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
    """
    cursor = conn.cursor()
    cursor.execute(
        query,
        (
            meta["name"],
            meta["latitude"],
            meta["longitude"],
            meta["elevation"],
            meta["timezone"],
            meta["utc_offset_seconds"],
        ),
    )
    location_id = cursor.fetchone()[0]
    cursor.close()
    return location_id


def upsert_hourly_forecasts(
    conn: sqlite3.Connection, location_id: int, df: pd.DataFrame
) -> int:
    """
    Idempotently upsert hourly forecast records for a location.
    Uses ON CONFLICT(location_id, forecast_time) DO UPDATE to prevent duplicate rows.
    """
    query = """
        INSERT INTO hourly_forecasts (
            location_id,
            forecast_time,
            temperature_2m,
            relative_humidity_2m,
            apparent_temperature,
            precipitation,
            rain,
            weather_code,
            weather_condition,
            surface_pressure,
            wind_speed_10m,
            temp_feels_diff,
            wind_category,
            ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(location_id, forecast_time) DO UPDATE SET
            temperature_2m = excluded.temperature_2m,
            relative_humidity_2m = excluded.relative_humidity_2m,
            apparent_temperature = excluded.apparent_temperature,
            precipitation = excluded.precipitation,
            rain = excluded.rain,
            weather_code = excluded.weather_code,
            weather_condition = excluded.weather_condition,
            surface_pressure = excluded.surface_pressure,
            wind_speed_10m = excluded.wind_speed_10m,
            temp_feels_diff = excluded.temp_feels_diff,
            wind_category = excluded.wind_category,
            ingested_at = CURRENT_TIMESTAMP;
    """
    records = []
    for _, row in df.iterrows():
        records.append(
            (
                location_id,
                row["forecast_time"],
                round(float(row["temperature_2m"]), 2),
                round(float(row["relative_humidity_2m"]), 2),
                round(float(row["apparent_temperature"]), 2),
                round(float(row["precipitation"]), 2),
                round(float(row["rain"]), 2),
                int(row["weather_code"]),
                str(row["weather_condition"]),
                round(float(row["surface_pressure"]), 2),
                round(float(row["wind_speed_10m"]), 2),
                round(float(row["temp_feels_diff"]), 2),
                str(row["wind_category"]),
            )
        )

    cursor = conn.cursor()
    cursor.executemany(query, records)
    affected = cursor.rowcount
    cursor.close()
    return affected


def get_db_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Retrieve database metrics including table row counts and basic statistics."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM locations;")
    location_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM hourly_forecasts;")
    forecast_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT 
            MIN(temperature_2m),
            MAX(temperature_2m),
            ROUND(AVG(temperature_2m), 2),
            ROUND(SUM(precipitation), 2)
        FROM hourly_forecasts;
        """
    )
    t_min, t_max, t_avg, precip_sum = cursor.fetchone()
    cursor.close()

    return {
        "locations_count": location_count,
        "forecasts_count": forecast_count,
        "temp_min": t_min,
        "temp_max": t_max,
        "temp_avg": t_avg,
        "total_precipitation": precip_sum,
    }


def run_pipeline(
    db_path: Path = DEFAULT_DB_PATH, locations: List[Dict[str, Any]] = LOCATIONS
) -> Dict[str, Any]:
    """
    Execute the complete end-to-end weather ETL pipeline.
    Ensures migrations, extracts from Open-Meteo, cleans, transforms, and upserts data.
    """
    logger.info("=" * 60)
    logger.info("Starting Open-Meteo Weather Pipeline")
    logger.info("Target Database: %s", db_path)
    logger.info("=" * 60)

    # Step 1: Ensure database schema migration is applied
    run_migrations(db_path)

    # Connect to database
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    metrics_before = get_db_metrics(conn)
    logger.info(
        "DB State BEFORE: %d locations, %d forecast rows",
        metrics_before["locations_count"],
        metrics_before["forecasts_count"],
    )

    client = get_api_client()
    total_processed = 0

    try:
        for loc in locations:
            # Step 2: Extraction
            extracted = extract_weather_data(client, loc)
            metadata = extracted["metadata"]
            raw_df = extracted["raw_dataframe"]
            logger.info("Extracted %d raw forecast rows for %s", len(raw_df), metadata["name"])

            # Step 3: Cleaning
            clean_df = clean_weather_data(raw_df)
            logger.info("Cleaned dataset: %d rows remain", len(clean_df))

            # Step 4: Transformation
            transformed_df = transform_weather_data(clean_df)
            logger.info(
                "Transformed dataset: features engineered (weather_condition, temp_feels_diff, wind_category)"
            )

            # Step 5: Idempotent Load
            with conn:
                location_id = upsert_location(conn, metadata)
                upserted_rows = upsert_hourly_forecasts(conn, location_id, transformed_df)
                total_processed += upserted_rows
                logger.info(
                    "Upserted %d rows for location %s (id=%d)",
                    len(transformed_df),
                    metadata["name"],
                    location_id,
                )

        metrics_after = get_db_metrics(conn)
        logger.info(
            "DB State AFTER: %d locations, %d forecast rows (avg temp: %s°C, total precip: %s mm)",
            metrics_after["locations_count"],
            metrics_after["forecasts_count"],
            metrics_after["temp_avg"],
            metrics_after["total_precipitation"],
        )
        logger.info("=" * 60)
        logger.info("Pipeline execution completed successfully.")
        logger.info("=" * 60)

        return {
            "metrics_before": metrics_before,
            "metrics_after": metrics_after,
            "total_processed": total_processed,
        }

    finally:
        conn.close()


def verify_idempotence(db_path: Path = DEFAULT_DB_PATH) -> bool:
    """
    Verify pipeline idempotence by running the pipeline twice back-to-back
    and verifying that the database row counts and state are identical.
    """
    logger.info("\n>>> VERIFYING IDEMPOTENCY <<<")
    logger.info("Executing Run #1...")
    run1 = run_pipeline(db_path)

    logger.info("\nExecuting Run #2 with identical inputs...")
    run2 = run_pipeline(db_path)

    m1 = run1["metrics_after"]
    m2 = run2["metrics_after"]

    logger.info("Comparing Run #1 vs Run #2:")
    logger.info("Run 1 Final Forecast Rows: %d", m1["forecasts_count"])
    logger.info("Run 2 Final Forecast Rows: %d", m2["forecasts_count"])
    logger.info("Run 1 Locations Count:     %d", m1["locations_count"])
    logger.info("Run 2 Locations Count:     %d", m2["locations_count"])
    logger.info("Run 1 Avg Temp:            %s", m1["temp_avg"])
    logger.info("Run 2 Avg Temp:            %s", m2["temp_avg"])

    assert m1["forecasts_count"] == m2["forecasts_count"], "Idempotency failed: Row counts differ!"
    assert m1["locations_count"] == m2["locations_count"], "Idempotency failed: Location counts differ!"
    assert m1["temp_avg"] == m2["temp_avg"], "Idempotency failed: Average temp differs!"
    assert m1["total_precipitation"] == m2["total_precipitation"], "Idempotency failed: Precip differs!"

    logger.info("\n[SUCCESS] Pipeline is strictly IDEMPOTENT! Running it twice produces the exact same result.\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Open-Meteo Weather Forecast ETL Pipeline")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to SQLite database file",
    )
    parser.add_argument(
        "--verify-idempotency",
        action="store_true",
        help="Run pipeline twice back-to-back to verify idempotence",
    )
    args = parser.parse_args()

    if args.verify_idempotency:
        verify_idempotence(args.db_path)
    else:
        run_pipeline(args.db_path)