-- Migration: 001_create_weather_tables.sql
-- Description: Create initial schema for Open-Meteo weather forecast pipeline

-- Table to track executed migrations for idempotency and version control
CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_name TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Locations entity table
CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    elevation REAL,
    timezone TEXT,
    utc_offset_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Hourly weather forecast entity table
CREATE TABLE IF NOT EXISTS hourly_forecasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    forecast_time TEXT NOT NULL,
    temperature_2m REAL,
    relative_humidity_2m REAL,
    apparent_temperature REAL,
    precipitation REAL,
    rain REAL,
    weather_code INTEGER,
    weather_condition TEXT,
    surface_pressure REAL,
    wind_speed_10m REAL,
    temp_feels_diff REAL,
    wind_category TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE CASCADE,
    CONSTRAINT uq_location_time UNIQUE (location_id, forecast_time)
);

-- Indexes to optimize analytical and lookup queries
CREATE INDEX IF NOT EXISTS idx_forecasts_location_id ON hourly_forecasts(location_id);
CREATE INDEX IF NOT EXISTS idx_forecasts_time ON hourly_forecasts(forecast_time);
CREATE INDEX IF NOT EXISTS idx_forecasts_weather_code ON hourly_forecasts(weather_code);
