# Open-Meteo Weather Forecast ETL Pipeline (15-10-2026)

This project implements a data engineering pipeline that extracts forecast data from the [Open-Meteo API](https://open-meteo.com/en/docs), cleans and transforms the dataset, applies relational database migrations, and loads the data into SQLite with strict idempotency guarantees.

---

## Architecture Overview

```
                                      ┌─────────────────────────────────┐
                                      │        Open-Meteo API           │
                                      │   (/v1/forecast endpoint)       │
                                      └────────────────┬────────────────┘
                                                       │
                                            Extraction │ (Cached & Retried)
                                                       ▼
                                      ┌─────────────────────────────────┐
                                      │         Raw DataFrames          │
                                      └────────────────┬────────────────┘
                                                       │
                                      Cleaning & Transf│
                                                       ▼
                                      ┌─────────────────────────────────┐
                                      │   Cleaned & Transformed Data    │
                                      │ - WMO code mapped to text       │
                                      │ - Bounds & missing data handled │
                                      │ - temp_feels_diff calculated    │
                                      │ - wind_category classified      │
                                      └────────────────┬────────────────┘
                                                       │
                                       Idempotent Load │ (ON CONFLICT UPSERT)
                                                       ▼
                                      ┌─────────────────────────────────┐
                                      │     SQLite Database             │
                                      │ - schema_migrations             │
                                      │ - locations                     │
                                      │ - hourly_forecasts              │
                                      └─────────────────────────────────┘
```

---

## 1. Selected Open-Meteo API Endpoint

- **Endpoint**: `https://api.open-meteo.com/v1/forecast`
- **Locations**: Baku (`40.4093°N, 49.8671°E`) and Berlin (`52.5200°N, 13.4050°E`)
- **Hourly Variables Extracted**:
  - `temperature_2m`: Air temperature at 2 meters above ground (°C)
  - `relative_humidity_2m`: Relative humidity at 2 meters (%)
  - `apparent_temperature`: Feels-like temperature (°C)
  - `precipitation`: Total precipitation (mm)
  - `rain`: Rain amount (mm)
  - `weather_code`: WMO weather interpretation code
  - `surface_pressure`: Atmospheric pressure at surface level (hPa)
  - `wind_speed_10m`: Wind speed at 10 meters above ground (km/h)

---

## 2. Database Schema & Migration

Database migrations are tracked in `schema_migrations` and executed via `migrate.py` or automatically during pipeline initialization.

### Schema Entities

1. **`schema_migrations`**:
   - `id`: Primary key
   - `migration_name`: Unique migration filename
   - `applied_at`: Execution timestamp

2. **`locations`**:
   - `id`: Primary key
   - `name`: Unique location name (e.g., "Baku", "Berlin")
   - `latitude`, `longitude`, `elevation`: Geographic coordinates
   - `timezone`, `utc_offset_seconds`: Timezone metadata
   - `created_at`, `updated_at`: Audit timestamps

3. **`hourly_forecasts`**:
   - `id`: Primary key
   - `location_id`: Foreign key referencing `locations(id)`
   - `forecast_time`: UTC timestamp of the hourly forecast
   - `temperature_2m`, `apparent_temperature`, `temp_feels_diff`: Temperatures and difference
   - `relative_humidity_2m`, `precipitation`, `rain`, `surface_pressure`, `wind_speed_10m`
   - `weather_code`, `weather_condition`: WMO code and descriptive text
   - `wind_category`: Beaufort scale classification
   - `ingested_at`: Audit timestamp
   - **`CONSTRAINT uq_location_time UNIQUE (location_id, forecast_time)`**: Ensures relational uniqueness and forms the basis for idempotent upserts.

---

## 3. Data Cleaning and Transformation

The data pipeline applies rigorous cleaning and transformation stages before loading:

### Cleaning (`clean_weather_data`)
- **Missing Value Handling**: Forward/backward fills continuous physical properties (temperatures, pressure, humidity).
- **Physical Boundary Validation**:
  - `precipitation` and `rain` clipped to `[0.0, ∞)`
  - `relative_humidity_2m` clipped to `[0.0, 100.0]`
  - `wind_speed_10m` clipped to `[0.0, ∞)`
- **Deduplication**: Removes duplicate timestamps keeping the most recent data point.

### Transformation & Feature Engineering (`transform_weather_data`)
- **Weather Condition Mapping**: Decodes WMO codes (0–99) to human-readable strings (e.g., "Clear sky", "Partly cloudy", "Rain showers").
- **Temperature Difference (`temp_feels_diff`)**: Derives `temperature_2m - apparent_temperature` to measure discrepancy between real and perceived temperatures.
- **Wind Speed Categorization (`wind_category`)**: Categorizes wind speeds into *Calm*, *Light Breeze*, *Moderate Breeze*, *Strong Breeze*, or *High Wind / Gale*.
- **Standardization**: Converts datetime to ISO 8601 UTC string format and rounds numerical measurements to 2 decimal places.

---

## 4. Idempotency Guarantees

The pipeline is guaranteed to be **idempotent**: running it multiple times with the same inputs produces the exact same database state without duplicate records or primary key violations.

### Mechanism
- Database tables define unique constraints: `UNIQUE(name)` on locations and `UNIQUE(location_id, forecast_time)` on forecasts.
- Database writes use `INSERT ... ON CONFLICT (...) DO UPDATE SET ...` (UPSERT).
- Re-running the pipeline updates existing rows in place without creating new records.

### Verification
Run the built-in idempotency verification suite:
```bash
uv run python pipeline.py --verify-idempotency
```
This executes the pipeline twice in succession and asserts that:
- Total location count remains identical
- Total forecast row count remains identical
- Statistical metrics (min/max/average temperatures, precipitation) remain identical

---

## 5. Getting Started & Execution

### Prerequisites
- Python 3.12+
- `uv` package manager

### Run Database Migrations
```bash
uv run python migrate.py
```

### Run the Pipeline
```bash
uv run python pipeline.py
```

### Verify Idempotency
```bash
uv run python pipeline.py --verify-idempotency
```

### Query Database Directly
```bash
sqlite3 weather_data.db "SELECT l.name, h.forecast_time, h.temperature_2m, h.weather_condition, h.wind_category FROM hourly_forecasts h JOIN locations l ON h.location_id = l.id LIMIT 10;"
```
