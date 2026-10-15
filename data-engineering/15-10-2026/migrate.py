import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "weather_data.db"
MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_name TEXT NOT NULL UNIQUE,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()


def get_applied_migrations(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT migration_name FROM schema_migrations;")
    applied = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return applied


def run_migrations(db_path: Path = DEFAULT_DB_PATH) -> None:
    print(f"Connecting to database: {db_path}")
    conn = get_connection(db_path)
    try:
        init_migrations_table(conn)
        applied = get_applied_migrations(conn)

        if not MIGRATIONS_DIR.exists():
            print(f"Migrations directory not found: {MIGRATIONS_DIR}")
            return

        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        if not migration_files:
            print("No SQL migration files found.")
            return

        for migration_file in migration_files:
            name = migration_file.name
            if name in applied:
                print(f"[SKIP] Migration already applied: {name}")
                continue

            print(f"[RUNNING] Applying migration: {name}...")
            with open(migration_file, "r", encoding="utf-8") as f:
                sql_script = f.read()

            cursor = conn.cursor()
            try:
                cursor.executescript(sql_script)
                cursor.execute(
                    "INSERT INTO schema_migrations (migration_name) VALUES (?);",
                    (name,),
                )
                conn.commit()
                print(f"[SUCCESS] Migration applied successfully: {name}")
            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Failed to apply migration {name}: {e}")
                raise
            finally:
                cursor.close()

    finally:
        conn.close()


if __name__ == "__main__":
    db_file = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DB_PATH
    run_migrations(db_file)
