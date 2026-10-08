import psycopg2
import time
import os

def migrate():
    # Connection parameters
    conn_params = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "password",
        "dbname": "northwind"
    }

    # Wait for the DB to become available
    max_retries = 10
    conn = None
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**conn_params)
            break
        except psycopg2.OperationalError:
            print(f"Waiting for database to start... ({i+1}/{max_retries})")
            time.sleep(2)
            
    if not conn:
        print("Could not connect to the database.")
        return

    print("Connected to the database. Running migrations...")
    cursor = conn.cursor()
    
    with open('init.sql', 'r') as f:
        sql = f.read()
        
    try:
        cursor.execute(sql)
        conn.commit()
        print("Migration completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during migration: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate()
