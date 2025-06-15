import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

TIMEZONE_CSV_PATH = os.getenv("TIMEZONE_CSV_PATH")
BUSINESS_HOURS_CSV_PATH = os.getenv("BUSINESS_HOURS_CSV_PATH")
STATUS_CSV_PATH = os.getenv("STATUS_CSV_PATH")

def execute_sql(conn, cur, query, params=None):
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        conn.commit()
        print(f"Successfully executed:\n {query}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error executing query:\n{query}\nError: {e}")
        raise

conn = None
cur = None
try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Set isolation level to autocommit for CREATE TYPE
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Create ENUM type 
    create_enum_query = sql.SQL("""
        CREATE TYPE store_status_enum AS ENUM ('active', 'inactive');
    """)
    execute_sql(conn, cur, create_enum_query)

    # Revert to default isolation level 
    conn.set_isolation_level(psycopg2.IsolationLevel.READ_COMMITTED)

    # Table Creation
    create_status_table_query = sql.SQL("""
        CREATE TABLE store_status (
            store_id VARCHAR(50) NOT NULL,
            timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
            status VARCHAR(20) NOT NULL,
            PRIMARY KEY (store_id, timestamp_utc)
        );
    """)
    execute_sql(conn, cur, create_status_table_query)

    create_business_hours_table_query = sql.SQL("""
        CREATE TABLE store_business_hours (
            store_id VARCHAR(50) NOT NULL,
            dayOfWeek SMALLINT NOT NULL,
            start_time_local TIME NOT NULL,
            end_time_local TIME NOT NULL,
            PRIMARY KEY (store_id, dayOfWeek)
        );
    """)
    execute_sql(conn, cur, create_business_hours_table_query)

    create_timezones_table_query = sql.SQL("""
        CREATE TABLE timezones (
            store_id VARCHAR(50) NOT NULL,
            timezone_str VARCHAR(100) NOT NULL,
            PRIMARY KEY (store_id)
        );
    """)
    execute_sql(conn, cur, create_timezones_table_query)

    # Add Foreign Key Constraints
    add_fk_status_query = sql.SQL("""
        ALTER TABLE store_status
        ADD CONSTRAINT fk_store_status_store_id
        FOREIGN KEY (store_id) REFERENCES timezones(store_id);
    """)
    execute_sql(conn, cur, add_fk_status_query)

    add_fk_business_hours_query = sql.SQL("""
        ALTER TABLE store_business_hours
        ADD CONSTRAINT fk_store_business_hours_store_id
        FOREIGN KEY (store_id) REFERENCES timezones(store_id);
    """)
    execute_sql(conn, cur, add_fk_business_hours_query)

    # Load initial Timezones data 
    copy_timezones_query = sql.SQL("""
        COPY timezones(store_id, timezone_str)
        FROM {csv_path} DELIMITER ',' CSV HEADER;
    """).format(csv_path=sql.Literal(TIMEZONE_CSV_PATH))
    execute_sql(conn, cur, copy_timezones_query)

    # Modify Primary Key for store_business_hours
    drop_pk_business_hours_query = sql.SQL("""
        ALTER TABLE store_business_hours
        DROP CONSTRAINT IF EXISTS store_business_hours_pkey; 
    """)
    execute_sql(conn, cur, drop_pk_business_hours_query)

    add_composite_pk_business_hours_query = sql.SQL("""
        ALTER TABLE store_business_hours
        ADD CONSTRAINT pk_store_business_hours PRIMARY KEY (store_id, dayOfWeek, start_time_local, end_time_local);
    """)
    execute_sql(conn, cur, add_composite_pk_business_hours_query)

    # Load Business Hours with conditional timezone insert 
    create_temp_business_hours_query = sql.SQL("""
        CREATE TEMPORARY TABLE temp_business_hours (
            store_id VARCHAR(50),
            dayOfWeek SMALLINT,
            start_time_local TIME,
            end_time_local TIME
        );
    """)
    execute_sql(conn, cur, create_temp_business_hours_query)

    copy_temp_business_hours_query = sql.SQL("""
        COPY temp_business_hours(store_id, dayOfWeek, start_time_local, end_time_local)
        FROM {csv_path} DELIMITER ',' CSV HEADER;
    """).format(csv_path=sql.Literal(BUSINESS_HOURS_CSV_PATH))
    execute_sql(conn, cur, copy_temp_business_hours_query)

    insert_missing_timezones_query = sql.SQL("""
        INSERT INTO timezones (store_id, timezone_str)
        SELECT DISTINCT tbh.store_id, 'America/Chicago'
        FROM temp_business_hours tbh
        WHERE NOT EXISTS (
            SELECT 1
            FROM timezones tz 
            WHERE tz.store_id = tbh.store_id
        );
    """)
    execute_sql(conn, cur, insert_missing_timezones_query)

    insert_business_hours_query = sql.SQL("""
        INSERT INTO store_business_hours (store_id, dayOfWeek, start_time_local, end_time_local)
        SELECT store_id, dayOfWeek, start_time_local, end_time_local
        FROM temp_business_hours;
    """)
    execute_sql(conn, cur, insert_business_hours_query)

    drop_temp_business_hours_query = sql.SQL("""
        DROP TABLE temp_business_hours;
    """)
    execute_sql(conn, cur, drop_temp_business_hours_query)

    # Alter status column to ENUM
    alter_status_column_query = sql.SQL("""
        ALTER TABLE store_status
        ALTER COLUMN status TYPE store_status_enum USING status::store_status_enum;
    """)
    execute_sql(conn, cur, alter_status_column_query)
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()