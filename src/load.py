import psycopg
from psycopg import sql
import pandas as pd
from typing import List, Optional
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection(db_url):
    """
    Database connection context manager
    """

    conn = None

    try:
        # Establish connection
        conn = psycopg.connect(db_url)
        logger.info("Database connection established")

        # Yield connection to caller
        yield conn

        conn.commit()
        logger.info("Transaction committed successfully")

    except Exception as e:
        # Error occurred, rollback transaction
        if conn:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {str(e)}")
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


def load_dataframe(
        df,
        table,
        primary_keys,
        db_url,
        batch_size = 5000
):
    """
    Load DataFrame into PostgreSQL table using UPSERT
    """

    if df.empty:
        logger.warning(f'No data to load into {table}')
        return {'inserted': 0, 'total': 0}
    
    columns = df.columns.tolist()
    cols_sql = ', '.join(columns)

    placeholders = ', '.join(['%s'] * len(columns))

    # Build UPSERT query if primary keys exist
    if primary_keys:
        pk_conflict = ', '.join(primary_keys)
        update_cols = [col for col in columns if col not in primary_keys]
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        query = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES ({placeholders})
            ON CONFLICT ({pk_conflict})
            DO UPDATE SET {update_set}
        """
    else:
        query = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"

    total_rows = len(df)
    rows_processed = 0

    logger.info(f'Loading {total_rows} rows into {table} (batch size: {batch_size})')

    # Use context manager to handle DB connection
    with get_db_connection(db_url) as conn:
        with conn.cursor() as cursor:
            # batches to avoid memory issues
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]

                rows = [tuple(row) for row in batch_df.values]

                # Execute batch insert
                cursor.executemany(query, rows)

                rows_processed += len(rows)
                logger.info(f'Loaded batch: {rows_processed}/{total_rows} rows')
                logger.info(f'Successfully loaded {rows_processed} rows into {table}')
    
    return {'inserted': rows_processed, 'total': total_rows}


def write_rejects(
        rejects,
        source_name,
        db_url
):
    """
    Write rejected records to stg_rejects table
    """

    if not rejects:
        logger.info("No rejects to write")
        return 0
    
    logger.info(f"Writing {len(rejects)} rejected records for source '{source_name}'")

    with get_db_connection(db_url) as conn:
        with conn.cursor() as cursor:
            # Query to insert rejects
            query = """
                INSERT INTO stg_rejects (source_name, raw_payload, reason)
                VALUES (%s, %s, %s)
            """
            rows = []

            for reject in rejects:
                # Convert pandas Timestamps to strings for JSON serialization
                data_copy = {}
                for key, value in reject['data'].items():
                    if hasattr(value, 'isoformat'):  # Check if it's a datetime like object
                        data_copy[key] = value.isoformat()
                    else:
                        data_copy[key] = value
                
                rows.append((
                    source_name,
                    psycopg.types.json.Json(data_copy),
                    reject['reason']
                ))

            # Insert all rejects in one batch
            cursor.executemany(query, rows)
    
    logger.info(f'Successfully wrote {len(rejects)} rejects')

    return len(rejects)