import logging
import time
import re
from pathlib import Path
from typing import Dict, Any
from config import Config
from csv_reader import read_csv
from validate import apply_schema
from rules import apply_rules
from clean import normalize_columns, trim_strings
from load import load_dataframe, write_rejects


# Configure logging
logging.basicConfig(
    level=logging.INFO, # Show INFO, WARNING, ERROR messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), # Print to console
        logging.FileHandler('logs/ingestion.log') # Save to file
    ]
)

logger = logging.getLogger(__name__)

def run_telemetry_ingestion(source_config, db_url):
    """
    Run the complete ingestion pipeline for a data source
    """

    source_name = source_config['name']
    start_time = time.time()

    # banner for better readability
    logger.info('=' * 80)
    logger.info(f'Starting ingestion for source: {source_name}')
    logger.info('=' * 80)

    # Initialize stats
    stats = {
        'source': source_name,
        'status': 'failed', # assume failure until success
        'rows_read': 0,
        'rows_valid': 0,
        'rows_rejected': 0,
        'rows_loaded': 0,
        'duration_seconds': 0.0,
        'error': None
    }

    try:
        logger.info(f"Step 1/6: Reading CSV file: {source_config['path']}")
        df = read_csv(source_config['path'])
        stats['rows_read'] = len(df)
        logger.info(f'Read {len(df)} rows from CSV')

        logger.info('Step 2/6: Normalizing column names for database')
        df = normalize_columns(df)

        logger.info('Step 3/6: Cleaning data (trimming strings)')
        df = trim_strings(df)

        logger.info('Step 4/6: Applying validation schema and type casting')
        all_rejects = [] # to collect rejects

        # Normalize schema column names to match normalized DataFrame columns
        normalized_schema = {}
        for col, dtype in source_config['schema'].items():
            if col.isupper():
                normalized_col = col.lower()
            else:
                normalized_col = re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower()
            normalized_schema[normalized_col] = dtype

        df, schema_rejects = apply_schema(df, normalized_schema)
        all_rejects.extend(schema_rejects)

        if 'rules' in source_config:
            logger.info('Step 5/6: Applying validation rules')
            df, rule_rejects = apply_rules(df, source_config['rules'])
            all_rejects.extend(rule_rejects)
        else:
            logger.info('Step 5/6: No validation rules configured, skipping')
        
        # Update stats after validation
        stats['rows_valid'] = len(df)
        stats['rows_rejected'] = len(all_rejects)

        logger.info(f'Step 6/6: Loading {len(df)} valid records into {source_config["target_table"]}')
        load_stats = load_dataframe(
            df=df,
            table=source_config['target_table'],
            primary_keys=source_config.get('pk', []),
            db_url=db_url,
            batch_size=source_config.get('batch_size', 5000)
        )
        stats['rows_loaded'] = load_stats['total']

        # Write rejects
        if all_rejects:
            logger.info(f"Writing {len(all_rejects)} rejected records")
            write_rejects(all_rejects, source_name, db_url)

        stats['status'] = 'success'

    except Exception as e:
        # Something went wrong
        logger.error(f'Error during ingestion: {str(e)}', exc_info=True)
        stats['error'] = str(e)
        stats['status'] = 'failed'
        raise # re-raise after logging so caller knows

    finally: 
        stats['duration_seconds'] = round(time.time() - start_time, 2)

        logger.info('=' * 80)
        logger.info(f'Ingestion summary for source: {source_name}')
        logger.info(f"Status: {stats['status']}")
        logger.info(f"Rows read: {stats['rows_read']}")
        logger.info(f"Rows valid: {stats['rows_valid']}")
        logger.info(f"Rows rejected: {stats['rows_rejected']}")
        logger.info(f"Rows loaded: {stats['rows_loaded']}")
        logger.info(f"Duration (seconds): {stats['duration_seconds']}")
        if stats['error']:
            logger.info(f"Error: {stats['error']}")
        logger.info('=' * 80)
    
    return stats


def run_all_sources(config_path = "config/sources.yml"):
    """
    Run ingestion for all configured sources
    """

    logger.info('Starting ingestion pipeline')

    # Load config
    config = Config(config_path)
    db_url = config.get_db_url()
    sources = config.get_sources()

    results = []

    # Process each source
    for source_config in sources:
        try:
            result = run_telemetry_ingestion(source_config, db_url)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process source {source_config['name']}: {str(e)}")
            result.append({
                'source': source_config['name'],
                'status': 'failed',
                'error': str(e)
            })
    
    # Print overall summary
    logger.info('=' * 80)
    logger.info('Pipeline completed. Overall summary:')
    logger.info(f'Total sources processed: {len(results)}')
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'failed')
    logger.info(f'Successful: {successful}, Failed: {failed}')
    logger.info('=' * 80)
    
    return results


if __name__ == "__main__":
    run_all_sources()