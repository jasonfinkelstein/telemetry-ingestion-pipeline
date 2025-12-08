import pandas as pd
from pathlib import Path
import logging

# Set up logging
logger = logging.getLogger(__name__)

def read_csv(file_path, **kwargs):
    """
    Read CSV file into DataFrame
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    try:
        logger.info(f"Reading CSV file: {file_path}")

        df = pd.read_csv(path, **kwargs)

        if 'Unnamed: 0' in df.columns:
            df = df.rename(columns={'Unnamed: 0': 'record_id'})
            logger.info("Renamed 'Unnamed: 0' column to 'record_id")

        logger.info(f"Successfully read CSV file: {file_path} with {len(df)} records.")
        return df
    
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {str(e)}")
        raise 