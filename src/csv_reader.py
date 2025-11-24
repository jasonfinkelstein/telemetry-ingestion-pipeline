import pandas as pd
from pathlib import Path
import logging

# Set up logging
logger = logging.getLogger(__name__)

def read_csv(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Read CSV file into pandas DataFrame with error handling.
    
    Args:
        file_path (str): Path to the CSV file. (e.g. "data/verstappen_telemetry_miami_2024.csv")
        **kwargs: Additional keyword arguments to pass to pd.read_csv (e.g. delimiter=';', encoding='utf-8').
    
    Returns:
        pd.DataFrame: DataFrame containing the CSV data.

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        Exception: If CSV is malformed or can't be parsed
    """

    # Convert string path to Path object for better path handling
    path = Path(file_path)

    # Check if file exists before trying to read
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    try:
        logger.info(f"Reading CSV file: {file_path}")

        # pandas.read_csv reads the entire CSV into memory and returns a DataFrame
        df = pd.read_csv(path, **kwargs)

        if 'Unnamed: 0' in df.columns:
            df = df.rename(columns={'Unnamed: 0': 'record_id'})
            logger.info("Renamed 'Unnamed: 0' column to 'record_id")

        logger.info(f"Successfully read CSV file: {file_path} with {len(df)} records.")
        return df
    
    except Exception as e:
        # Log the error with details
        logger.error(f"Error reading CSV file {file_path}: {str(e)}")
        # Re-raise the exception so caller knows it failed
        raise 