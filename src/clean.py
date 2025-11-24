import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to database friendly format.

    Transformations:
    - Converts camelCase to snake_case
    - Lowercase all names
    - Removes special characters

    Examples:
    - 'Date' -> 'date'
    - 'RPM' -> 'rpm'
    - 'nGear' -> 'n_gear'
    - 'SessionTime' -> 'session_time'

    Args:
        df: Input dataframe with original CSV column names
    
    Returns:
        Dataframe with normalized column names
    """

    df = df.copy() # keeps original dataframe unmodified

    original_cols = df.columns.tolist()
    normalized_cols = []

    for col in original_cols:
        # strip whitespace
        normalized = str(col).strip()

        if normalized.isupper(): # Check is col is all caps (e.g. RPM, DRS)
            normalized = normalized.lower()
        else: # Handles camelCase
            normalized = re.sub(r'(?<!^)(?=[A-Z])', '_', normalized).lower()

        normalized_cols.append(normalized)
    
    df.columns = normalized_cols

    logger.info(f'Normalized {len(df.columns)} column names')
    logger.debug(f'Mapping: {dict(zip(original_cols, normalized_cols))}')

    return df

def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace from all string columns.
    
    Fixes issues like:
    - 'car ' -> 'car'
    - ' car' -> 'car'
    - ' car ' -> 'car'

    Args:
        df: Input dataframe

    Returns:
        Dataframe with trimmed strings
    """

    df = df.copy()

    # Find all string/object columns
    string_cols = df.select_dtypes(include=['object']).columns

    # Trim each string column
    for col in string_cols:
        df[col] = df[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )
    
    if len(string_cols) > 0:
        logger.info(f'Trimmed whitespace from {len(string_cols)} string columns')
    
    return df