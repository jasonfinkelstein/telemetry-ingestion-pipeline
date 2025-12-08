import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

def normalize_columns(df):
    """
    Normalize column names for database
    """

    df = df.copy() # keeps original df unmodified

    original_cols = df.columns.tolist()
    normalized_cols = []

    for col in original_cols:
        normalized = str(col).strip()

        if normalized.isupper():
            normalized = normalized.lower()
        else: # camelCase handling
            normalized = re.sub(r'(?<!^)(?=[A-Z])', '_', normalized).lower()

        normalized_cols.append(normalized)
    
    df.columns = normalized_cols

    logger.info(f'Normalized {len(df.columns)} column names')
    logger.debug(f'Mapping: {dict(zip(original_cols, normalized_cols))}')

    return df

def trim_strings(df):
    """
    Trim whitespace from all string columns
    """

    df = df.copy()

    string_cols = df.select_dtypes(include=['object']).columns

    for col in string_cols:
        df[col] = df[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )
    
    if len(string_cols) > 0:
        logger.info(f'Trimmed whitespace from {len(string_cols)} string columns')
    
    return df