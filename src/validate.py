import pandas as pd
import numpy as np
from typing import Tuple, Dict, List
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def parse_timedelta(value):
    """
    Parse time delta string in format '0 days HH:MM:SS.ffffff'
    Args:
        value (str): String representation of time duration
    
    Returns:
        pandas.Timedelta object or original value if already a timedelta
    """
    # Handle missing values
    if pd.isna(value):
        return value
    
    try:
        # built into pandas
        if isinstance(value, pd.Timedelta):
            return value
        
        if isinstance(value, str):
            return pd.to_timedelta(value)
        
        return value
    
    except Exception as e:
        logger.error(f"Failed to parse timedelta from value '{value}': {str(e)}")

        raise ValueError(f"Could not parse timedelta from value '{value}': {str(e)}")

def cast_to_type(value, target_type: str):
    """
    Cast a single value to the specified target type.

    Args:
        value: The value to be cast.
        target_type (str): The target type as a string. ('int', 'float', 'str', 'bool', 'timedelta').

    Returns:
        The value cast to the target type.

    Raises:
        ValueError: If the target type is unsupported or casting fails.
    """

    # Missing values stay missing
    if pd.isna(value):
        return value
    
    try:
        if target_type == 'int':
            # convert to float first to handle num.0 then to int
            return int(float(value))
        
        elif target_type == 'float':
            return float(value)
        
        elif target_type == 'str':
            return str(value)
        
        elif target_type == 'bool':
            # Hndle various string representations of boolean
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.upper() in ('TRUE', '1', 'YES')
            return bool(value)
        
        elif target_type == 'datetime':
            return pd.to_datetime(value)
        
        elif target_type == 'timedelta':
            return parse_timedelta(value)
        
        else:
            return value
        
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to cast value '{value}' to type '{target_type}': {str(e)}")
        raise ValueError(f"Could not cast value '{value}' to type '{target_type}': {str(e)}")
    

def apply_schema(
        df: pd.DataFrame,
        schema: Dict[str, str]
    ) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Apply schema to DataFrame, casting columns to specified types.
    This func processes the entire DataFrame:
    1. Goes through each column in the schema.
    2. Tries to convert each value to the target type.
    3. If converstion fails, marks that row as rejected
    4. Returns valid rows and rejected rows separately.

    Args:
        df: Input DataFrame from CSV
        schema: Dict mapping column names to target types (e.g. {{'RPM': 'int', 'Speed': 'int', 'Brake': 'bool'})

    Returns:
        Tuple containing:
            - DataFrame with valid rows cast to target types
            - List of dicts with rejected rows and error reasons
    """
    rejects = [] # Will hold all rejected rows
    valid_indices = set(df.index) # Track which rows are still valid

    # Process each column in the schema
    for col, dtype in schema.items():
        if col not in df.columns:
            logger.warning(f"Schema column '{col}' not found in CSV data")
            continue

        logger.info(f"Casting colum '{col}' to type '{dtype}'")

        # process each row in the column
        for idx in df.index:
            # Skip if row already rejected
            if idx not in valid_indices:
                continue

            try:
                # try to case this cell's value
                df.at[idx, col] = cast_to_type(df.at[idx, col], dtype)
            
            except ValueError as e:
                # Casting failed, mark this row as rejected
                valid_indices.discard(idx)
                rejects.append({
                    'index': idx,
                    'data': df.loc[idx].to_dict(), # Save entire row data
                    'reason': f"Type casting failed for column '{col}': {str(e)}"
                })
    # Create DataFrame with only valid rows
    valid_df = df.loc[list(valid_indices)].copy()

    # Convert column dtypes explicitly for valid rows
    for col, dtype in schema.items():
        if col in valid_df.columns:
            if dtype == 'int':
                valid_df[col] = valid_df[col].astype('int64')
            elif dtype == 'float':
                valid_df[col] = valid_df[col].astype('float64')
            elif dtype == 'bool':
                valid_df[col] = valid_df[col].astype('bool')

    logger.info(f"Schema validation: {len(valid_df)} valid, {len(rejects)} rejected")

    return valid_df, rejects
