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
    

