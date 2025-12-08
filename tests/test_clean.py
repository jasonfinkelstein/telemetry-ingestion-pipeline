import pytest
import pandas as pd
from src.clean import normalize_columns, trim_strings

def test_normalize_columns_acronyms():
    """Test normalization of acronym columns"""
    df = pd.DataFrame({
        'Date': [1, 2],
        'RPM': [100, 200],
        'DRS': [1, 0]
    })
    
    result = normalize_columns(df)
    
    # should be lowercased
    assert 'date' in result.columns
    assert 'rpm' in result.columns
    assert 'drs' in result.columns

def test_normalize_columns_camelcase():
    """Test normalization of camelCase columns"""
    df = pd.DataFrame({
        'nGear': [1, 2],
        'SessionTime': ['a', 'b']
    })
    
    result = normalize_columns(df)
    
    # CamelCase should convert to snake_case
    assert 'n_gear' in result.columns
    assert 'session_time' in result.columns

def test_trim_strings():
    """Test string trimming"""
    df = pd.DataFrame({
        'source': ['  car  ', 'car ', ' car'],
        'rpm': [100, 200, 300]
    })
    
    result = trim_strings(df)
    
    assert result['source'].iloc[0] == 'car'
    assert result['source'].iloc[1] == 'car'
    assert result['source'].iloc[2] == 'car'