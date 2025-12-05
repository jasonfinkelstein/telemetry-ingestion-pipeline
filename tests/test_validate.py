import pytest
import pandas as pd
from src.validate import cast_to_type, apply_schema

def test_cast_to_type_int():
    '''Test integer casting'''
    assert cast_to_type('100', 'int') == 100
    assert cast_to_type('100.5', 'int') == 100
    assert cast_to_type(100, 'int') == 100

def test_cast_to_type_bool():
    '''Test boolean casting'''
    assert cast_to_type('TRUE', 'bool') == True
    assert cast_to_type('FALSE', 'bool') == False
    assert cast_to_type('1', 'bool') == True
    assert cast_to_type(True, 'bool') == True

def test_cast_to_type_datetime():
    '''Test datetime casting'''
    result = cast_to_type('2025-01-01', 'datetime')
    assert pd.notna(result)
    assert isinstance(result, pd.Timestamp)

def test_cast_to_type_invalid():
    '''Test that invalid casts raise ValueError'''
    with pytest.raises(ValueError):
        cast_to_type('not_a_number', 'int')

def test_apply_schema_all_valid():
    '''Test schema application with valid data'''
    df = pd.DataFrame({
        'rpm': ['10000', '12000'],
        'speed': ['50', '60']
    })
    schema = {'rpm':'int', 'speed':'int'}

    valid_df, rejects = apply_schema(df, schema)

    assert len(valid_df) == 2
    assert len(rejects) == 0
    assert valid_df['rpm'].dtype == 'int64'

def test_apply_schema_some_invalid():
    '''Test schema application with some invalid data'''
    df = pd.DataFrame({
        'rpm': ['10000', 'invalid', '12000'],
        'speed': ['50', '60', '70']
    })
    schema = {'rpm':'int', 'speed':'int'}

    valid_df, rejects = apply_schema(df, schema)

    assert len(valid_df) == 2 # rows 0, 2
    assert len(rejects) == 1 # row 1
    assert 'Type casting failed' in rejects[0]['reason']