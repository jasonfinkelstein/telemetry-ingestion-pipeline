import pytest
import pandas as pd
from pathlib import Path
import tempfile
from src.csv_reader import read_csv


def test_read_csv_success():
    '''Test reading a valid CSV file'''
    # create temp csv file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('Date,RPM,Speed\n')
        f.write('2025-12-01,10000,50\n')
        f.write('2025-12-02,12000,60\n')
        temp_path = f.name

    try:
        # read the file
        df = read_csv(temp_path)

        # assert expectations
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'Date' in df.columns
        assert 'RPM' in df.columns
        assert 'Speed' in df.columns

    finally:
        Path(temp_path).unlink()

def test_read_csv_with_index():
    '''Test reading csv with unnamed index colunm'''
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('Unnamed: 0,Date,RPM,Speed\n')
        f.write('0,2025-12-01,10000,50\n')
        f.write('1,2025-12-02,10000,60\n')
        temp_path = f.name
    
    try:
        df = read_csv(temp_path)

        # index should be renamed to record_id
        assert 'record_id' in df.columns
        assert 'Unnamed: 0' not in df.columns
    
    finally:
        Path(temp_path).unlink()

def test_read_csv_file_not_found():
    '''Test taht FileNotFoundError is raised if files are missing'''
    with pytest.raises(FileNotFoundError):
        read_csv('nonexistent_file.csv')

def test_read_csv_empty_file():
    '''Test reading an empty CSV (only headers)'''
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('Date,RPM,Speed\n')
        temp_path = f.name
    
    try:
        df = read_csv(temp_path)
        assert len(df) == 0
        assert len(df.columns) == 3

    finally:
        Path(temp_path).unlink()