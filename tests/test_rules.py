import pytest
import pandas as pd
from src.rules import apply_rules

def test_apply_rules_all_pass():
    '''Test when all rows pass validation rules'''
    df = pd.DataFrame({
        'rpm': [10000, 12000, 15000],
        'speed': [50, 60, 70]
    })
    rules = [
        {'rule': 'rpm >= 0', 'message': 'RPM must be positive'},
        {'rule': 'rpm <= 20000', 'message': 'RPM too high'}
    ]

    valid_df, rejects = apply_rules(df, rules)
    
    assert len(valid_df) == 3
    assert len(rejects) == 0

def test_apply_rules_some_fail():
    """Test when some rows fail validation"""
    df = pd.DataFrame({
        'rpm': [10000, 25000, -100],  # Row 1 too high, row 2 negative
        'speed': [50, 60, 70]
    })
    rules = [
        {'rule': 'rpm >= 0', 'message': 'RPM must be positive'},
        {'rule': 'rpm <= 20000', 'message': 'RPM too high'}
    ]
    
    valid_df, rejects = apply_rules(df, rules)
    
    assert len(valid_df) == 1  # Only row 0
    assert len(rejects) == 2   # Rows 1 and 2
    assert any('too high' in r['reason'] for r in rejects)
    assert any('positive' in r['reason'] for r in rejects)

def test_apply_rules_empty_rules():
    """Test with no rules (all rows should pass)"""
    df = pd.DataFrame({'rpm': [10000, 12000]})
    
    valid_df, rejects = apply_rules(df, [])
    
    assert len(valid_df) == 2
    assert len(rejects) == 0