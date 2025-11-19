import pandas as pd
from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

def apply_rules(
        df: pd.DataFrame,
        rules: List[Dict[str, str]]
) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Apply validation rules to DataFrame using pandas queries.

    Rules are written in pandas query syntax:
    - "RPM >= 0" checks that RPM values are non-negative
    - "Speed <= 400" checks if speed is within realistic limits
    - "Throttle >= 0" and "Throttle <= 100" checks throttle percentage

    Args:
        df: DataFrame with typed data (after schema application)
        rules: List of rule dictionaries from config
            Format: [{'rule': 'RPM >= 0', 'message': 'RPM must be positive'}]

    Returns:
        Tuple of (valid DataFrame, list of rejected records)
    """

    # Handle edge cases
    if not rules or df.empty:
        return df, []
    
    rejects = []

    # Track which rows pass all rules
    valid_mask = pd.Series([True] * len(df), index=df.index)

    # Apply each rule sequentially
    for rule_config in rules:
        rule_expr = rule_config.get('rule', '')
        message = rule_config.get('message', f'Failed rule: {rule_expr}')

        try:
            logger.info(f"Applying rule: {rule_expr}")

            # Evaluate the rule expression
            # df.eval() runs the expression on the entire DataFrame and returns a boolean Series (True/False per row)
            rule_result = df.eval(rule_expr)

            # Find rows that FAIL this rule (False values)
            # elementwise logical NOT using Python's 'not' via map
            # failed_mask = ~rule_result # ~ is deprecated
            failed_mask = rule_result.map(lambda v: not v)
            failed_indices = df[failed_mask].index

            # Mark failed rows as rejected
            for idx in failed_indices:
                if valid_mask[idx]: # Only add if not already rejected
                    valid_mask[idx] = False
                    rejects.append({
                        'index': idx,
                        'data': df.loc[idx].to_dict(),
                        'reason': message
                    })
            logger.info(f"Rule '{rule_expr}': {failed_mask.sum()} rows failed")

        except Exception as e:
            # If rule syntax is bad, log error but continue with other rules
            logger.error(f"Error applying rule '{rule_expr}': {str(e)}")
            continue

    # Filter DataFrame to only valid rows
    valid_df = df[valid_mask].copy()

    logger.info(f"Rules validation: {len(valid_df)} valid, {len(rejects)} rejected")

    return valid_df, rejects
