import logging
import time
from pathlib import Path
from typing import Dict, Any
from config import Config
from csv_reader import read_csv
from validate import apply_schema
from rules import apply_rules
from clean import normalize_columns, trim_strings
from load import load_dataframe, write_rejects

# Configure logging
logging.basicConfig(
    level=logging.INFO, # Show INFO, WARNING, ERROR messages
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), # Print to console
        logging.FileHandler('logs/ingestion.log') # Save to file
    ]
)

logger = logging.getLogger(__name__)