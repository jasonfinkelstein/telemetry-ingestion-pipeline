import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict

# Loads environment variables from .env file
load_dotenv()

class Config:
    """
    Config manager for the data ingestion system. Loads database credentials from .env and source definitions from YAML file.
    """
    def __init__(self, config_path: str = "config/sources.yml"):
        self.config_path = Path(config_path)
        self.config_data = self._load_config()

        # get db credentials from environment variables
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_name = os.getenv("DB_NAME", "telemetry_db")
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load and parse the YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_db_url(self) -> str:
        """Build the database connection URL from credentials."""
        return (f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}")
    
    def get_sources(self) -> list:
        """Get the list of data sources from the configuration."""
        return self.config_data.get("sources", [])
    
    def get_source_by_name(self, name: str) -> Dict [str, Any]:
        """Get a specific data source config by name"""
        sources = self.get_sources()
        for source in sources:
            if source['name'] == name:
                return source
        raise ValueError(f"Source '{name}' not found in configuration.")
        