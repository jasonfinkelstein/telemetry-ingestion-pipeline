# F1 Telemetry Data Ingestion Pipeline

A Python-based data pipeline for ingesting Formula 1 telemetry data from CSV files into PostgreSQL with automatic validation and cleaning.

## Features

- CSV data ingestion with automatic type casting
- Configurable validation rules
- Data cleaning and normalization
- PostgreSQL storage with UPSERT support
- Comprehensive logging and error handling

## Requirements

- Python 3.9+
- PostgreSQL 12+

## Installation
```bash
# Clone repository
git clone <repo-url>
cd telemetry_ingestion

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Setup

1. **Configure database credentials** in `.env`:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=telemetry_db
DB_USER=postgres
DB_PASSWORD=your_password
```

2. **Initialize database**:
```bash
psql -U postgres -d telemetry_db -f schema.sql
```

3. **Place CSV file** in `data/` folder

4. **Update config** in `config/sources.yml` with your CSV details

## Usage

Run the ingestion pipeline:
```bash
python src/main.py
```

## Configuration

Edit `config/sources.yml` to define data sources and validation rules.

## Testing
```bash
pytest tests/ -v
```
