# End-to-end Data Ingestion Pipeline 
#### CSV → Parquet → Cloud Storage → Snowflake (Staging Layer)
### Project Overview
This project implements an end-to-end data ingestion pipeline that takes heterogeneous raw CSV files and delivers a clean, typed Snowflake staging table, ready for downstream analytics or transformation.

### High Level Architecture 
    Raw CSV Files
          ↓
    Schema-Driven Normalization (Python)
          ↓
    Parquet (Staging Files)
          ↓
    Azure Blob Storage
          ↓
    Snowflake External Stage
          ↓
    Snowflake Staging Table

### Project Structure
    project/
    ├── data_raw/
    │   └── source_*.csv
    ├── data_processed/
    │   └── output.parquet
    ├── src/
    │   ├── csv_normalization.py
    │   ├── snowflake_loader.py
    │   └── utils/
    ├── config/
    │   ├── schema_mapping.json
    │   └── config.toml
    └── README.md

### Processing Flow
1. Read raw CSV files
2. Normalize schema and values
3. Write normalized Parquet
4. Upload Parquet to Azure Blob Storage
5. Infer Snowflake table schema from Parquet
6. Load data incrementally using COPY INTO


## Phase 1 — CSV Normalization & Parquet Staging
Convert heterogeneous CSV inputs into a consistent, analytics-ready dataset while preserving all source records. (No deduplication)

### Input Data
The pipeline ingests multiple CSV files located in a configurable directory.

Characteristics of input data:
- Different column names for similar fields
- Partial overlap of information across files
- Inconsistent formatting (dates, emails, phone numbers)
- Missing or null values

### Schema Mapping
Schema normalization is driven by an external JSON configuration file:

    {
      "name": "email",
      "type": "email",
      "values": ["Email"]
    }

### Data Normalization Rules
The following standardization rules are applied across all files:
- Strings: trimmed and cleaned
- Emails: lowercased and stripped of whitespace
- Dates: parsed using flexible date parsing and converted to YYYY-MM-DD
- Phone numbers: normalized to XXX-XXX-XXXX when valid
- Null values: standardized to NaN
- Invalid values (e.g., malformed phone numbers) are logged and preserved as nulls to avoid pipeline failure.

### Data Lineage 
To support traceability and debugging, the pipeline adds:
- source_file — name of the originating CSV
- row_id — source-specific row identifier using UUID

### Output
The final output is a single Parquet file containing all normalized records.

### How to Run
    python src/pipeline.py

## Phase 2 — Cloud Storage & Snowflake Ingestion
Load normalized Parquet data into Snowflake using a cloud-native ingestion pattern.

### Schema Handling Strategy
- Table schema is inferred once from Parquet metadata
- Avoids duplicating schema definitions in code
- Keeps staging tables aligned with upstream data
- Parquet fields are mapped to table columns using MATCH_BY_COLUMN_NAME
- Produces typed, columnar Snowflake tables

### Output
The final output is a single Parquet file containing all normalized records.

### How to Run
    set config.toml
    python src/snowflake_loader.py