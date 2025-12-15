## CSV Normalization & Staging Pipeline (Parquet Output)
### Project Overview
This project implements a schema-driven data ingestion and normalization pipeline that consolidates multiple heterogeneous CSV files into a single, clean parquet staging dataset. 
The pipeline simulates a real-world staging (Silver) layer in a data engineering architecture, where raw source data is standardized and prepared for downstream processing such as deduplication, analytics, or warehouse loading.

### Objectives
- Ingest multiple CSV files with different schemas
- Normalize column names and data formats using a configurable schema mapping
- Apply consistent data-cleaning rules across all sources
- Preserve all records without premature deduplication
- Track data lineage at the row level
- Output a single, columnar Parquet dataset

### Input Data
The pipeline ingests multiple CSV files located in a configurable directory.

Characteristics of input data:
- Different column names for similar fields
- Partial overlap of information across files
- Inconsistent formatting (dates, emails, phone numbers)
- Missing or null values

### Project Structure
    project/
    ├── data_raw/
    │   ├── source_a.csv
    │   ├── source_b.csv
    │   └── source_c.csv
    ├── data_processed/
    │   └── output.parquet
    ├── config/
    │   └── schema_mapping.json
    ├── src/
    │   └── pipeline.py
    └── README.md

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

### Processing Flow
1. Discover all CSV files in the input directory
2. Apply schema-driven column normalization
3. Normalize values by declared data type
4. Add lineage metadata
5. Concatenate all normalized records into a single dataset
6. Write output as a Parquet file using PyArrow

### Output
The final output is a single Parquet file containing all normalized records.

### How to Run
    python src/pipeline.py


`

