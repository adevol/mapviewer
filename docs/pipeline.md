# Data Pipeline Documentation

## Overview

This document describes the MapViewer data pipeline, including data sources, transformations, and known data quality issues.

## Data Sources

### DVF (Demandes de Valeurs Foncières)
- **Description**: French real estate transaction records from the government
- **Source**: `data/raw_data/valeursfoncieres-*.txt.zip`
- **Coverage**: 2020 S2 - 2025 S1 (~4.5 years)
- **Contents**: Transaction details including price, surface, location, property type
- **Filters**:
  - `price_m2` between **100 €** and **50,000 €** (defined in `config.py`)
  - `nature_mutation` = 'Vente'
  - Standard types: 'Maison', 'Appartement' only

### Admin Express
- **Description**: Administrative boundaries (regions, departments, communes)
- **Source**: IGN Admin Express 2025
- **Version**: `ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03`

---

## Data Quality Issues

### Issue 1: Multi-Lot Transactions

DVF records **bulk building sales** with the **total transaction price on each lot row**.

**Example - Villeron bulk sale:**

| Row | Property | Price (€) | Surface (m²) | Calculated €/m² |
|-----|----------|-----------|--------------|-----------------|
| 1 | Apartment | 42,048,908 | 23 | 1,828,213 ❌ |
| 2 | Apartment | 42,048,908 | 24 | 1,752,038 ❌ |
| ... | ... | ... | ... | ... |

The €42M is the **total building price**, not the individual apartment price.

### Issue 2: Missing Mutation ID

The `"Identifiant de document"` column is **always NULL** in DVF data (all 20M+ rows). This field was intended to group lots within the same transaction, but cannot be used.

### Solution: Synthetic Transaction ID

The `dvf_clean` table uses a **synthetic transaction ID** built from multiple fields:

```sql
"Date mutation" || '|' || 
"Code departement" || '|' ||
LPAD(CAST("Code commune" AS VARCHAR), 3, '0') || '|' ||
"No disposition" || '|' ||
CAST("Valeur fonciere" AS VARCHAR) AS mutation_id
```

Transactions are grouped by:
- Date, Department, Commune, Disposition number, Price, Postal code, Commune name, Property type

This correctly aggregates multi-lot sales while preserving distinct transactions.

---

## Price Filtering

Outlier filtering is applied at different pipeline stages:

### ETL Stage (`dvf_clean` table)
```sql
WHERE "Valeur fonciere" > 0
AND "Surface reelle bati" > 0
AND "Type local" IN ('Maison', 'Appartement')
```

### Precompute Stage (statistics calculation)
```sql
WHERE price_m2 > 100     -- Exclude unrealistic low prices
AND price_m2 < 50000     -- Exclude extreme luxury/errors
```

| Threshold | Value | Rationale |
|-----------|-------|-----------|
| Minimum | 100 €/m² | Below this is likely data error or special sale |
| Maximum | 50,000 €/m² | Above this is extreme luxury or bulk sale error |

---

## Tables

| Table | Description |
|-------|-------------|
| `dvf` | Raw DVF data (one row per lot) |
| `dvf_clean` | Cleaned data (one row per unique transaction) |

**Always use `dvf_clean` for price statistics.**

---

## Pipeline Steps

Run the full pipeline with:
```bash
uv run python -m src.data.pipeline
```

Or run individual steps:
```bash
uv run python -m src.data.pipeline --step etl        # Steps 1-3
uv run python -m src.data.pipeline --step precompute # Steps 4-5
uv run python -m src.data.pipeline --step split      # Step 6
```

### Step 1: ETL (`--step etl`)
1. **Extract DVF zips** → `data/dvf_extracted/`
2. **Ingest raw DVF** → `dvf` table
3. **Create cleaned DVF** → `dvf_clean` table (deduplicates multi-lot)
4. **Download Admin Express** → `data/admin_express/`

### Step 2: Precompute (`--step precompute`)
5. **Geometry Processing** (`src.data.pipeline.geometry`)
   - Generates simplified GeoJSONs for all levels
   - **Optimization**: Coordinates rounded to 5 decimals (~1m precision)
   - **Optimization**: Standard GEOS simplification (fast, memory efficient)
   - **Features**: Merges Paris/Lyon/Marseille arrondissements for better granularity
6. **Statistics** (`src.data.pipeline.stats`)
   - SQL-based aggregation for speed
   - Computes weighted median price, Q25, Q75
   - Aggregates up from Commune -> Canton -> Region -> Country

### Step 3: Split (`--step split`)
7. **Split communes by department** → `src/frontend/communes/*.geojson`
