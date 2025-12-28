# Data Pipeline Documentation

## Overview

This document describes the MapViewer data pipeline, including data sources, transformations, and known data quality issues.

## Data Sources

### DVF (Demandes de Valeurs Foncières)
- **Description**: French real estate transaction records from the government
- **Source**: `data/raw_data/valeursfoncieres-*.txt.zip`
- **Coverage**: 2020 S2 - 2025 S1 (~4.5 years)
- **Contents**: Transaction details including price, surface, location, property type

### Admin Express
- **Description**: Administrative boundaries (regions, departments, communes)
- **Source**: IGN Admin Express 2025
- **Version**: `ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03`

---

## Data Quality Issue: Multi-Lot Transactions

### Problem Description

DVF records **bulk building sales** (e.g., entire apartment buildings) with the **total transaction price on each lot row**.

**Example - Villeron bulk sale:**

| Row | Property | Price (€) | Surface (m²) | Calculated €/m² |
|-----|----------|-----------|--------------|-----------------|
| 1 | Apartment | 42,048,908 | 23 | 1,828,213 ❌ |
| 2 | Apartment | 42,048,908 | 24 | 1,752,038 ❌ |
| 3 | Apartment | 42,048,908 | 28 | 1,501,747 ❌ |
| ... | ... | ... | ... | ... |

The €42M is the **total building price**, not the individual apartment price. Dividing by each apartment's surface gives absurd €1.8M/m² values.

### Solution: Deduplicate by Mutation ID

The `dvf_clean` table groups transactions by `Identifiant de document` (mutation ID) and **sums surfaces**:

```sql
SELECT
    "Identifiant de document" AS mutation_id,
    MAX("Valeur fonciere") AS price,        -- Total transaction price
    SUM("Surface reelle bati") AS total_surface,  -- Sum all lot surfaces
    MAX("Valeur fonciere") / SUM("Surface reelle bati") AS price_m2
FROM dvf
GROUP BY "Identifiant de document", ...
```

**Corrected result:**
- Price: €42,048,908
- Total Surface: ~800 m² (sum of all apartments)
- Correct €/m²: ~52,561 (still high, but realistic for a bulk investment)

### Implementation

The fix is implemented in `src/data/etl.py`:

```python
def create_dvf_clean(con):
    """Creates cleaned DVF table with deduplicated multi-lot transactions."""
    con.execute("""
        CREATE TABLE dvf_clean AS
        SELECT
            "Identifiant de document" AS mutation_id,
            MAX("Valeur fonciere") AS price,
            SUM("Surface reelle bati") AS total_surface,
            COUNT(*) AS n_lots,
            MAX("Valeur fonciere") / NULLIF(SUM("Surface reelle bati"), 0) AS price_m2
        FROM dvf
        WHERE "Nature mutation" = 'Vente'
        AND "Type local" IN ('Maison', 'Appartement')
        GROUP BY "Identifiant de document", ...
    """)
```

### Tables

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
5. **Convert shapefiles** → `src/frontend/*.geojson`
6. **Precompute stats** → `stats_cache.json`

### Step 3: Split (`--step split`)
7. **Split communes by department** → `src/frontend/communes/*.geojson`
