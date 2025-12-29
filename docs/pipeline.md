# Data Pipeline Documentation

## Overview

This document describes the MapViewer data pipeline, including data sources, transformations, and known data quality issues.

## Data Sources

### DVF (Demandes de Valeurs Foncières)

| Field | Value |
|-------|-------|
| **Description** | French real estate transaction records |
| **Source** | [data.gouv.fr/datasets/demandes-de-valeurs-foncieres](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/) |
| **Coverage** | 2020 S2 - 2025 S1 (approx 5 years) |
| **Filters Applied** | `price_m2` between 100€ and 50,000€ |
| | `nature_mutation` = 'Vente' |
| | Property types: 'Maison', 'Appartement' only |

### Admin Express (Administrative Boundaries)

| Field | Value |
|-------|-------|
| **Description** | Official French administrative boundaries |
| **Source** | [geoservices.ign.fr](https://geoservices.ign.fr/telechargement-api/ADMIN-EXPRESS-COG?zone=FRA) |
| **Version** | ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03 |
| **Levels** | Regions, Departments, Cantons, Communes, Arrondissements |

### Cadastral Parcels

| Field | Value |
|-------|-------|
| **Description** | Property parcel boundaries |
| **Source (visualization)** | IGN WMTS: `data.geopf.fr/wmts` |
| **Source (data)** | [cadastre.data.gouv.fr](https://cadastre.data.gouv.fr/datasets/cadastre-etalab) |

> [!NOTE]
> Parcel visualization uses IGN's WMTS tile service for simplicity. Self-hosted MVT tiles with price coloring are a future enhancement (see `generate_tiles.py`).

---

## Data Quality Issues

### Issue 1: Multi-Lot Transactions

DVF records **bulk building sales** with the **total transaction price on each lot row**.

**Example - bulk sale:**

| Row | Property | Price (€) | Surface (m²) | Calculated €/m² |
|-----|----------|-----------|--------------|-----------------|
| 1 | Apartment | 42,048,908 | 23 | 1,828,213 |
| 2 | Apartment | 42,048,908 | 24 | 1,752,038 |
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

## Database Tables

| Table | Description |
|-------|-------------|
| `dvf` | Raw DVF data (one row per lot) |
| `dvf_clean` | Cleaned data (one row per unique transaction) |
| `parcels` | View over cadastre.parquet (external query) |

> [!IMPORTANT]
> **Always use `dvf_clean` for price statistics** - the raw `dvf` table contains duplicate prices as explained in the [Data Quality Issues](#data-quality-issues) section.

---

## Pipeline Steps

Run the full pipeline:
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
   - Coordinates rounded to 5 decimals (~1m precision)
   - Standard GEOS simplification (fast, memory efficient)
   - Merges Paris/Lyon/Marseille arrondissements for better granularity

6. **Statistics** (`src.data.pipeline.stats`)
   - SQL-based aggregation for speed
   - Computes weighted median price, Q25, Q75
   - Aggregates up from Commune → Canton → Department → Region → Country

### Step 3: Split (`--step split`)

7. **Split communes by department** → `src/frontend/communes/*.geojson`
   - Enables lazy loading on the frontend
   - Only loads departments visible in viewport

---

## Output Files

| File | Location | Description |
|------|----------|-------------|
| `country.geojson` | `src/frontend/` | Country outline with stats |
| `regions.geojson` | `src/frontend/` | 18 regions with stats |
| `departements.geojson` | `src/frontend/` | 101 departments with stats |
| `cantons.geojson` | `src/frontend/` | ~2,000 cantons with stats |
| `{dept}.geojson` | `src/frontend/communes/` | Per-department commune files |
| `stats_cache.json` | `src/frontend/` | All-level stats cache |
| `top_expensive.json` | `src/frontend/` | Top 10 expensive communes |

---

## Configuration

All pipeline configuration is centralized in `src/data/config.py`:

- **Paths**: Data directories, file locations
- **Simplification tolerances**: Per-level geometry simplification
- **Field mappings**: Admin Express column names
- **Price thresholds**: Min/max €/m² for outlier filtering
- **Department → Region mapping**: 2016 region boundaries

---

## Future Work

The following modules are preserved for future benchmarking:

| Module | Purpose |
|--------|---------|
| `generate_tiles.py` | tippecanoe-based PMTiles generation |
| `precompute_parcels.py` | Parcel extraction with DVF join |

These would enable:
- Self-hosted vector tiles with custom parcel styling
- Per-parcel price coloring (vs. commune-level choropleth)
- Hover tooltips on individual properties
