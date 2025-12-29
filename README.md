# MapViewer - French Real Estate Price Visualization

Interactive map showing real estate prices (€/m²) across France using DVF (Demandes de Valeurs Foncières) open data.

## Data Setup

Before running the pipeline, download DVF data (last 5 years recommended):

1. Visit [DVF on data.gouv.fr](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/)
2. Download the annual CSV files (e.g., `valeursfoncieres-2024.txt`, `valeursfoncieres-2023.txt`, etc.)
3. Place them in `data_raw/`:

```
data_raw/
├── valeursfoncieres-2024.txt
├── valeursfoncieres-2023.txt
├── valeursfoncieres-2022.txt
├── valeursfoncieres-2021.txt
└── valeursfoncieres-2020.txt
```

> **Note**: Each file is ~1-2 GB. The pipeline will merge all files during ETL.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the full data pipeline (first time only, ~10 min)
uv run python -m src.data.pipeline

# Start the server
uv run uvicorn src.backend.main:app --reload

# Open http://localhost:8000
```

## Features

- **Multi-level choropleth**: Zoom from country → regions → departments → cantons → communes
- **Price visualization**: Color-coded areas (green = affordable, red = expensive)
- **Cadastral parcels**: IGN parcel boundaries visible at zoom 14+
- **Glassmorphism UI**: Modern, premium design aesthetic
- **Fast loading**: Pre-computed stats + per-department GeoJSON splitting

## Architecture

```
src/
├── data/
│   ├── pipeline/           # Modular data pipeline
│   │   ├── geometry.py     # Geometry simplification
│   │   ├── stats.py        # SQL-based statistics
│   │   └── main.py         # Orchestrator
│   ├── etl.py              # DVF ingestion + cleaning
│   ├── split_communes.py   # Per-department splitting
│   ├── config.py           # Centralized configuration
│   ├── generate_tiles.py   # [FUTURE] PMTiles generation
│   └── precompute_parcels.py # [FUTURE] Parcel extraction
├── backend/
│   └── main.py             # FastAPI server
└── frontend/
    ├── index.html          # Map interface
    ├── style.css           # Glassmorphism styling
    ├── app.js              # MapLibre GL JS logic
    ├── communes/           # Per-department GeoJSON files
    └── *.geojson           # Pre-computed admin boundaries
```

## Data Sources

| Source | Description | Usage |
|--------|-------------|-------|
| [DVF](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/) | Real estate transactions | Price statistics |
| [Admin Express](https://geoservices.ign.fr/) | Administrative boundaries | Choropleth geometries |
| [IGN WMTS](https://data.geopf.fr/) | Cadastral parcels | Parcel visualization |

## Visualization Approach

### Why IGN WMTS for Parcels?

The map uses **IGN's WMTS tile service** for cadastral parcel visualization rather than self-hosted vector tiles:

| Approach | Pros | Cons |
|----------|------|------|
| **IGN WMTS** | Zero hosting, always up-to-date, no preprocessing | No custom styling, no hover interaction |
| **Self-hosted MVT** | Custom price coloring, hover tooltips | Requires tile server, >20GB parquet, latency issues |

For a technical test, leveraging France's official geodata service provides immediate value without infrastructure overhead. The MVT approach is preserved in `generate_tiles.py` and `precompute_parcels.py` for future benchmarking.

### Choropleth Coloring

Admin boundaries (country → communes) use a choropleth with per-area median €/m²:

```
Green  = 1,000 €/m² (affordable)
Yellow = 4,000 €/m²
Orange = 7,000 €/m²
Red    = 12,000 €/m² (expensive)
```

## Pipeline Steps

```bash
# Run all steps
uv run python -m src.data.pipeline

# Or individual steps:
uv run python -m src.data.pipeline --step etl        # Download + ingest data
uv run python -m src.data.pipeline --step precompute # Generate GeoJSON + stats
uv run python -m src.data.pipeline --step split      # Split communes by department
```

See [docs/pipeline.md](docs/pipeline.md) for detailed documentation on data quality handling.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Frontend application |
| `GET /api/health` | Health check |
| `GET /api/stats/departments` | Department-level price stats |
| `GET /api/top10cities` | Top cities by transaction volume |

## Deployment

```bash
docker-compose up -d
```

## Future Work

- [ ] Self-hosted MVT tiles for parcel-level price coloring
- [ ] Hover tooltips on individual parcels
- [ ] k-nearest neighbor price interpolation
- [ ] Time-series price evolution

## License

MIT
