# MapViewer - French Real Estate Price Visualization

Interactive map application displaying real estate prices (€/m²) across France using DVF (Demandes de Valeurs Foncières) public data.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the full data pipeline (first time only)
uv run python -m src.data.pipeline

# Or run individual pipeline steps
uv run python -m src.data.pipeline --step etl        # Download & ingest data
uv run python -m src.data.pipeline --step precompute # Generate GeoJSON & stats
uv run python -m src.data.pipeline --step split      # Split communes by department

# Start the server
uv run uvicorn src.backend.main:app --reload

# Open http://localhost:8000
```

## Features

- **Interactive Map**: Zoom from country level to individual parcels
- **Vector Tiles**: Dynamic MVT generation for smooth performance
- **Price Visualization**: Color-coded parcels (blue=cheap, red=expensive)
- **Glassmorphism UI**: Modern, premium design aesthetic

## Architecture

```
src/
├── data/
│   └── etl.py         # Data extraction and loading
├── backend/
│   └── main.py        # FastAPI server with MVT endpoints
└── frontend/
    ├── index.html     # Map interface
    ├── style.css      # Glassmorphism styling
    └── app.js         # MapLibre GL JS logic
```

## Data Sources

- **DVF**: [data.gouv.fr/datasets/demandes-de-valeurs-foncieres](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres/)
- **Cadastre**: [cadastre.data.gouv.fr](https://cadastre.data.gouv.fr/)
- **Admin Express**: [geoservices.ign.fr](https://geoservices.ign.fr/)

## Deployment (VPS)

```bash
docker-compose up -d
```

## License

MIT
