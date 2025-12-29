"""
FastAPI backend for the MapViewer project.

Current endpoints:
- Health check
- Aggregated statistics for map coloring
- Top 10 cities report
- Static file serving (frontend)

Usage:
    uv run uvicorn src.backend.main:app --reload
"""

import time
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Constants
STATIC_DIR = Path("src/frontend")
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "real_estate.duckdb"
CACHE_TTL_SECONDS = 3600  # 1 hour cache

# In-memory cache for expensive queries
_cache: dict[str, dict[str, Any]] = {}

app = FastAPI(
    title="MapViewer API",
    description="French Real Estate Price Visualization API",
    version="0.1.0",
)

# CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db() -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB database."""

    if not DB_PATH.exists():
        print(f"CRITICAL ERROR: Database NOT FOUND at {DB_PATH.absolute()}")
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    return duckdb.connect(str(DB_PATH), read_only=True)


@app.on_event("startup")
async def startup_event():
    print("\n--- MAPVIEWER BACKEND STARTING ---")
    print(f"DB Path: {DB_PATH.absolute()}")


@app.get("/api/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    print(">>> Health check endpoint hit! Backend is active.")
    return {"status": "ok", "db_exists": DB_PATH.exists()}


print("--- MAPVIEWER BACKEND STARTING ---")
print(f"Database: {DB_PATH.absolute()}")
print("----------------------------------")


def _compute_department_stats() -> dict[str, Any]:
    """Computes department statistics from DVF data.

    Uses APPROX_QUANTILE for faster computation.

    Returns:
        Dictionary with department codes as keys and stats as values.
    """
    con = get_db()

    # Query DVF data for department-level stats
    # Using APPROX_QUANTILE instead of MEDIAN for ~10x faster performance
    query = """
    SELECT
        "Code departement" AS dept,
        APPROX_QUANTILE("Valeur fonciere" / NULLIF("Surface reelle bati", 0), 0.5) AS median_price_m2,
        COUNT(*) AS volume
    FROM dvf
    WHERE "Nature mutation" = 'Vente'
    AND "Valeur fonciere" > 0
    AND "Surface reelle bati" > 0
    AND "Type local" IN ('Maison', 'Appartement')
    GROUP BY "Code departement"
    HAVING COUNT(*) >= 10
    ORDER BY median_price_m2 DESC;
    """

    result = con.execute(query).fetchall()
    con.close()

    stats = {}
    for row in result:
        dept, price, vol = row
        if dept:
            stats[dept] = {
                "price_m2": round(price, 2) if price else None,
                "volume": vol,
            }

    return stats


def _get_cached_department_stats() -> dict[str, Any]:
    """Gets department stats from cache or computes them.

    Returns:
        Cached or freshly computed department statistics.
    """
    cache_key = "department_stats"
    now = time.time()

    if cache_key in _cache:
        cached = _cache[cache_key]
        if now - cached["timestamp"] < CACHE_TTL_SECONDS:
            return cached["data"]

    # Compute and cache
    stats = _compute_department_stats()
    _cache[cache_key] = {"data": stats, "timestamp": now}
    return stats


@app.get("/api/stats/departments")
async def get_department_stats() -> dict[str, Any]:
    """Gets aggregated price statistics by department.

    Results are cached for 1 hour for fast responses.

    Returns:
        Dictionary with department codes as keys and stats as values.
    """
    try:
        stats = _get_cached_department_stats()
        return {"data": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/api/stats/departments/refresh")
async def refresh_department_stats() -> dict[str, str]:
    """Forces a refresh of the department stats cache."""
    try:
        _cache.pop("department_stats", None)
        _get_cached_department_stats()  # Recompute
        return {"status": "refreshed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/api/top10cities")
async def get_top10_cities() -> dict[str, Any]:
    """Gets price per m² for top 10 biggest cities by population.

    Returns:
        List of cities with their price statistics.
    """
    try:
        con = get_db()

        # Top 10 French cities by population (approximate commune codes)
        # Paris: 75, Marseille: 13055, Lyon: 69123, etc.
        query = """
        SELECT
            "Commune" AS city,
            "Code commune" AS code,
            "Type local" AS property_type,
            MEDIAN("Valeur fonciere" / NULLIF("Surface reelle bati", 0)) AS median_price_m2,
            COUNT(*) AS volume
        FROM dvf
        WHERE "Nature mutation" = 'Vente'
        AND "Valeur fonciere" > 0
        AND "Surface reelle bati" > 0
        AND "Type local" IN ('Maison', 'Appartement')
        GROUP BY "Commune", "Code commune", "Type local"
        HAVING COUNT(*) >= 50
        ORDER BY volume DESC
        LIMIT 20;
        """

        result = con.execute(query).fetchall()
        con.close()

        cities = []
        for row in result:
            city, code, prop_type, price, vol = row
            cities.append(
                {
                    "city": city,
                    "code": code,
                    "property_type": prop_type,
                    "median_price_m2": round(price, 2) if price else None,
                    "volume": vol,
                }
            )

        return {"data": cities}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# Mount static files for frontend (served at root)
frontend_path = Path("src/frontend")
if frontend_path.exists():
    app.mount(
        "/", StaticFiles(directory=str(frontend_path), html=True), name="frontend"
    )
