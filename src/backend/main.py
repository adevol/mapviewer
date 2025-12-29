"""
FastAPI backend for the MapViewer project.

Provides endpoints for:
- Vector Tile (MVT) generation from DuckDB.
- Aggregated statistics for map coloring.
- Top 10 cities report.

Usage:
    uv run uvicorn src.backend.main:app --reload
"""

import math
import time
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Constants
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
    """Gets a DuckDB connection.

    Returns:
        A read-only DuckDB connection.
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    return con


@app.get("/api/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/api/debug/tile.pbf")
async def debug_tile() -> Response:
    """Serves a pre-generated valid MVT tile for debugging."""
    tile_file = Path("test_mvt_output.pbf")
    if tile_file.exists():
        with open(tile_file, "rb") as f:
            return Response(content=f.read(), media_type="application/x-protobuf")
    return Response(content=b"", media_type="application/x-protobuf")


def tile_to_bbox_2154(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert tile coordinates to bounding box in EPSG:2154 (Lambert-93).

    Uses pyproj for accurate coordinate transformation.

    Returns:
        (xmin, ymin, xmax, ymax) in EPSG:2154 coordinates.
    """
    from pyproj import Transformer

    # First get WGS84 bounds from tile coordinates
    n = 2.0**z
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))

    # Transform to Lambert-93 (EPSG:2154) using pyproj
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)
    x1, y1 = transformer.transform(lon_min, lat_min)
    x2, y2 = transformer.transform(lon_max, lat_max)

    # Add 20% buffer for safety
    dx = abs(x2 - x1) * 0.2
    dy = abs(y2 - y1) * 0.2

    return (min(x1, x2) - dx, min(y1, y2) - dy, max(x1, x2) + dx, max(y1, y2) + dy)


@app.get("/api/tiles/{z}/{x}/{y}.pbf")
async def get_tile(z: int, x: int, y: int) -> Response:
    """Generates a Mapbox Vector Tile (MVT) for the given tile coordinates.

    Uses bbox pre-filtering on geometry_bbox column for efficient queries.

    Args:
        z: Zoom level.
        x: Tile X coordinate.
        y: Tile Y coordinate.

    Returns:
        The MVT tile as binary protobuf.
    """
    try:
        # Return empty tile for low zoom levels (too many parcels)
        if z < 14:
            return Response(content=b"", media_type="application/x-protobuf")

        # Pre-compute approximate bbox in EPSG:2154 for fast filtering
        xmin, ymin, xmax, ymax = tile_to_bbox_2154(z, x, y)

        con = get_db()

        # Use geometry_bbox for fast pre-filtering before expensive ST_Intersects
        # This dramatically reduces rows scanned from 160M to just those in bbox
        query = f"""
        WITH tile_env AS (
            SELECT ST_TileEnvelope({z}, {x}, {y}) AS env
        ),
        tile_bbox AS (
            SELECT ST_Extent(env) AS bbox FROM tile_env
        ),
        bounds_2154 AS (
            SELECT ST_Transform(env, 'EPSG:3857', 'EPSG:2154') AS geom
            FROM tile_env
        ),
        -- Get commune stats for coloring (V1: uniform price per commune)
        commune_stats AS (
            SELECT
                dept_code || LPAD(CAST(commune_code AS VARCHAR), 3, '0') AS insee_com,
                APPROX_QUANTILE(price_m2, 0.5) AS median_price_m2,
                COUNT(*) AS volume
            FROM dvf_clean
            GROUP BY 1
        ),
        -- Fast bbox pre-filter using geometry_bbox column
        candidate_parcels AS (
            SELECT id, commune, departement, contenance, geometry
            FROM parcels
            WHERE geometry_bbox.xmin <= {xmax}
              AND geometry_bbox.xmax >= {xmin}
              AND geometry_bbox.ymin <= {ymax}
              AND geometry_bbox.ymax >= {ymin}
              AND geometry IS NOT NULL
            LIMIT 20000
        ),
        tile_data AS (
            SELECT
                p.id,
                p.commune,
                p.departement,
                p.contenance,
                -- Use commune median price
                COALESCE(cs.median_price_m2, 0) AS price_m2,
                COALESCE(cs.volume, 0) AS n_sales,
                ST_AsMVTGeom(
                    ST_Transform(p.geometry, 'EPSG:2154', 'EPSG:3857'),
                    (SELECT bbox FROM tile_bbox),
                    4096,
                    256,
                    true
                ) AS geom
            FROM candidate_parcels p
            CROSS JOIN bounds_2154 b
            LEFT JOIN commune_stats cs ON p.commune = cs.insee_com
            WHERE ST_Intersects(p.geometry, b.geom)
            LIMIT 10000
        )
        SELECT ST_AsMVT(tile_data, 'parcels', 4096, 'geom') AS mvt
        FROM tile_data
        WHERE geom IS NOT NULL;
        """

        result = con.execute(query).fetchone()
        con.close()

        if result is None or result[0] is None:
            return Response(content=b"", media_type="application/x-protobuf")

        mvt_bytes = bytes(result[0])
        return Response(content=mvt_bytes, media_type="application/x-protobuf")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


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
