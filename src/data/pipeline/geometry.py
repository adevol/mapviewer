"""
Geometry processing module for the MapViewer pipeline.

Handles shapefile loading, TopoJSON-based simplification (preserving shared boundaries),
coordinate rounding for file size optimization, and GeoJSON output.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping

from src.data.config import SIMPLIFY_TOLERANCE

logger = logging.getLogger(__name__)


def simplify_with_topology(gdf: gpd.GeoDataFrame, level: str) -> gpd.GeoDataFrame:
    """Simplifies geometries using standard GEOS simplification.

    Note:
        TopoJSON implementation was removed due to OOM issues.
        This is now an alias for simplify_fast().

    Args:
        gdf: GeoDataFrame with geometries to simplify.
        level: Geographic level (determines simplification tolerance).

    Returns:
        GeoDataFrame with simplified geometries.
    """
    logger.debug(f"Simplifying {level} (using fast approach)...")
    return simplify_fast(gdf, level)


def simplify_fast(gdf: gpd.GeoDataFrame, level: str) -> gpd.GeoDataFrame:
    """Simplifies geometries using GEOS topology-preserving simplification.

    Args:
        gdf: GeoDataFrame with geometries to simplify.
        level: Geographic level key for SIMPLIFY_TOLERANCE lookup.

    Returns:
        GeoDataFrame with simplified geometries.
    """
    tolerance = SIMPLIFY_TOLERANCE.get(level, 0.001)
    logger.debug(f"Fast simplifying {level}: tolerance={tolerance}m")

    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.simplify(tolerance, preserve_topology=True)
    return gdf


def round_coordinates(geom: Dict[str, Any], precision: int = 5) -> Dict[str, Any]:
    """Recursively rounds coordinates in a GeoJSON geometry dictionary.

    Args:
        geom: GeoJSON geometry dict with 'coordinates' key.
        precision: Decimal places to round to (default 5 = ~1m accuracy).

    Returns:
        New geometry dict with rounded coordinates.
    """
    if not geom:
        return geom

    def _round_coords(coords):
        if not coords:
            return coords
        # If it's a point (list of floats)
        if isinstance(coords[0], (int, float)):
            return [round(c, precision) for c in coords]
        # Recursively handle lists (LineString, Polygon, MultiPolygon)
        return [_round_coords(c) for c in coords]

    new_geom = geom.copy()
    new_geom["coordinates"] = _round_coords(geom["coordinates"])
    return new_geom


def save_geojson(gdf: gpd.GeoDataFrame, output_path: Path, precision: int = 5) -> None:
    """Saves GeoDataFrame to GeoJSON with optimizations for file size.

    Applies coordinate rounding and minimal whitespace formatting.
    Converts CRS to WGS84 (EPSG:4326) if needed.

    Args:
        gdf: GeoDataFrame to save.
        output_path: Path for the output GeoJSON file.
        precision: Decimal places for coordinate rounding.
    """
    logger.info(f"Saving {output_path.name}...")

    # Convert to WGS84 if not already
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    features = []
    for _, row in gdf.iterrows():
        if row.geometry is None or row.geometry.is_empty:
            continue

        geom_json = mapping(row.geometry)
        geom_rounded = round_coordinates(geom_json, precision)

        props = row.drop("geometry").to_dict()
        props = {k: v for k, v in props.items() if pd.notna(v)}

        feature = {"type": "Feature", "properties": props, "geometry": geom_rounded}
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  -> Saved {len(features)} features, {size_mb:.1f} MB")


def load_and_simplify(
    shp_path: Path, code_field: str, name_field: str, level: str
) -> gpd.GeoDataFrame:
    """Loads a shapefile, renames columns, and simplifies geometries.

    Standardizes column names to 'code', 'name', 'geometry' for
    consistent downstream processing.

    Args:
        shp_path: Path to the input shapefile.
        code_field: Source column name for the code/id.
        name_field: Source column name for the name.
        level: Geographic level for simplification tolerance.

    Returns:
        GeoDataFrame with standardized columns and simplified geometries.
    """
    logger.info(f"Processing {shp_path.name} ({level})...")

    gdf = gpd.read_file(shp_path)

    # Choose simplification strategy
    # TopoJSON is memory intensive. Use standard simplification for large features
    if level in ("region", "departement", "country"):
        gdf = simplify_fast(gdf, level)
    else:
        gdf = simplify_with_topology(gdf, level)

    gdf = gdf.rename(columns={code_field: "code", name_field: "name"})
    gdf = gdf[["code", "name", "geometry"]]

    return gdf
