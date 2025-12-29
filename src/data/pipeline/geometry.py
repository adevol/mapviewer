"""
Geometry processing module for the MapViewer pipeline.

Handles shapefile loading, TopoJSON-based simplification (preserving shared boundaries),
coordinate rounding for file size optimization, and GeoJSON output.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import shape, mapping

from src.data.config import SIMPLIFY_TOLERANCE

logger = logging.getLogger(__name__)


def simplify_with_topology(gdf: gpd.GeoDataFrame, level: str) -> gpd.GeoDataFrame:
    """Simplifies geometries using standard GEOS simplification (faster, less memory).

    NOTE: TopoJSON implementation removed due to OOM issues on large datasets.
    Now just an alias for simplify_fast.
    """
    logger.debug(f"Simplifying {level} (using fast approach)...")
    return simplify_fast(gdf, level)


def simplify_fast(gdf: gpd.GeoDataFrame, level: str) -> gpd.GeoDataFrame:
    """Simplifies geometries using standard GEOS simplification (faster, less memory)."""
    tolerance = SIMPLIFY_TOLERANCE.get(level, 0.001)
    logger.debug(f"Fast simplifying {level}: tolerance={tolerance}m")

    # preserve_topology=True prevents invalid geometries (self-intersection)
    # but does NOT guarantee shared boundaries between polygons.
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.simplify(tolerance, preserve_topology=True)
    return gdf


def round_coordinates(geom: Dict[str, Any], precision: int = 5) -> Dict[str, Any]:
    """Recursively rounds coordinates in a GeoJSON geometry dictionary."""
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
    """Saves GeoDataFrame to GeoJSON with rounded coordinates and minimal whitespace."""
    logger.info(f"Saving {output_path.name}...")

    # Convert to WGS84 if not already
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Manual serialization to control precision and formatting
    features = []
    for _, row in gdf.iterrows():
        if row.geometry is None or row.geometry.is_empty:
            continue

        geom_json = mapping(row.geometry)
        geom_rounded = round_coordinates(geom_json, precision)

        props = row.drop("geometry").to_dict()
        # Remove null properties to save space
        props = {k: v for k, v in props.items() if pd.notna(v)}

        feature = {"type": "Feature", "properties": props, "geometry": geom_rounded}
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open(output_path, "w", encoding="utf-8") as f:
        # separators=(',', ':') eliminates spaces
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  → Saved {len(features)} features, {size_mb:.1f} MB")


def load_and_simplify(
    shp_path: Path, code_field: str, name_field: str, level: str
) -> gpd.GeoDataFrame:
    """Loads a shapefile, renames columns, and simplifies."""
    logger.info(f"Processing {shp_path.name} ({level})...")

    gdf = gpd.read_file(shp_path)

    # Choose simplification strategy
    # TopoJSON is memory intensive. Use standard simplification for large features
    # (regions/departments) to avoid OOM.
    if level in ("region", "departement", "country"):
        gdf = simplify_fast(gdf, level)
    else:
        gdf = simplify_with_topology(gdf, level)

    # Standardize columns
    gdf = gdf.rename(columns={code_field: "code", name_field: "name"})
    gdf = gdf[["code", "name", "geometry"]]

    return gdf
