"""
[FUTURE WORK] Vector tile generation using tippecanoe.

This module is NOT currently used in the production pipeline.
The map uses IGN's WMTS service for cadastral visualization instead.

Why this was deferred:
- Self-hosted MVT requires tippecanoe (Linux/WSL only)
- Cadastre parquet is ~21GB, generates large tile files
- Latency issues with real-time MVT generation from DuckDB
- IGN WMTS provides immediate value without infrastructure

To enable in future:
1. Define missing config variables (PARCELS_GEOJSON_DIR, TILES_OUTPUT_DIR, etc.)
2. Run precompute_parcels.py to generate parcel GeoJSONs
3. Install tippecanoe in WSL
4. Run: python -m src.data.generate_tiles

Converts enriched GeoJSON to PMTiles for each aggregation level.
Non-parcel layers are combined into a single tileset; parcels are separate.
"""

import json
import logging
import os
import platform
import subprocess
from pathlib import Path

from .config import (
    OUTPUT_DIR,
    PARCELS_GEOJSON_DIR,
    POSTCODE_FILE,
    POSTCODE_OUTPUT,
    STATS_OUTPUT,
    TILES_OUTPUT_DIR,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Check if running on Windows (need to use WSL for tippecanoe)
IS_WINDOWS = platform.system() == "Windows"
SKIP_PARCELS = os.getenv("SKIP_PARCELS_TILES", "").lower() in ("1", "true", "yes")


# Layer configurations: (geojson_name, layer_name, min_zoom, max_zoom)
TILE_LAYERS = [
    ("country.geojson", "country", 0, 5),
    ("regions.geojson", "regions", 5, 7),
    ("departements.geojson", "departments", 7, 10),
    ("postcodes.geojson", "postcodes", 10, 12),
    ("communes.geojson", "communes", 12, 15),
]


def to_wsl_path(windows_path: Path) -> str:
    """Converts a Windows path to WSL path format.

    Example: C:\\Users\\alexa\\project -> /mnt/c/Users/alexa/project
    """
    path_str = str(windows_path.resolve())
    # Replace backslashes with forward slashes
    path_str = path_str.replace("\\", "/")
    # Convert drive letter (C:) to /mnt/c
    if len(path_str) >= 2 and path_str[1] == ":":
        drive = path_str[0].lower()
        path_str = f"/mnt/{drive}{path_str[2:]}"
    return path_str


def run_tippecanoe(
    input_files: list[Path],
    output_file: Path,
    layer_name: str = None,
    min_zoom: int = 0,
    max_zoom: int = 14,
    extra_args: list[str] = None,
) -> None:
    """Runs tippecanoe to generate PMTiles from GeoJSON.

    On Windows, runs through WSL using a temp directory in the Linux filesystem
    to avoid SQLite locking issues on NTFS, then copies the result back.

    Args:
        input_files: List of GeoJSON files to process.
        output_file: Output PMTiles file path.
        layer_name: Name for the layer in the tileset.
        min_zoom: Minimum zoom level.
        max_zoom: Maximum zoom level.
        extra_args: Additional tippecanoe arguments.
    """
    if IS_WINDOWS:
        # Use WSL temp directory to avoid SQLite locking issues on NTFS
        wsl_temp_output = f"/tmp/{output_file.name}"
        output_path_str = wsl_temp_output
        input_paths = [to_wsl_path(f) for f in input_files]
        final_output = to_wsl_path(output_file)
    else:
        output_path_str = str(output_file)
        input_paths = [str(f) for f in input_files]
        wsl_temp_output = None

    tippecanoe_cmd = [
        "tippecanoe",
        "-o",
        output_path_str,
        "-Z",
        str(min_zoom),
        "-z",
        str(max_zoom),
        "--force",  # Overwrite existing
        "--read-parallel",
        "--drop-densest-as-needed",  # Auto-simplify at low zooms
        "--extend-zooms-if-still-dropping",
    ]

    if layer_name:
        tippecanoe_cmd.extend(["--layer", layer_name])

    if extra_args:
        tippecanoe_cmd.extend(extra_args)

    tippecanoe_cmd.extend(input_paths)

    # Wrap in WSL on Windows
    if IS_WINDOWS:
        cmd = ["wsl"] + tippecanoe_cmd
    else:
        cmd = tippecanoe_cmd

    logger.info(f"Running: {' '.join(cmd[:12])}...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"tippecanoe failed: {result.stderr}")
        raise RuntimeError(f"tippecanoe failed: {result.stderr}")

    # On Windows, copy from WSL temp to final destination
    if IS_WINDOWS and wsl_temp_output:
        copy_cmd = ["wsl", "cp", "-f", wsl_temp_output, final_output]
        copy_result = subprocess.run(copy_cmd, capture_output=True, text=True)
        if copy_result.returncode != 0:
            logger.error(f"Copy failed: {copy_result.stderr}")
            raise RuntimeError(f"Copy failed: {copy_result.stderr}")
        # Clean up temp file
        subprocess.run(["wsl", "rm", "-f", wsl_temp_output], capture_output=True)

    size_mb = output_file.stat().st_size / (1024 * 1024)
    logger.info(f"  -> {output_file.name}: {size_mb:.1f} MB")


def enrich_postcodes_with_stats() -> None:
    """Enriches postcode GeoJSON with price statistics.

    Loads stats from stats_cache.json and adds to postcode features.
    """
    if not POSTCODE_FILE.exists():
        logger.warning("Postcode file not found, skipping enrichment")
        return

    if POSTCODE_OUTPUT.exists():
        logger.info("Enriched postcodes already exist")
        return

    logger.info("Enriching postcodes with stats...")

    # Load stats
    with open(STATS_OUTPUT, "r", encoding="utf-8") as f:
        stats = json.load(f)
    postcode_stats = stats.get("postcode", {})

    # Load postcode GeoJSON
    with open(POSTCODE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Enrich features
    enriched_count = 0
    for feature in data["features"]:
        # The postcode field name may vary - check common names
        code = (
            feature["properties"].get("code_postal")
            or feature["properties"].get("postal_code")
            or feature["properties"].get("code")
        )

        if code and code in postcode_stats:
            s = postcode_stats[code]
            feature["properties"]["price_m2"] = s["median_price_m2"]
            feature["properties"]["n_sales"] = s["n_sales"]
            feature["properties"]["q25"] = s.get("q25")
            feature["properties"]["q75"] = s.get("q75")
            enriched_count += 1

    logger.info(f"  Enriched {enriched_count}/{len(data['features'])} postcodes")

    # Save enriched GeoJSON
    with open(POSTCODE_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def generate_layer_tiles() -> None:
    """Generates individual PMTiles for each geographic layer."""
    TILES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for geojson_name, layer_name, min_z, max_z in TILE_LAYERS:
        geojson_path = OUTPUT_DIR / geojson_name
        output_path = TILES_OUTPUT_DIR / f"{layer_name}.pmtiles"

        if not geojson_path.exists():
            logger.warning(f"GeoJSON not found: {geojson_path}")
            continue

        if output_path.exists():
            logger.info(f"Tiles already exist: {output_path.name}")
            continue

        logger.info(f"Generating tiles for {layer_name}...")
        run_tippecanoe(
            input_files=[geojson_path],
            output_file=output_path,
            layer_name=layer_name,
            min_zoom=min_z,
            max_zoom=max_z,
        )


def generate_parcel_tiles() -> None:
    """Generates PMTiles for sold parcels.

    Parcels are processed per department then merged.
    Only includes parcels that have DVF transactions.
    """
    if not PARCELS_GEOJSON_DIR.exists():
        logger.warning("Parcels GeoJSON directory not found, skipping")
        return

    parcel_files = list(PARCELS_GEOJSON_DIR.glob("*.geojson"))
    if not parcel_files:
        logger.warning("No parcel GeoJSON files found")
        return

    output_path = TILES_OUTPUT_DIR / "parcels.pmtiles"
    if output_path.exists():
        logger.info("Parcel tiles already exist")
        return

    logger.info(f"Generating parcel tiles from {len(parcel_files)} files...")

    # Generate tiles from all parcel files at once
    # tippecanoe can handle multiple input files
    run_tippecanoe(
        input_files=parcel_files,
        output_file=output_path,
        layer_name="parcels",
        min_zoom=16,
        max_zoom=20,
        extra_args=[
            "--no-feature-limit",
            "--no-tile-size-limit",
        ],
    )


def merge_all_tiles() -> None:
    """Merges all non-parcel layer tiles into a single PMTiles file.

    This allows the frontend to load a single source.
    On Windows, runs tile-join through WSL.
    """
    output_path = TILES_OUTPUT_DIR / "mapviewer.pmtiles"

    tile_files = list(TILES_OUTPUT_DIR.glob("*.pmtiles"))
    tile_files = [
        f for f in tile_files if f.name not in {"mapviewer.pmtiles", "parcels.pmtiles"}
    ]

    if not tile_files:
        logger.warning("No tile files to merge")
        return

    if output_path.exists():
        logger.info("Merged tiles already exist")
        return

    logger.info(f"Merging {len(tile_files)} tile files...")

    # Build tile-join command with path conversion for WSL
    if IS_WINDOWS:
        output_str = to_wsl_path(output_path)
        input_strs = [to_wsl_path(f) for f in tile_files]
    else:
        output_str = str(output_path)
        input_strs = [str(f) for f in tile_files]

    tile_join_cmd = [
        "tile-join",
        "-o",
        output_str,
        "--force",
        *input_strs,
    ]

    if IS_WINDOWS:
        cmd = ["wsl"] + tile_join_cmd
    else:
        cmd = tile_join_cmd

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"tile-join failed: {result.stderr}")
        raise RuntimeError(f"tile-join failed: {result.stderr}")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  -> mapviewer.pmtiles: {size_mb:.1f} MB")


def main() -> None:
    """Main tile generation pipeline."""
    logger.info("Starting tile generation...")

    # Ensure output directory exists
    TILES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Enrich postcodes with stats
    enrich_postcodes_with_stats()

    # 2. Generate tiles for each layer
    generate_layer_tiles()

    # 3. Generate parcel tiles (if available)
    if SKIP_PARCELS:
        logger.info("Skipping parcel tiles because SKIP_PARCELS_TILES is set")
    else:
        generate_parcel_tiles()

    # 4. Merge all tiles into single file
    merge_all_tiles()

    logger.info("Tile generation complete!")


if __name__ == "__main__":
    main()
