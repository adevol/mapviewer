"""
Precompute statistics pipeline for MapViewer.

This script:
1. Converts ADMIN EXPRESS COG shapefiles to GeoJSON (WGS84)
2. Precomputes price statistics for each geographic level
3. Outputs simplified GeoJSON + stats JSON for fast serving

Usage:
    python -m src.data.precompute
"""

import json
import logging

import duckdb
import geopandas as gpd
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from .config import (
    ARRONDISSEMENT_TO_COMMUNE,
    CODE_FIELDS,
    DB_PATH,
    DEPT_TO_REGION,
    INSEE_COMMUNE_EXPR,
    MIN_SALES_FOR_STATS,
    NAME_FIELDS,
    OUTPUT_DIR,
    SHAPEFILE_PATHS,
    SIMPLIFY_TOLERANCE,
    STATS_OUTPUT,
    VALID_PROPERTY_TYPES,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_db() -> duckdb.DuckDBPyConnection:
    """Gets a DuckDB connection with spatial extension and memory limits.

    Limits memory usage to prevent OOM on systems with limited RAM.
    Uses disk-based temp storage for large aggregations.
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    # Limit memory to 8GB to prevent OOM on 16GB systems
    con.execute("SET memory_limit = '8GB';")
    # Use disk for temp storage when memory limit is reached
    con.execute("SET temp_directory = 'data/temp';")
    return con


def convert_shapefile_to_geojson(
    shp_path: Path, output_path: Path, code_field: str, name_field: str, level: str
) -> None:
    """Converts a shapefile to simplified GeoJSON with WGS84 projection.

    Args:
        shp_path: Path to input shapefile.
        output_path: Path to output GeoJSON.
        code_field: Field name for geographic code.
        name_field: Field name for geographic name.
        level: Geographic level for determining simplification tolerance.
    """
    if output_path.exists():
        logger.info(f"GeoJSON already exists: {output_path.name}")
        return

    logger.info(f"Converting {shp_path.name} to GeoJSON...")

    gdf = gpd.read_file(shp_path)

    # Simplify in native CRS (Lambert 93 = meters)
    tolerance = SIMPLIFY_TOLERANCE.get(level, 30)
    gdf["geometry"] = gdf.geometry.simplify(tolerance, preserve_topology=True)

    # Reproject to WGS84
    gdf = gdf.to_crs(epsg=4326)

    # Select and rename fields
    gdf = gdf[[code_field, name_field, "geometry"]].copy()
    gdf.columns = ["code", "name", "geometry"]

    # Remove empty geometries
    gdf = gdf[~gdf.geometry.is_empty]

    # Write GeoJSON
    gdf.to_file(output_path, driver="GeoJSON")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  → {output_path.name}: {len(gdf)} features, {size_mb:.1f} MB")


def convert_cantons_to_geojson() -> None:
    """Converts canton shapefile to GeoJSON with DEP_CAN code format.

    Canton shapefile has INSEE_CAN (canton number) and INSEE_DEP (department).
    We combine them as DEP_CAN (e.g., "01_05") to match the stats cache keys.

    Also adds pseudo-cantons for Paris, Lyon, and Marseille which don't have
    real cantons but have arrondissements.
    """
    output_path = OUTPUT_DIR / "cantons.geojson"

    if output_path.exists():
        logger.info("cantons.geojson already exists")
        return

    canton_shp = SHAPEFILE_PATHS.get("canton")
    if not canton_shp or not canton_shp.exists():
        logger.warning("Canton shapefile not found")
        return

    logger.info("Converting cantons to GeoJSON...")

    gdf = gpd.read_file(canton_shp)

    # Simplify in native CRS (Lambert 93 = meters)
    tolerance = SIMPLIFY_TOLERANCE.get("canton", 100)
    gdf["geometry"] = gdf.geometry.simplify(tolerance, preserve_topology=True)

    # Reproject to WGS84
    gdf = gdf.to_crs(epsg=4326)

    # Create code as DEP_CAN to match stats cache keys
    gdf["code"] = gdf["INSEE_DEP"].astype(str) + "_" + gdf["INSEE_CAN"].astype(str)
    gdf["name"] = "Canton " + gdf["INSEE_CAN"].astype(str)

    # Select fields
    gdf = gdf[["code", "name", "geometry"]].copy()

    # Add pseudo-cantons for Paris, Lyon, Marseille (they don't have real cantons)
    commune_shp = SHAPEFILE_PATHS.get("commune")
    if commune_shp and commune_shp.exists():
        communes = gpd.read_file(commune_shp)
        communes = communes.to_crs(epsg=4326)

        # Paris (75056), Lyon (69123), Marseille (13055)
        plm_codes = {
            "75056": ("75_PARIS", "Paris"),
            "69123": ("69_LYON", "Lyon"),
            "13055": ("13_MARSEILLE", "Marseille"),
        }

        for insee_com, (code, name) in plm_codes.items():
            plm = communes[communes["INSEE_COM"] == insee_com]
            if not plm.empty:
                plm_row = gpd.GeoDataFrame(
                    {
                        "code": [code],
                        "name": [name],
                        "geometry": [plm.geometry.iloc[0]],
                    },
                    crs=gdf.crs,
                )
                gdf = gpd.GeoDataFrame(
                    pd.concat([gdf, plm_row], ignore_index=True), crs=gdf.crs
                )
                logger.info(f"  Added pseudo-canton: {name}")

    # Remove empty geometries
    gdf = gdf[~gdf.geometry.is_empty]

    # Write GeoJSON
    gdf.to_file(output_path, driver="GeoJSON")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  → cantons.geojson: {len(gdf)} features, {size_mb:.1f} MB")


def convert_communes_with_arrondissements() -> None:
    """Converts communes shapefile to GeoJSON with arrondissements merged inline.

    For Paris, Lyon, and Marseille, replaces the parent commune geometry with
    individual arrondissement geometries for granular display.
    """
    output_path = OUTPUT_DIR / "communes.geojson"

    if output_path.exists():
        logger.info("communes.geojson already exists")
        return

    logger.info("Converting communes with arrondissements merged...")

    # Load communes shapefile
    communes_shp = SHAPEFILE_PATHS["commune"]
    if not communes_shp.exists():
        logger.error(f"Communes shapefile not found: {communes_shp}")
        return

    communes_gdf = gpd.read_file(communes_shp)

    # Load arrondissements shapefile
    arr_shp = SHAPEFILE_PATHS["arrondissement"]
    if arr_shp.exists():
        arr_gdf = gpd.read_file(arr_shp)

        # Simplify arrondissements
        arr_tolerance = SIMPLIFY_TOLERANCE.get("arrondissement", 50)
        arr_gdf["geometry"] = arr_gdf.geometry.simplify(
            arr_tolerance, preserve_topology=True
        )

        # Rename fields
        arr_gdf = arr_gdf[
            [CODE_FIELDS["arrondissement"], NAME_FIELDS["arrondissement"], "geometry"]
        ].copy()
        arr_gdf.columns = ["code", "name", "geometry"]

        # Remove parent commune codes (Paris, Lyon, Marseille)
        parent_codes = {"75056", "69123", "13055"}
        initial_count = len(communes_gdf)
        communes_gdf = communes_gdf[
            ~communes_gdf[CODE_FIELDS["commune"]].isin(parent_codes)
        ]
        logger.info(f"  Removed {initial_count - len(communes_gdf)} parent communes")
        logger.info(f"  Adding {len(arr_gdf)} arrondissements")
    else:
        logger.warning("Arrondissements shapefile not found, skipping merge")
        arr_gdf = None

    # Simplify communes
    tolerance = SIMPLIFY_TOLERANCE.get("commune", 75)
    communes_gdf["geometry"] = communes_gdf.geometry.simplify(
        tolerance, preserve_topology=True
    )

    # Rename fields
    communes_gdf = communes_gdf[
        [CODE_FIELDS["commune"], NAME_FIELDS["commune"], "geometry"]
    ].copy()
    communes_gdf.columns = ["code", "name", "geometry"]

    # Merge if arrondissements loaded
    if arr_gdf is not None:
        communes_gdf = gpd.GeoDataFrame(
            pd.concat([communes_gdf, arr_gdf], ignore_index=True),
            crs=communes_gdf.crs,
        )

    # Reproject to WGS84
    communes_gdf = communes_gdf.to_crs(epsg=4326)

    # Remove empty geometries
    communes_gdf = communes_gdf[~communes_gdf.geometry.is_empty]

    # Write GeoJSON
    communes_gdf.to_file(output_path, driver="GeoJSON")

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"  → communes.geojson: {len(communes_gdf)} features, {size_mb:.1f} MB")


def compute_stats_for_level(level: str, code_expr: str) -> dict:
    """Computes price statistics for a geographic level.

    For commune level, batches by department to reduce memory pressure.

    Args:
        level: Geographic level name (for logging).
        code_expr: SQL expression for the geographic code (can be a column name
                   or a complex expression like concatenation).

    Returns:
        Dictionary of stats keyed by geographic code.
    """
    logger.info(f"Computing stats for {level}...")

    stats = {}

    if level == "commune":
        # Batch by department to avoid OOM
        con = get_db()
        depts = con.execute("SELECT DISTINCT dept_code FROM dvf_clean").fetchall()
        con.close()

        dept_codes = [d[0] for d in depts if d[0]]
        logger.info(f"  → Processing {len(dept_codes)} departments in batches...")

        for dept_code in tqdm(dept_codes, desc="Communes by dept", unit="dept"):
            con = get_db()
            query = f"""
            SELECT
                {code_expr} AS code,
                APPROX_QUANTILE(price_m2, 0.5) AS median,
                APPROX_QUANTILE(price_m2, 0.25) AS q25,
                APPROX_QUANTILE(price_m2, 0.75) AS q75,
                COUNT(*) AS n
            FROM dvf_clean
            WHERE dept_code = '{dept_code}'
            GROUP BY {code_expr}
            HAVING COUNT(*) >= {MIN_SALES_FOR_STATS}
            """
            result = con.execute(query).fetchall()
            con.close()

            for row in result:
                code, median, q25, q75, n = row
                if code and median:
                    stats[str(code)] = {
                        "median_price_m2": round(median, 0),
                        "q25": round(q25, 0) if q25 else None,
                        "q75": round(q75, 0) if q75 else None,
                        "n_sales": n,
                    }
    else:
        # Standard non-batched query for other levels
        con = get_db()
        query = f"""
        SELECT
            {code_expr} AS code,
            APPROX_QUANTILE(price_m2, 0.5) AS median,
            APPROX_QUANTILE(price_m2, 0.25) AS q25,
            APPROX_QUANTILE(price_m2, 0.75) AS q75,
            COUNT(*) AS n
        FROM dvf_clean
        GROUP BY {code_expr}
        HAVING COUNT(*) >= {MIN_SALES_FOR_STATS}
        """
        result = con.execute(query).fetchall()
        con.close()

        for row in result:
            code, median, q25, q75, n = row
            if code and median:
                stats[str(code)] = {
                    "median_price_m2": round(median, 0),
                    "q25": round(q25, 0) if q25 else None,
                    "q75": round(q75, 0) if q75 else None,
                    "n_sales": n,
                }

    logger.info(f"  → {len(stats)} areas with stats")
    return stats


def merge_arrondissements_to_communes(commune_stats: dict) -> dict:
    """Aggregates arrondissement stats into parent commune codes.

    Paris, Lyon, and Marseille use arrondissement codes in DVF but single
    commune codes in shapefiles. This combines arrondissement data.

    Args:
        commune_stats: Dictionary of commune stats keyed by INSEE code.

    Returns:
        Updated stats dict with aggregated parent communes added.
    """
    parent_data = {}

    for arr_code, parent_code in ARRONDISSEMENT_TO_COMMUNE.items():
        if arr_code in commune_stats:
            if parent_code not in parent_data:
                parent_data[parent_code] = {
                    "prices": [],
                    "n_sales": 0,
                    "q25s": [],
                    "q75s": [],
                }
            stats = commune_stats[arr_code]
            parent_data[parent_code]["prices"].append(
                (stats["median_price_m2"], stats["n_sales"])
            )
            parent_data[parent_code]["n_sales"] += stats["n_sales"]
            if stats["q25"]:
                parent_data[parent_code]["q25s"].append(stats["q25"])
            if stats["q75"]:
                parent_data[parent_code]["q75s"].append(stats["q75"])

    for parent_code, data in parent_data.items():
        if data["prices"]:
            # Weighted median by n_sales
            total_weight = sum(w for _, w in data["prices"])
            weighted_median = sum(p * w for p, w in data["prices"]) / total_weight
            commune_stats[parent_code] = {
                "median_price_m2": round(weighted_median, 0),
                "q25": (
                    round(sum(data["q25s"]) / len(data["q25s"]), 0)
                    if data["q25s"]
                    else None
                ),
                "q75": (
                    round(sum(data["q75s"]) / len(data["q75s"]), 0)
                    if data["q75s"]
                    else None
                ),
                "n_sales": data["n_sales"],
            }

    return commune_stats


def compute_region_stats() -> dict:
    """Computes price statistics aggregated by region using SQL.

    Uses CASE expression to map department codes to region codes directly
    in SQL, enabling DuckDB to compute true region-level quantiles.

    Returns:
        Dictionary of stats keyed by region code.
    """
    logger.info("Computing stats for region...")

    # Build CASE expression for dept_code -> region_code mapping
    case_clauses = " ".join(
        f"WHEN dept_code = '{dept}' THEN '{region}'"
        for dept, region in DEPT_TO_REGION.items()
    )
    region_expr = f"CASE {case_clauses} END"

    con = get_db()
    query = f"""
    SELECT
        {region_expr} AS region_code,
        APPROX_QUANTILE(price_m2, 0.5) AS median,
        APPROX_QUANTILE(price_m2, 0.25) AS q25,
        APPROX_QUANTILE(price_m2, 0.75) AS q75,
        COUNT(*) AS n
    FROM dvf_clean
    WHERE {region_expr} IS NOT NULL
    GROUP BY region_code
    HAVING COUNT(*) >= {MIN_SALES_FOR_STATS}
    """
    result = con.execute(query).fetchall()
    con.close()

    stats = {}
    for row in result:
        region_code, median, q25, q75, n = row
        if region_code and median:
            stats[region_code] = {
                "median_price_m2": round(median, 0),
                "q25": round(q25, 0) if q25 else None,
                "q75": round(q75, 0) if q75 else None,
                "n_sales": n,
            }

    logger.info(f"  → {len(stats)} regions with stats")
    return stats


def compute_canton_stats(commune_stats: dict) -> dict:
    """Computes canton statistics by aggregating pre-computed commune stats.

    This approach is memory-efficient: instead of joining the mapping with
    millions of DVF rows, we aggregate the ~35k commune stats in Python.

    Args:
        commune_stats: Pre-computed commune statistics dict.

    Returns:
        Dictionary of stats keyed by canton code (format: DEP_CAN, e.g., "01_01").
    """
    logger.info("Computing stats for canton (from commune stats)...")

    # Load commune→canton mapping from COMMUNE.shp
    commune_shp = SHAPEFILE_PATHS.get("commune")
    if not commune_shp or not commune_shp.exists():
        logger.warning("Commune shapefile not found, skipping canton stats")
        return {}

    gdf = gpd.read_file(commune_shp, columns=["INSEE_COM", "INSEE_DEP", "INSEE_CAN"])

    # Build INSEE_COM -> canton_code mapping
    commune_to_canton = {}
    for _, row in gdf.iterrows():
        insee_com = str(row["INSEE_COM"])
        insee_dep = str(row["INSEE_DEP"])
        insee_can = str(row["INSEE_CAN"]) if pd.notna(row["INSEE_CAN"]) else None
        if insee_can:
            canton_code = f"{insee_dep}_{insee_can}"
            commune_to_canton[insee_com] = canton_code

    logger.info(f"  → Mapped {len(commune_to_canton)} communes to cantons")

    # Aggregate commune stats to canton level
    # Using weighted average by n_sales for median, simple average for quartiles
    canton_data = {}
    for insee_com, canton_code in commune_to_canton.items():
        if insee_com not in commune_stats:
            continue

        stats = commune_stats[insee_com]
        if canton_code not in canton_data:
            canton_data[canton_code] = {
                "weighted_prices": [],  # (median, n_sales) pairs
                "n_sales": 0,
                "q25s": [],
                "q75s": [],
            }

        data = canton_data[canton_code]
        data["weighted_prices"].append((stats["median_price_m2"], stats["n_sales"]))
        data["n_sales"] += stats["n_sales"]
        if stats.get("q25"):
            data["q25s"].append(stats["q25"])
        if stats.get("q75"):
            data["q75s"].append(stats["q75"])

    # Convert to final stats format
    stats = {}
    for canton_code, data in canton_data.items():
        if data["n_sales"] >= MIN_SALES_FOR_STATS and data["weighted_prices"]:
            # Weighted average median by n_sales
            total_weight = sum(n for _, n in data["weighted_prices"])
            weighted_median = (
                sum(p * n for p, n in data["weighted_prices"]) / total_weight
            )

            stats[canton_code] = {
                "median_price_m2": round(weighted_median, 0),
                "q25": (
                    round(sum(data["q25s"]) / len(data["q25s"]), 0)
                    if data["q25s"]
                    else None
                ),
                "q75": (
                    round(sum(data["q75s"]) / len(data["q75s"]), 0)
                    if data["q75s"]
                    else None
                ),
                "n_sales": data["n_sales"],
            }

    # Add pseudo-cantons for Paris, Lyon, Marseille (aggregate arrondissements)
    plm_mapping = {
        "75_PARIS": [f"75{i:03d}" for i in range(101, 121)],  # 75101-75120
        "69_LYON": [f"69{i:03d}" for i in range(381, 390)],  # 69381-69389
        "13_MARSEILLE": [f"13{i:03d}" for i in range(201, 217)],  # 13201-13216
    }

    for plm_code, arr_codes in plm_mapping.items():
        plm_data = {"weighted_prices": [], "n_sales": 0, "q25s": [], "q75s": []}
        for arr_code in arr_codes:
            if arr_code in commune_stats:
                s = commune_stats[arr_code]
                plm_data["weighted_prices"].append((s["median_price_m2"], s["n_sales"]))
                plm_data["n_sales"] += s["n_sales"]
                if s.get("q25"):
                    plm_data["q25s"].append(s["q25"])
                if s.get("q75"):
                    plm_data["q75s"].append(s["q75"])

        if plm_data["n_sales"] >= MIN_SALES_FOR_STATS and plm_data["weighted_prices"]:
            total_weight = sum(n for _, n in plm_data["weighted_prices"])
            weighted_median = (
                sum(p * n for p, n in plm_data["weighted_prices"]) / total_weight
            )
            stats[plm_code] = {
                "median_price_m2": round(weighted_median, 0),
                "q25": (
                    round(sum(plm_data["q25s"]) / len(plm_data["q25s"]), 0)
                    if plm_data["q25s"]
                    else None
                ),
                "q75": (
                    round(sum(plm_data["q75s"]) / len(plm_data["q75s"]), 0)
                    if plm_data["q75s"]
                    else None
                ),
                "n_sales": plm_data["n_sales"],
            }
            logger.info(f"  Added pseudo-canton stats: {plm_code}")

    logger.info(f"  → {len(stats)} cantons with stats")
    return stats


def create_country_geojson() -> None:
    """Creates a France country outline by dissolving regions."""
    output_path = OUTPUT_DIR / "country.geojson"

    if output_path.exists():
        logger.info("Country GeoJSON already exists")
        return

    logger.info("Creating country outline...")

    regions_path = SHAPEFILE_PATHS.get("region")
    if not regions_path or not regions_path.exists():
        logger.warning("Region shapefile not found, skipping country outline")
        return

    gdf = gpd.read_file(regions_path)

    # Filter to metropolitan France (exclude overseas)
    metro_regions = [
        "11",
        "24",
        "27",
        "28",
        "32",
        "44",
        "52",
        "53",
        "75",
        "76",
        "84",
        "93",
        "94",
    ]
    gdf = gdf[gdf["INSEE_REG"].isin(metro_regions)]

    # Simplify
    gdf["geometry"] = gdf.geometry.simplify(
        SIMPLIFY_TOLERANCE["country"], preserve_topology=True
    )

    # Dissolve all regions
    gdf["country"] = "FR"
    dissolved = gdf.dissolve(by="country")

    # Reproject to WGS84
    dissolved = dissolved.to_crs(epsg=4326)

    # Create GeoJSON manually for simpler output
    dissolved = dissolved.reset_index()
    dissolved["code"] = "FR"
    dissolved["name"] = "France"
    dissolved = dissolved[["code", "name", "geometry"]]

    dissolved.to_file(output_path, driver="GeoJSON")
    size_kb = output_path.stat().st_size / 1024
    logger.info(f"  → country.geojson: {size_kb:.1f} KB")


def main() -> None:
    """Main pipeline execution."""
    logger.info("Starting precompute pipeline...")

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Convert shapefiles to GeoJSON
    logger.info("=== Phase 1: Converting shapefiles to GeoJSON ===")
    create_country_geojson()

    # Process region and departement shapefiles
    for level in ["region", "departement"]:
        shp_path = SHAPEFILE_PATHS.get(level)
        if not shp_path or not shp_path.exists():
            logger.warning(f"Shapefile not found for {level}")
            continue

        output_path = OUTPUT_DIR / f"{level}s.geojson"
        convert_shapefile_to_geojson(
            shp_path, output_path, CODE_FIELDS[level], NAME_FIELDS[level], level
        )

    # Convert cantons with DEP_CAN code format
    convert_cantons_to_geojson()

    # Process communes with arrondissements merged inline
    convert_communes_with_arrondissements()

    # 2. Compute stats for each level
    logger.info("=== Phase 2: Computing price statistics ===")

    region_stats = compute_region_stats()

    # Compute commune stats first (needed for canton aggregation)
    commune_stats = compute_stats_for_level("commune", INSEE_COMMUNE_EXPR)

    # Canton stats are aggregated from commune stats (memory-efficient)
    canton_stats = compute_canton_stats(commune_stats)

    all_stats = {
        "country": {},
        "region": region_stats,
        "departement": compute_stats_for_level("departement", "dept_code"),
        "canton": canton_stats,
        "commune": commune_stats,
    }

    # Compute country-level aggregate from dvf_clean
    con = get_db()
    country_query = """
    SELECT
        APPROX_QUANTILE(price_m2, 0.5) AS median,
        APPROX_QUANTILE(price_m2, 0.25) AS q25,
        APPROX_QUANTILE(price_m2, 0.75) AS q75,
        COUNT(*) AS n
    FROM dvf_clean
    """
    result = con.execute(country_query).fetchone()
    con.close()

    all_stats["country"]["FR"] = {
        "median_price_m2": round(result[0], 0) if result[0] else None,
        "q25": round(result[1], 0) if result[1] else None,
        "q75": round(result[2], 0) if result[2] else None,
        "n_sales": result[3],
    }

    # Save stats cache
    with open(STATS_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)

    logger.info(f"Stats saved to {STATS_OUTPUT}")
    logger.info("Pipeline complete!")


if __name__ == "__main__":
    main()
