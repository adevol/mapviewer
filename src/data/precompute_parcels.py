"""
Extract sold parcels from cadastre by joining with DVF transactions.

Creates GeoJSON files per department containing only parcels that have
been sold, enriched with transaction price data.

Usage:
    python -m src.data.precompute_parcels
"""

import json
import logging
from pathlib import Path

import duckdb
from tqdm import tqdm

from .config import (
    CADASTRE_FILE,
    DB_PATH,
    OUTPUT_DIR,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PARCELS_DIR = OUTPUT_DIR / "parcels"
SIMPLIFY_TOLERANCE = 0.00001  # ~1m at France latitude


def get_db() -> duckdb.DuckDBPyConnection:
    """Gets a DuckDB connection with spatial extension."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET temp_directory = 'data/temp';")
    return con


def get_departments_with_sales() -> list[str]:
    """Gets list of department codes that have DVF sales."""
    con = get_db()
    result = con.execute(
        """
        SELECT DISTINCT dept_code 
        FROM dvf_clean 
        WHERE dept_code IS NOT NULL
        ORDER BY dept_code
    """
    ).fetchall()
    con.close()
    return [r[0] for r in result]


def extract_sold_parcels_for_dept(dept_code: str) -> dict:
    """Extracts sold parcels for a single department.

    Returns stats dict with count and file size.
    """
    output_file = PARCELS_DIR / f"{dept_code}.geojson"

    if output_file.exists():
        size_kb = output_file.stat().st_size / 1024
        logger.info(f"  {dept_code}: Already exists ({size_kb:.1f} KB)")
        return {"count": 0, "size_kb": size_kb, "skipped": True}

    if not CADASTRE_FILE.exists():
        logger.warning(f"Cadastre file not found: {CADASTRE_FILE}")
        return {"count": 0, "size_kb": 0, "error": "no cadastre"}

    con = get_db()

    # Query: Join DVF with cadastre by matching commune codes
    # DVF has dept_code + commune_code, cadastre has 'commune' as full INSEE code
    query = f"""
    WITH dvf_sales AS (
        SELECT
            dept_code || LPAD(CAST(commune_code AS VARCHAR), 3, '0') as insee_com,
            commune_name,
            -- Aggregate by commune to reduce data
            COUNT(*) as n_sales,
            APPROX_QUANTILE(price_m2, 0.5) as median_price_m2
        FROM dvf_clean
        WHERE dept_code = '{dept_code}'
        AND price_m2 > 100
        AND price_m2 < 50000
        GROUP BY dept_code, commune_code, commune_name
    ),
    cadastre_parcels AS (
        SELECT
            id,
            commune,
            -- Simplify geometry in Lambert93 coordinates (0.5m tolerance)
            ST_SimplifyPreserveTopology(geometry, 0.5) as geometry
        FROM read_parquet('{CADASTRE_FILE}')
        WHERE departement = '{dept_code}'
        AND type_objet = 'parcelle'
        AND geometry IS NOT NULL
    )
    SELECT
        c.id as parcel_id,
        d.median_price_m2 as price_m2,
        d.n_sales,
        d.commune_name,
        ST_AsGeoJSON(
            ST_Transform(c.geometry, 'EPSG:2154', 'EPSG:4326')
        ) AS geom_geojson
    FROM cadastre_parcels c
    INNER JOIN dvf_sales d ON c.commune = d.insee_com
    LIMIT 20000  -- Cap per department
    """

    try:
        result = con.execute(query).fetchall()
        columns = ["parcel_id", "price_m2", "n_sales", "commune_name", "geom_geojson"]

        if not result:
            logger.info(f"  {dept_code}: No matching parcels found")
            con.close()
            return {"count": 0, "size_kb": 0}

        # Convert to GeoJSON features
        features = []
        for row in result:
            data = dict(zip(columns, row))
            if not data["geom_geojson"]:
                continue
            try:
                geometry = json.loads(data["geom_geojson"])
            except:
                continue

            feature = {
                "type": "Feature",
                "properties": {
                    "price_m2": (
                        round(data["price_m2"], 0) if data["price_m2"] else None
                    ),
                    "n_sales": data["n_sales"],
                    "name": data["commune_name"],
                },
                "geometry": geometry,
            }
            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features,
        }

        # Write with minimal whitespace
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

        size_kb = output_file.stat().st_size / 1024
        logger.info(f"  {dept_code}: {len(features)} parcels, {size_kb:.1f} KB")
        return {"count": len(features), "size_kb": size_kb}

    except Exception as e:
        logger.error(f"  {dept_code}: Error - {e}")
        return {"count": 0, "size_kb": 0, "error": str(e)}

    finally:
        con.close()


def main() -> None:
    """Main parcel extraction pipeline."""
    logger.info("Starting parcel extraction...")

    # Create output directory
    PARCELS_DIR.mkdir(parents=True, exist_ok=True)

    # Get departments with sales
    departments = get_departments_with_sales()
    logger.info(f"Found {len(departments)} departments with sales")

    # Extract parcels for each department
    total_count = 0
    total_size = 0

    for dept in tqdm(departments, desc="Extracting parcels"):
        stats = extract_sold_parcels_for_dept(dept)
        total_count += stats.get("count", 0)
        total_size += stats.get("size_kb", 0)

    logger.info(
        f"Extraction complete: {total_count} parcels, {total_size/1024:.1f} MB total"
    )


if __name__ == "__main__":
    main()
