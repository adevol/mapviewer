"""
Extract sold parcels from cadastre by joining with DVF transactions.

Creates GeoJSON files per department containing only parcels that have
been sold, enriched with transaction price data and parcel geometry for tiles.

Usage:
    python -m src.data.precompute_parcels
"""

import json
import logging
from pathlib import Path

import duckdb

from .config import (
    CADASTRE_FILE,
    DB_PATH,
    PARCELS_GEOJSON_DIR,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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


def extract_sold_parcels_for_dept(dept_code: str) -> None:
    """Extracts sold parcels for a single department.

    Args:
        dept_code: Department code (e.g., '75', '69', '971').
    """
    output_file = PARCELS_GEOJSON_DIR / f"{dept_code}.geojson"

    if output_file.exists():
        logger.info(f"  {dept_code}: Already exists, skipping")
        return

    if not CADASTRE_FILE.exists():
        logger.warning(f"Cadastre file not found: {CADASTRE_FILE}")
        return

    con = get_db()

    # Build parcel ID expression
    # Format: dept_code + commune_code (3 digits) + section_prefix + section + plan_number
    parcel_id_expr = """
        d.dept_code || 
        LPAD(CAST(d.commune_code AS VARCHAR), 3, '0') ||
        COALESCE(d.section_prefix, '000') ||
        COALESCE(d.section, '') ||
        LPAD(CAST(d.plan_number AS VARCHAR), 4, '0')
    """

    # Query to join DVF with cadastre and bring back parcel geometry
    # Note: This assumes cadastre parquet has 'id' column matching our format
    # May need adjustment based on actual cadastre schema
    query = f"""
    WITH sales AS (
        SELECT
            d.mutation_id,
            d.mutation_date,
            d.price,
            d.total_surface,
            d.price_m2,
            d.property_type,
            d.commune_name,
            d.postal_code,
            ({parcel_id_expr}) AS parcel_id,
            ROW_NUMBER() OVER (
                PARTITION BY ({parcel_id_expr})
                ORDER BY d.mutation_date DESC, d.price DESC
            ) AS sale_rank
        FROM dvf_clean d
        WHERE d.dept_code = '{dept_code}'
        AND d.price_m2 > 0
        AND d.price_m2 < 50000  -- Filter extreme outliers
    )
    SELECT
        s.mutation_id,
        s.mutation_date,
        s.price,
        s.total_surface,
        s.price_m2,
        s.property_type,
        s.commune_name,
        s.postal_code,
        s.parcel_id,
        ST_AsGeoJSON(
            ST_Transform(p.geometry, 'EPSG:2154', 'EPSG:4326')
        ) AS geom_geojson
    FROM sales s
    JOIN parcels p
        ON p.id = s.parcel_id
    WHERE s.sale_rank = 1
    AND p.geometry IS NOT NULL
    AND p.geom_srid = 2154
    """

    try:
        result = con.execute(query).fetchall()
        columns = [
            "mutation_id",
            "mutation_date",
            "price",
            "total_surface",
            "price_m2",
            "property_type",
            "commune_name",
            "postal_code",
            "parcel_id",
            "geom_geojson",
        ]

        if not result:
            logger.info(f"  {dept_code}: No sales found")
            con.close()
            return

        # Convert to GeoJSON features with parcel geometry
        features = []
        for row in result:
            data = dict(zip(columns, row))
            if not data["geom_geojson"]:
                continue
            geometry = json.loads(data["geom_geojson"])
            feature = {
                "type": "Feature",
                "properties": {
                    "price_m2": (
                        round(data["price_m2"], 0) if data["price_m2"] else None
                    ),
                    "price": data["price"],
                    "surface": data["total_surface"],
                    "date": (
                        str(data["mutation_date"]) if data["mutation_date"] else None
                    ),
                    "type": data["property_type"],
                    "commune": data["commune_name"],
                    "postcode": data["postal_code"],
                    "parcel_id": data["parcel_id"],
                    "n_sales": 1,
                },
                "geometry": geometry,
            }
            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False)

        logger.info(f"  {dept_code}: {len(features)} sold parcels")

    except Exception as e:
        logger.error(f"  {dept_code}: Error - {e}")

    finally:
        con.close()


def main() -> None:
    """Main parcel extraction pipeline."""
    logger.info("Starting parcel extraction...")

    # Create output directory
    PARCELS_GEOJSON_DIR.mkdir(parents=True, exist_ok=True)

    # Get departments with sales
    departments = get_departments_with_sales()
    logger.info(f"Found {len(departments)} departments with sales")

    # Extract parcels for each department
    for i, dept in enumerate(departments):
        logger.info(f"Processing department {i+1}/{len(departments)}: {dept}")
        extract_sold_parcels_for_dept(dept)

    logger.info("Parcel extraction complete!")


if __name__ == "__main__":
    main()
