"""
Main execution script for the precompute pipeline.

Orchestrates geometry processing and statistics computation.
"""

import json
import logging
import geopandas as gpd
import pandas as pd
from pathlib import Path

from src.data.config import (
    OUTPUT_DIR,
    STATS_OUTPUT,
    SHAPEFILE_PATHS,
    CODE_FIELDS,
    NAME_FIELDS,
    MIN_SALES_FOR_STATS,
)
from src.data.pipeline import geometry, stats

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_simple_layers():
    """Processes simple layers (Region, Department)."""
    for level in ["region", "departement"]:
        shp_path = SHAPEFILE_PATHS.get(level)
        if not shp_path or not shp_path.exists():
            logger.warning(f"Shapefile not found for {level}")
            continue

        output_path = OUTPUT_DIR / f"{level}s.geojson"
        if output_path.exists():
            logger.info(f"{output_path.name} already exists.")
            continue

        gdf = geometry.load_and_simplify(
            shp_path, CODE_FIELDS[level], NAME_FIELDS[level], level
        )
        geometry.save_geojson(gdf, output_path)


def process_country_layer():
    """Creates country outline from regions."""
    output_path = OUTPUT_DIR / "country.geojson"
    if output_path.exists():
        logger.info("country.geojson already exists.")
        return

    regions_path = SHAPEFILE_PATHS.get("region")
    if not regions_path or not regions_path.exists():
        return

    logger.info("Creating country outline...")
    gdf = gpd.read_file(regions_path)

    # Metropolitan France filter
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

    # Simplify lightly before dissolve to speed it up
    # "country" level tolerance is high (1500m)
    gdf = geometry.simplify_with_topology(gdf, "country")

    # Dissolve
    gdf["country"] = "FR"
    dissolved = gdf.dissolve(by="country").reset_index()
    dissolved["code"] = "FR"
    dissolved["name"] = "France"
    dissolved = dissolved[["code", "name", "geometry"]]

    geometry.save_geojson(dissolved, output_path)


def process_cantons_layer() -> dict:
    """Processes cantons, adding pseudo-cantons for PLM. Returns commune->canton map."""
    output_path = OUTPUT_DIR / "cantons.geojson"
    commune_to_canton = {}

    # Always build the map, even if geojson exists (needed for stats)
    folder_shp = SHAPEFILE_PATHS.get("commune")
    if folder_shp and folder_shp.exists():
        # We need to map INSEE_COM -> CODE_CANTON (DEP_CAN)
        # Using the commune shapefile or canton shapefile?
        # Commune shapefile has INSEE_CAN usually.
        gdf_com = gpd.read_file(
            folder_shp, columns=["INSEE_COM", "INSEE_DEP", "INSEE_CAN"]
        )
        for _, row in gdf_com.iterrows():
            if pd.notna(row["INSEE_CAN"]):
                commune_to_canton[str(row["INSEE_COM"])] = (
                    f"{row['INSEE_DEP']}_{row['INSEE_CAN']}"
                )

    # Manually map PLM (Paris, Lyon, Marseille) cities to their pseudo-canton codes
    # This ensures compute_canton_stats finds them
    commune_to_canton["75056"] = "75_PARIS"
    commune_to_canton["69123"] = "69_LYON"
    commune_to_canton["13055"] = "13_MARSEILLE"

    if output_path.exists():
        logger.info("cantons.geojson already exists.")
        return commune_to_canton

    canton_shp = SHAPEFILE_PATHS.get("canton")
    if not canton_shp:
        return commune_to_canton

    logger.info("Processing cantons...")
    gdf = gpd.read_file(canton_shp)
    gdf = geometry.simplify_with_topology(gdf, "canton")

    # Create code
    gdf["code"] = gdf["INSEE_DEP"].astype(str) + "_" + gdf["INSEE_CAN"].astype(str)
    gdf["name"] = "Canton " + gdf["INSEE_CAN"].astype(str)
    gdf = gdf[["code", "name", "geometry"]]

    # Add pseudo-cantons (PLM)
    # logic: get geometries of Paris/Lyon/Marseille from communes shp
    if folder_shp and folder_shp.exists():
        # Re-read full geometry for PLM
        # Paris 75056, Lyon 69123, Marseille 13055
        plm_codes = {
            "75056": ("75_PARIS", "Paris"),
            "69123": ("69_LYON", "Lyon"),
            "13055": ("13_MARSEILLE", "Marseille"),
        }

        # We need these geometries to match the simplified commune geometries?
        # Actually, best to load them from the processed communes if available?
        # For now, read from source and simplify same way
        plm_gdf = gpd.read_file(folder_shp)
        plm_gdf = plm_gdf[plm_gdf["INSEE_COM"].isin(plm_codes.keys())]
        plm_gdf = geometry.simplify_with_topology(
            plm_gdf, "canton"
        )  # use canton tolerance

        new_rows = []
        for _, row in plm_gdf.iterrows():
            insee = str(row["INSEE_COM"])
            if insee in plm_codes:
                code, name = plm_codes[insee]
                new_rows.append({"code": code, "name": name, "geometry": row.geometry})

        if new_rows:
            gdf = pd.concat(
                [gdf, gpd.GeoDataFrame(new_rows, crs=gdf.crs)], ignore_index=True
            )

    geometry.save_geojson(gdf, output_path)
    return commune_to_canton


def process_communes_layer():
    """Processes communes, merging arrondissements into them."""
    output_path = OUTPUT_DIR / "communes.geojson"
    if output_path.exists():
        logger.info("communes.geojson already exists.")
        return

    logger.info("Processing communes (with arrondissements)...")

    communes_shp = SHAPEFILE_PATHS["commune"]
    arr_shp = SHAPEFILE_PATHS["arrondissement"]

    communes_gdf = gpd.read_file(communes_shp)

    # Prepare parent communes (remove PLM parents, we will use arrondissements?
    # WAIT: logic in old script was: remove PLM parent rows, insert arrondissement rows.
    # Because we want to display granular data in PLM.

    parent_codes = {"75056", "69123", "13055"}
    communes_gdf = communes_gdf[~communes_gdf["INSEE_COM"].isin(parent_codes)]

    communes_gdf = communes_gdf.rename(columns={"INSEE_COM": "code", "NOM": "name"})
    communes_gdf = communes_gdf[["code", "name", "geometry"]]

    if arr_shp.exists():
        arr_gdf = gpd.read_file(arr_shp)
        arr_gdf = arr_gdf.rename(columns={"INSEE_ARM": "code", "NOM": "name"})
        arr_gdf = arr_gdf[["code", "name", "geometry"]]

        communes_gdf = pd.concat([communes_gdf, arr_gdf], ignore_index=True)

    # Simplify all together
    communes_gdf = geometry.simplify_with_topology(communes_gdf, "commune")
    geometry.save_geojson(communes_gdf, output_path)


def main():
    logger.info("Starting pipeline (Modular Refactor)...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Geometry Phase ---
    process_country_layer()
    process_simple_layers()
    # process cantons and get mapping
    commune_to_canton = process_cantons_layer()
    process_communes_layer()

    # --- Stats Phase ---
    logger.info("Computing statistics...")

    commune_stats, commune_names = stats.compute_commune_stats()

    all_stats = {
        "country": stats.compute_country_stats(),
        "region": stats.compute_region_stats(),
        "departement": stats.compute_department_stats(),
        "commune": commune_stats,
    }

    # Compute canton stats using results + mapping
    all_stats["canton"] = stats.compute_canton_stats(
        all_stats["commune"], commune_to_canton
    )

    # Save cache
    with open(STATS_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)

    # Precompute Top 10 expensive communes (min 100 sales)
    top_10 = stats.compute_top_expensive_communes(all_stats["commune"], commune_names)
    top_output = OUTPUT_DIR / "top_expensive.json"
    with open(top_output, "w", encoding="utf-8") as f:
        json.dump({"data": top_10}, f, ensure_ascii=False, indent=2)

    logger.info(f"Stats saved to {STATS_OUTPUT}")
    logger.info(f"Top 10 saved to {top_output}")
    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
