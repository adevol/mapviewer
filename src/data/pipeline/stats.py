"""
Statistics calculation module for the MapViewer pipeline.

Handles DuckDB queries to compute weighted price statistics (median, q25, q75)
for all geographic levels, applying filtering and aggregation.
"""

import logging
from typing import Dict, Any

import duckdb
from tqdm import tqdm

from src.data.config import (
    DB_PATH,
    MIN_PRICE_M2,
    MAX_PRICE_M2,
    MIN_SALES_FOR_STATS,
    DEPT_TO_REGION,
    INSEE_COMMUNE_EXPR,
    ARRONDISSEMENT_TO_COMMUNE,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_TEMP_DIR,
    DUCKDB_THREADS,
)

logger = logging.getLogger(__name__)


def get_db() -> duckdb.DuckDBPyConnection:
    """Gets a DuckDB connection with spatial extension."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory = '{DUCKDB_TEMP_DIR}';")
    con.execute(f"SET threads = {DUCKDB_THREADS};")
    return con


def get_base_filter() -> str:
    """Returns the SQL WHERE clause for valid price and sales."""
    return f"""
        price_m2 BETWEEN {MIN_PRICE_M2} AND {MAX_PRICE_M2}
        AND nature = 'Vente'
    """


def compute_standard_stats(
    group_by_expr: str,
    alias: str = "code",
    additional_where: str = "1=1",
) -> Dict[str, Any]:
    """Computes standard stats (median, quantiles) for a given grouping."""
    con = get_db()

    query = f"""
    SELECT
        {group_by_expr} AS {alias},
        APPROX_QUANTILE(price_m2, 0.5) AS median,
        APPROX_QUANTILE(price_m2, 0.25) AS q25,
        APPROX_QUANTILE(price_m2, 0.75) AS q75,
        COUNT(*) AS n
    FROM dvf_clean
    WHERE {get_base_filter()}
    AND {additional_where}
    GROUP BY {alias}
    HAVING COUNT(*) >= {MIN_SALES_FOR_STATS}
    """

    try:
        result = con.execute(query).fetchall()
        stats = {}
        for row in result:
            code, median, q25, q75, n = row
            if code and median:
                stats[str(code)] = {
                    "median_price_m2": round(median, 0),
                    "q25": round(q25, 0) if q25 else None,
                    "q75": round(q75, 0) if q75 else None,
                    "n_sales": n,
                }
        return stats
    finally:
        con.close()


def compute_commune_stats() -> Dict[str, Any]:
    """Computes stats for communes, handling PLM arrondissements.

    Optimized to run in batches per department to reduce memory usage.
    """
    logger.info("Computing commune stats (SQL aggregated by department)...")

    stats = {}
    departments = sorted(list(set(DEPT_TO_REGION.keys())))

    # 1. Compute base stats for all communes (including arrondissements)
    # processing batch-by-batch (department) to avoid OOM
    for dept_code in tqdm(departments, desc="Processing departments"):
        # standard standard stats for this department
        dept_stats = compute_standard_stats(
            INSEE_COMMUNE_EXPR,
            additional_where=f"dept_code = '{dept_code}'",
        )
        stats.update(dept_stats)

    # 2. Aggregate arrondissements into parent communes (Paris, Lyon, Marseille)
    # logic: parent stats = weighted average of child (arrondissement) stats
    parent_data = {}

    for arr_code, parent_code in ARRONDISSEMENT_TO_COMMUNE.items():
        if arr_code in stats:
            s = stats[arr_code]
            if parent_code not in parent_data:
                parent_data[parent_code] = {
                    "w_prices": [],
                    "sales": 0,
                    "q25s": [],
                    "q75s": [],
                }

            p = parent_data[parent_code]
            p["w_prices"].append((s["median_price_m2"], s["n_sales"]))
            p["sales"] += s["n_sales"]
            if s["q25"]:
                p["q25s"].append(s["q25"])
            if s["q75"]:
                p["q75s"].append(s["q75"])

    # Merge parent stats back
    for parent_code, d in parent_data.items():
        if d["sales"] >= MIN_SALES_FOR_STATS and d["w_prices"]:
            total_weight = sum(w for _, w in d["w_prices"])
            weighted_median = sum(p * w for p, w in d["w_prices"]) / total_weight

            q25 = sum(d["q25s"]) / len(d["q25s"]) if d["q25s"] else None
            q75 = sum(d["q75s"]) / len(d["q75s"]) if d["q75s"] else None

            stats[parent_code] = {
                "median_price_m2": round(weighted_median, 0),
                "q25": round(q25, 0) if q25 else None,
                "q75": round(q75, 0) if q75 else None,
                "n_sales": d["sales"],
            }
            logger.debug(f"Aggregated {parent_code}: {d['sales']} sales")

    return stats


def compute_region_stats() -> Dict[str, Any]:
    """Computes region stats using dept->region mapping."""
    logger.info("Computing region stats...")

    case_stmt = (
        "CASE "
        + " ".join(
            f"WHEN dept_code = '{d}' THEN '{r}'" for d, r in DEPT_TO_REGION.items()
        )
        + " END"
    )

    return compute_standard_stats(case_stmt, "region_code", f"{case_stmt} IS NOT NULL")


def compute_department_stats() -> Dict[str, Any]:
    """Computes department stats."""
    logger.info("Computing department stats...")
    return compute_standard_stats("dept_code", "dept_code")


def compute_country_stats() -> Dict[str, Any]:
    """Computes single country-wide stat."""
    logger.info("Computing country stats...")
    con = get_db()
    query = f"""
    SELECT
        APPROX_QUANTILE(price_m2, 0.5),
        APPROX_QUANTILE(price_m2, 0.25),
        APPROX_QUANTILE(price_m2, 0.75),
        COUNT(*)
    FROM dvf_clean
    WHERE {get_base_filter()}
    """
    result = con.execute(query).fetchone()
    con.close()

    return {
        "FR": {
            "median_price_m2": round(result[0], 0) if result[0] else None,
            "q25": round(result[1], 0) if result[1] else None,
            "q75": round(result[2], 0) if result[2] else None,
            "n_sales": result[3],
        }
    }


def compute_canton_stats(
    commune_stats: Dict[str, Any], commune_to_canton: Dict[str, str]
) -> Dict[str, Any]:
    """Computes canton stats by aggregating pre-calculated commune stats."""
    # Note: commune_to_canton must be provided by the geometry module which reads the shp
    logger.info("Computing canton stats (from commune aggregation)...")

    canton_data = {}

    for insee_com, canton_code in commune_to_canton.items():
        if insee_com in commune_stats:
            s = commune_stats[insee_com]
            if canton_code not in canton_data:
                canton_data[canton_code] = {
                    "w_prices": [],
                    "sales": 0,
                    "q25s": [],
                    "q75s": [],
                }

            c = canton_data[canton_code]
            c["w_prices"].append((s["median_price_m2"], s["n_sales"]))
            c["sales"] += s["n_sales"]
            if s["q25"]:
                c["q25s"].append(s["q25"])
            if s["q75"]:
                c["q75s"].append(s["q75"])

    stats = {}
    for code, d in canton_data.items():
        if d["sales"] >= MIN_SALES_FOR_STATS and d["w_prices"]:
            total_weight = sum(w for _, w in d["w_prices"])
            weighted_median = sum(p * w for p, w in d["w_prices"]) / total_weight

            q25 = sum(d["q25s"]) / len(d["q25s"]) if d["q25s"] else None
            q75 = sum(d["q75s"]) / len(d["q75s"]) if d["q75s"] else None

            stats[code] = {
                "median_price_m2": round(weighted_median, 0),
                "q25": round(q25, 0) if q25 else None,
                "q75": round(q75, 0) if q75 else None,
                "n_sales": d["sales"],
            }

    return stats
