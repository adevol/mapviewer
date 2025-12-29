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
    """Gets a DuckDB connection with spatial extension.

    Configures memory limits and thread count from config.

    Returns:
        Configured DuckDB connection in read-only mode.
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("LOAD spatial;")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory = '{DUCKDB_TEMP_DIR}';")
    con.execute(f"SET threads = {DUCKDB_THREADS};")
    return con


def get_base_filter() -> str:
    """Returns the SQL WHERE clause for valid price filtering.

    Applies price range limits and filters to 'Vente' transactions only.

    Returns:
        SQL WHERE clause fragment.
    """
    return f"""
        price_m2 BETWEEN {MIN_PRICE_M2} AND {MAX_PRICE_M2}
        AND nature = 'Vente'
    """


def compute_standard_stats(
    group_by_expr: str,
    alias: str = "code",
    additional_where: str = "1=1",
) -> Dict[str, Any]:
    """Computes price statistics (median, quartiles) for a given grouping.

    Args:
        group_by_expr: SQL expression to group by (e.g., 'dept_code').
        alias: Column alias for the grouping in results.
        additional_where: Extra SQL WHERE conditions.

    Returns:
        Tuple of (stats_dict, name_map) with price statistics per group.
    """
    con = get_db()

    query = f"""
    SELECT
        {group_by_expr} AS {alias},
        APPROX_QUANTILE(price_m2, 0.5) AS median,
        APPROX_QUANTILE(price_m2, 0.25) AS q25,
        APPROX_QUANTILE(price_m2, 0.75) AS q75,
        COUNT(*) AS n,
        ANY_VALUE(commune_name) as name
    FROM dvf_clean
    WHERE {get_base_filter()}
    AND {additional_where}
    GROUP BY {alias}
    HAVING COUNT(*) >= {MIN_SALES_FOR_STATS}
    """

    try:
        result = con.execute(query).fetchall()
        stats = {}
        names = {}
        for row in result:
            code, median, q25, q75, n, name = row
            if code and median:
                stats[str(code)] = {
                    "median_price_m2": round(median, 0),
                    "q25": round(q25, 0) if q25 else None,
                    "q75": round(q75, 0) if q75 else None,
                    "n_sales": n,
                }
                if name:
                    names[str(code)] = name
        return stats, names
    finally:
        con.close()


def compute_commune_stats() -> tuple[Dict[str, Any], Dict[str, str]]:
    """Computes stats for communes, handling PLM arrondissements.

    Returns:
        tuple of (stats_dict, name_map)
    """
    logger.info("Computing commune stats (SQL aggregated by department)...")

    stats = {}
    names = {}
    departments = sorted(list(set(DEPT_TO_REGION.keys())))

    for dept_code in tqdm(departments, desc="Processing departments"):
        dept_stats, dept_names = compute_standard_stats(
            INSEE_COMMUNE_EXPR,
            additional_where=f"dept_code = '{dept_code}'",
        )
        stats.update(dept_stats)
        names.update(dept_names)

    # 2. Aggregate arrondissements into parent communes (Paris, Lyon, Marseille)
    parent_data = {}
    plm_names = {"75056": "Paris", "69123": "Lyon", "13055": "Marseille"}
    names.update(plm_names)

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

    return stats, names


def compute_region_stats() -> Dict[str, Any]:
    """Computes region-level price statistics.

    Maps department codes to region codes using DEPT_TO_REGION config.

    Returns:
        Dictionary of stats keyed by region code.
    """
    logger.info("Computing region stats...")

    case_stmt = (
        "CASE "
        + " ".join(
            f"WHEN dept_code = '{d}' THEN '{r}'" for d, r in DEPT_TO_REGION.items()
        )
        + " END"
    )

    res_stats, _ = compute_standard_stats(
        case_stmt, "region_code", f"{case_stmt} IS NOT NULL"
    )
    return res_stats


def compute_department_stats() -> Dict[str, Any]:
    """Computes department-level price statistics.

    Returns:
        Dictionary of stats keyed by department code.
    """
    logger.info("Computing department stats...")
    res_stats, _ = compute_standard_stats("dept_code", "dept_code")
    return res_stats


def compute_country_stats() -> Dict[str, Any]:
    """Computes country-wide aggregate price statistics.

    Returns:
        Dictionary with single 'FR' key containing national stats.
    """
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
    """Computes canton stats by aggregating pre-calculated commune stats.

    Uses weighted averaging based on sales volume per commune.

    Args:
        commune_stats: Pre-computed commune statistics.
        commune_to_canton: Mapping from commune INSEE codes to canton codes.

    Returns:
        Dictionary of stats keyed by canton code.
    """
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


def compute_top_expensive_communes(
    commune_stats: Dict[str, Any], name_map: Dict[str, str]
) -> list[Dict[str, Any]]:
    """Finds the top 10 most expensive communes by median price.

    Only includes communes with at least 100 sales for reliability.

    Args:
        commune_stats: Pre-computed commune statistics.
        name_map: Mapping from commune codes to names.

    Returns:
        List of top 10 commune dicts sorted by price descending.
    """
    logger.info("Filtering top 10 expensive communes from precomputed stats...")

    top_10 = []
    for code, s in commune_stats.items():
        if s.get("n_sales", 0) >= 100:
            name = name_map.get(code, code)
            top_10.append(
                {
                    "city": name,
                    "code": code,
                    "median_price_m2": s["median_price_m2"],
                    "volume": s["n_sales"],
                }
            )

    top_10.sort(key=lambda x: x["median_price_m2"], reverse=True)
    return top_10[:10]
