"""
Split communes.geojson by department for on-demand loading.

This creates smaller GeoJSON files in src/frontend/communes/ that can be
loaded dynamically based on the viewport.

Usage:
    python -m src.data.split_communes
"""

import json
import logging
from collections import defaultdict

from .config import COMMUNES_INPUT_FILE, COMMUNES_OUTPUT_DIR, STATS_OUTPUT

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Split communes GeoJSON by department."""
    logger.info("Loading communes.geojson...")
    with open(COMMUNES_INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info(f"Loaded {len(data['features'])} communes")

    # Load stats to enrich features
    logger.info("Loading stats_cache.json...")
    with open(STATS_OUTPUT, "r", encoding="utf-8") as f:
        stats = json.load(f)
    commune_stats = stats.get("commune", {})

    # Group by department (first 2 chars of commune code, or 3 for overseas)
    by_dept = defaultdict(list)
    enriched_count = 0

    for feature in data["features"]:
        code = feature["properties"]["code"]
        # Department code: first 2 chars (or 3 for overseas like 971, 972, etc.)
        if code.startswith("97") or code.startswith("98"):
            dept = code[:3]
        else:
            dept = code[:2]

        # Enrich with stats
        if code in commune_stats:
            s = commune_stats[code]
            feature["properties"]["price_m2"] = s["median_price_m2"]
            feature["properties"]["n_sales"] = s["n_sales"]
            feature["properties"]["q25"] = s["q25"]
            feature["properties"]["q75"] = s["q75"]
            enriched_count += 1

        by_dept[dept].append(feature)

    logger.info(f"Enriched {enriched_count} communes with stats")
    logger.info(f"Found {len(by_dept)} departments")

    # Create output directory
    COMMUNES_OUTPUT_DIR.mkdir(exist_ok=True)

    # Write each department file
    total_size = 0
    for dept, features in sorted(by_dept.items()):
        output_file = COMMUNES_OUTPUT_DIR / f"{dept}.geojson"
        geojson = {"type": "FeatureCollection", "features": features}

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False)

        size_kb = output_file.stat().st_size / 1024
        total_size += size_kb
        logger.info(f"  {dept}: {len(features)} communes, {size_kb:.1f} KB")

    # Create index file listing all departments
    index = {
        "departments": sorted(by_dept.keys()),
        "counts": {dept: len(features) for dept, features in by_dept.items()},
    }
    with open(COMMUNES_OUTPUT_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    logger.info(f"Total: {total_size:.1f} KB across {len(by_dept)} files")
    logger.info(f"Index saved to {COMMUNES_OUTPUT_DIR / 'index.json'}")


if __name__ == "__main__":
    main()
