"""
Unified data pipeline for MapViewer.

Orchestrates the complete data processing workflow:
1. ETL: Download data, ingest DVF, create cleaned tables
2. Precompute: Convert shapefiles to GeoJSON, compute statistics (Modular Refactor)
3. Split: Split communes by department for on-demand loading

Usage:
    python -m src.data.pipeline              # Run full pipeline
    python -m src.data.pipeline --step etl   # Run only ETL
    python -m src.data.pipeline --step precompute
    python -m src.data.pipeline --step split
"""

import argparse
import logging
import sys

# Import sibling modules from src.data
from src.data import etl
from src.data import split_communes
from src.data.pipeline import main as precompute_main

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

STEPS = ["etl", "precompute", "split"]


def run_pipeline(step: str = None) -> None:
    """Runs the data pipeline.

    Args:
        step: Optional step to run. If None, runs all steps in order.
    """
    if step is None:
        logger.info("Running FULL pipeline (3 steps)")

    # Step 1: ETL
    if step is None or step == "etl":
        logger.info("[STEP 1/3] ETL - Data extraction and loading")
        etl.main()

    # Step 2: Precompute
    if step is None or step == "precompute":
        logger.info("[STEP 2/3] PRECOMPUTE - GeoJSON conversion and statistics")
        precompute_main.main()

    # Step 3: Split communes
    if step is None or step == "split":
        logger.info("[STEP 3/3] SPLIT - Divide communes by department")
        split_communes.main()

    logger.info("Pipeline complete!")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="MapViewer data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m src.data.pipeline              # Run full pipeline
    python -m src.data.pipeline --step etl   # Run only ETL step
    python -m src.data.pipeline --step precompute
    python -m src.data.pipeline --step split
        """,
    )
    parser.add_argument(
        "--step",
        choices=STEPS,
        help="Run a specific step only. If omitted, runs all steps.",
    )
    args = parser.parse_args()

    try:
        run_pipeline(args.step)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
