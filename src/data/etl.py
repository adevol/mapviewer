"""
ETL script for the MapViewer project.

This script handles:
1.  Downloading Admin Express (referential geometries).
2.  Ingesting DVF data (Real Estate Transactions).
3.  Ingesting Cadastral Parcels (GeoParquet).
4.  Creating a DuckDB database with spatial extensions.

Usage:
    python -m src.data.etl
    python -m src.data.pipeline --step etl
"""

import logging
import zipfile
from pathlib import Path

import duckdb
import py7zr
import requests

from .config import (
    ADMIN_EXPRESS_DIR,
    ADMIN_EXPRESS_URL,
    CADASTRE_FILE,
    DATA_DIR,
    DB_PATH,
    EXTRACTED_DVF_DIR,
    RAW_DATA_DIR,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_admin_express(url: str, output_dir: Path) -> None:
    """Downloads and extracts the Admin Express dataset.

    Args:
        url: The URL of the Admin Express .7z archive.
        output_dir: The directory to extract files into.
    """
    if output_dir.exists() and any(output_dir.iterdir()):
        logger.info("Admin Express directory already exists. Skipping download.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading Admin Express...")
    response = requests.get(url, stream=True, timeout=600)
    response.raise_for_status()

    archive_path = DATA_DIR / "admin_express.7z"
    with open(archive_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("Extracting Admin Express...")
    with py7zr.SevenZipFile(archive_path, "r") as archive:
        archive.extractall(path=output_dir)

    archive_path.unlink()
    logger.info("Admin Express ready.")


def extract_dvf_zips() -> None:
    """Extracts all DVF zip files to a single directory."""
    EXTRACTED_DVF_DIR.mkdir(parents=True, exist_ok=True)

    zip_files = list(RAW_DATA_DIR.glob("valeursfoncieres-*.txt.zip"))
    if not zip_files:
        logger.warning("No DVF zip files found in raw_data.")
        return

    for zf in zip_files:
        logger.info(f"Extracting {zf.name}...")
        with zipfile.ZipFile(zf, "r") as z:
            z.extractall(EXTRACTED_DVF_DIR)

    logger.info(f"Extracted {len(zip_files)} DVF files.")


def init_duckdb() -> duckdb.DuckDBPyConnection:
    """Initializes the DuckDB database and installs spatial extensions.

    Returns:
        The database connection.
    """
    logger.info(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(str(DB_PATH))

    logger.info("Installing and loading spatial extension...")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    return con


def ingest_dvf_data(con: duckdb.DuckDBPyConnection) -> None:
    """Ingests DVF text files from the extracted directory into DuckDB.

    Args:
        con: The database connection.
    """
    # Check if table already exists
    existing = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'dvf'"
    ).fetchone()[0]
    if existing > 0:
        logger.info("DVF table already exists. Skipping ingestion.")
        return

    logger.info("Ingesting DVF data...")
    dvf_glob = str(EXTRACTED_DVF_DIR / "*.txt")

    # DVF uses pipe delimiter, French decimal format (comma), and has header
    con.execute(
        f"""
        CREATE TABLE dvf AS
        SELECT * FROM read_csv(
            '{dvf_glob}',
            delim = '|',
            header = true,
            decimal_separator = ',',
            ignore_errors = true,
            filename = true
        );
    """
    )

    count = con.execute("SELECT COUNT(*) FROM dvf").fetchone()[0]
    logger.info(f"DVF data ingested. Total rows: {count:,}")


def create_dvf_clean(con: duckdb.DuckDBPyConnection) -> None:
    """Creates a cleaned DVF table with deduplicated multi-lot transactions.

    DVF records bulk building sales with the TOTAL price on each lot row.
    E.g., a 42M EUR building sale with 20 apartments shows 42M EUR on each row,
    leading to absurd 1.8M EUR/m2 calculations when dividing by individual surfaces.

    Solution: Group by mutation ID ("Identifiant de document") and sum surfaces,
    keeping the transaction price once. This gives correct price/m2 for bulk sales.

    See docs/pipeline.md for detailed explanation.

    Args:
        con: The database connection.
    """
    # Check if table already exists
    existing = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'dvf_clean'"
    ).fetchone()[0]
    if existing > 0:
        logger.info("dvf_clean table already exists. Skipping creation.")
        return

    logger.info("Creating cleaned DVF table (deduplicating multi-lot transactions)...")

    # "Identifiant de document" is ALWAYS NULL in DVF data, so we cannot use it.
    # Instead, we create a synthetic transaction ID from fields that together
    # identify a unique real estate transaction:
    # - Date mutation: when the sale occurred
    # - Code departement + Code commune: where
    # - No disposition: disposition number (unique within a document/day)
    # - Valeur fonciere: the transaction price
    #
    # Note: We do NOT include "Type local" in the grouping key because a single
    # transaction can include multiple property types (e.g., apartment + parking).
    con.execute(
        """
        CREATE TABLE dvf_clean AS
        SELECT
            -- Synthetic transaction ID (since Identifiant de document is always NULL)
            "Date mutation" || '|' || 
            "Code departement" || '|' ||
            LPAD(CAST("Code commune" AS VARCHAR), 3, '0') || '|' ||
            "No disposition" || '|' ||
            CAST("Valeur fonciere" AS VARCHAR) AS mutation_id,
            
            "Date mutation" AS mutation_date,
            "Nature mutation" AS nature,
            "Code departement" AS dept_code,
            "Code commune" AS commune_code,
            "Code postal" AS postal_code,
            "Commune" AS commune_name,
            -- Note: Type local removed from grouping - a transaction can mix types
            
            -- For multi-lot transactions, price is the same on all rows
            "Valeur fonciere" AS price,
            
            -- Sum surfaces across all lots in the transaction
            SUM("Surface reelle bati") AS total_surface,
            
            -- Count lots in this transaction
            COUNT(*) AS n_lots,
            
            -- Calculated price per m2 (after aggregation)
            "Valeur fonciere" / NULLIF(SUM("Surface reelle bati"), 0) AS price_m2
            
        FROM dvf
        WHERE "Nature mutation" = 'Vente'
        AND "Valeur fonciere" > 0
        AND "Surface reelle bati" > 0
        AND "Type local" IN ('Maison', 'Appartement')
        GROUP BY
            -- Group by a synthetic transaction ID components
            "Date mutation",
            "Code departement",
            "Code commune",
            "No disposition",
            "Valeur fonciere",
            "Code postal",
            "Commune",
            "Nature mutation"
    """
    )

    count = con.execute("SELECT COUNT(*) FROM dvf_clean").fetchone()[0]
    logger.info(f"dvf_clean table created. Total unique transactions: {count:,}")


def ingest_cadastre(con: duckdb.DuckDBPyConnection) -> None:
    """Creates a view for the cadastre parquet file (external query).

    This approach avoids duplicating the 21GB parquet file into the database.
    DuckDB can query parquet files directly with good performance.

    Args:
        con: The database connection.
    """
    if not CADASTRE_FILE.exists():
        logger.error(f"Cadastre file not found: {CADASTRE_FILE}")
        return

    logger.info("Creating external view for Cadastre Parcels...")
    # Use VIEW for external parquet to save disk space
    # The parquet file is already optimized for queries
    con.execute(
        f"""
        CREATE OR REPLACE VIEW parcels AS
        SELECT * FROM read_parquet('{CADASTRE_FILE}');
    """
    )
    count = con.execute("SELECT COUNT(*) FROM parcels").fetchone()[0]
    logger.info(f"Parcels view created. Total count: {count:,}")


def main() -> None:
    """Main ETL execution flow."""
    try:
        # 1. Download Admin Reference (optional, for lower zoom levels)
        download_admin_express(ADMIN_EXPRESS_URL, ADMIN_EXPRESS_DIR)

        # 2. Extract DVF zips
        extract_dvf_zips()

        # 3. Init DB
        con = init_duckdb()

        # 4. Ingest DVF (raw)
        ingest_dvf_data(con)

        # 5. Create cleaned DVF table (deduplicates multi-lot transactions)
        create_dvf_clean(con)

        # 6. Ingest Cadastre
        ingest_cadastre(con)

        con.close()
        logger.info("ETL pipeline completed successfully.")

    except Exception as e:
        logger.exception(f"ETL failed: {e}")
        raise


if __name__ == "__main__":
    main()
