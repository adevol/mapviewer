"""
Configuration for MapViewer data processing pipeline.

Contains paths, field mappings, and processing parameters.
Consolidated from scripts/config.py and src/data/etl.py.
"""

from pathlib import Path

# =============================================================================
# Paths
# =============================================================================

DATA_DIR = Path("data")
RAW_DATA_DIR = DATA_DIR / "raw_data"
EXTRACTED_DVF_DIR = DATA_DIR / "dvf_extracted"
DB_PATH = DATA_DIR / "real_estate.duckdb"
OUTPUT_DIR = Path("src/frontend")

# DuckDB Configuration
DUCKDB_MEMORY_LIMIT = "4GB"
DUCKDB_TEMP_DIR = "data/temp"
DUCKDB_THREADS = 4

STATS_OUTPUT = OUTPUT_DIR / "stats_cache.json"
CADASTRE_FILE = DATA_DIR / "cadastre.parquet"

# Admin Express download URL and paths
ADMIN_EXPRESS_URL = (
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/"
    "ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03/"
    "ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03.7z"
)
ADMIN_EXPRESS_DIR = DATA_DIR / "admin_express"

# ADMIN EXPRESS 2025 paths (current commune boundaries)
ADMIN_EXPRESS_SHP_DIR = (
    ADMIN_EXPRESS_DIR
    / "ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03"
    / "ADMIN-EXPRESS"
    / "1_DONNEES_LIVRAISON_2025-02-00021"
    / "ADE_3-2_SHP_LAMB93_FXX-ED2025-02-03"
)

# =============================================================================
# Shapefile Paths
# =============================================================================

SHAPEFILE_PATHS = {
    "region": ADMIN_EXPRESS_SHP_DIR / "REGION.shp",
    "departement": ADMIN_EXPRESS_SHP_DIR / "DEPARTEMENT.shp",
    "canton": ADMIN_EXPRESS_SHP_DIR / "CANTON.shp",
    "commune": ADMIN_EXPRESS_SHP_DIR / "COMMUNE.shp",
    "arrondissement": ADMIN_EXPRESS_SHP_DIR / "ARRONDISSEMENT_MUNICIPAL.shp",
}

# =============================================================================
# Field Mappings (2025 Admin Express column names)
# =============================================================================

CODE_FIELDS = {
    "region": "INSEE_REG",
    "departement": "INSEE_DEP",
    "canton": "INSEE_CAN",
    "commune": "INSEE_COM",
    "arrondissement": "INSEE_ARM",
}

NAME_FIELDS = {
    "region": "NOM",
    "departement": "NOM",
    "canton": "INSEE_CAN",  # Canton shapefile has no NOM field, use code as name
    "commune": "NOM",
    "arrondissement": "NOM",
}

# =============================================================================
# DVF Column Mappings (for stats queries)
# =============================================================================

DVF_COLUMNS = {
    "price": "Valeur fonciere",
    "surface": "Surface reelle bati",
    "mutation_type": "Nature mutation",
    "property_type": "Type local",
    "mutation_date": "Date mutation",
    "dept_code": "Code departement",
    "commune_code": "Code commune",
    "postal_code": "Code postal",
}

# =============================================================================
# Geometry Simplification (tolerances in meters, Lambert 93)
# Higher = more simplified = smaller files
# =============================================================================

SIMPLIFY_TOLERANCE = {
    "country": 1500,
    "region": 300,
    "departement": 150,
    "canton": 100,
    "commune": 75,
    "arrondissement": 50,
}

# =============================================================================
# Price Filtering
# =============================================================================

MIN_SALES_FOR_STATS = 5
VALID_PROPERTY_TYPES = ("Maison", "Appartement")
MIN_PRICE_M2 = 100
MAX_PRICE_M2 = 50000

# =============================================================================
# SQL Expressions for Code Building (for dvf_clean table)
# =============================================================================

INSEE_COMMUNE_EXPR = "dept_code || LPAD(CAST(commune_code AS VARCHAR), 3, '0')"

# Arrondissement to main commune mapping
ARRONDISSEMENT_TO_COMMUNE = {
    # Paris arrondissements (75101-75120) → 75056
    **{f"75{i:03d}": "75056" for i in range(101, 121)},
    # Lyon arrondissements (69381-69389) → 69123
    **{f"69{i:03d}": "69123" for i in range(381, 390)},
    # Marseille arrondissements (13201-13216) → 13055
    **{f"13{i:03d}": "13055" for i in range(201, 217)},
}

# =============================================================================
# Department to Region Mapping (2016 regions)
# =============================================================================

DEPT_TO_REGION = {
    # Auvergne-Rhône-Alpes (84)
    "01": "84",
    "03": "84",
    "07": "84",
    "15": "84",
    "26": "84",
    "38": "84",
    "42": "84",
    "43": "84",
    "63": "84",
    "69": "84",
    "73": "84",
    "74": "84",
    # Bourgogne-Franche-Comté (27)
    "21": "27",
    "25": "27",
    "39": "27",
    "58": "27",
    "70": "27",
    "71": "27",
    "89": "27",
    "90": "27",
    # Bretagne (53)
    "22": "53",
    "29": "53",
    "35": "53",
    "56": "53",
    # Centre-Val de Loire (24)
    "18": "24",
    "28": "24",
    "36": "24",
    "37": "24",
    "41": "24",
    "45": "24",
    # Corse (94)
    "2A": "94",
    "2B": "94",
    # Grand Est (44)
    "08": "44",
    "10": "44",
    "51": "44",
    "52": "44",
    "54": "44",
    "55": "44",
    "57": "44",
    "67": "44",
    "68": "44",
    "88": "44",
    # Hauts-de-France (32)
    "02": "32",
    "59": "32",
    "60": "32",
    "62": "32",
    "80": "32",
    # Île-de-France (11)
    "75": "11",
    "77": "11",
    "78": "11",
    "91": "11",
    "92": "11",
    "93": "11",
    "94": "11",
    "95": "11",
    # Normandie (28)
    "14": "28",
    "27": "28",
    "50": "28",
    "61": "28",
    "76": "28",
    # Nouvelle-Aquitaine (75)
    "16": "75",
    "17": "75",
    "19": "75",
    "23": "75",
    "24": "75",
    "33": "75",
    "40": "75",
    "47": "75",
    "64": "75",
    "79": "75",
    "86": "75",
    "87": "75",
    # Occitanie (76)
    "09": "76",
    "11": "76",
    "12": "76",
    "30": "76",
    "31": "76",
    "32": "76",
    "34": "76",
    "46": "76",
    "48": "76",
    "65": "76",
    "66": "76",
    "81": "76",
    "82": "76",
    # Pays de la Loire (52)
    "44": "52",
    "49": "52",
    "53": "52",
    "72": "52",
    "85": "52",
    # Provence-Alpes-Côte d'Azur (93)
    "04": "93",
    "05": "93",
    "06": "93",
    "13": "93",
    "83": "93",
    "84": "93",
    # Overseas
    "971": "01",
    "972": "02",
    "973": "03",
    "974": "04",
    "976": "06",
}

# =============================================================================
# Split Communes Configuration
# =============================================================================

COMMUNES_INPUT_FILE = OUTPUT_DIR / "communes.geojson"
COMMUNES_OUTPUT_DIR = OUTPUT_DIR / "communes"
