# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ¥ˆ Silver â€” Nettoyage, transformation & standardisation
# MAGIC
# MAGIC | Ã‰tape | Description |
# MAGIC |---|---|
# MAGIC | 1 | Correction des types (dates â†’ timestamp, rÃ©sultats â†’ float) |
# MAGIC | 2 | Suppression des doublons |
# MAGIC | 3 | Normalisation des libellÃ©s et unitÃ©s |
# MAGIC | 4 | Gestion des valeurs manquantes |
# MAGIC | 5 | DÃ©tection et traitement des outliers |
# MAGIC | 6 | CatÃ©gorisation des paramÃ¨tres |
# MAGIC | 7 | Production des tables `silver.stations`, `silver.mesures`, `silver.conformite` |
# MAGIC | 8 | Partitionnement par annÃ©e et dÃ©partement |

# COMMAND ----------
# MAGIC %md ## 0. DÃ©pendances & configuration

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

import yaml, json, re, datetime, math, sys, tempfile, importlib.util
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from pyspark.sql import types as T

def resolve_project_root_for_imports() -> Path:
    candidates = []

    try:
        candidates.append(Path(__file__).resolve())
    except NameError:
        pass

    candidates.append(Path.cwd().resolve())

    for candidate in candidates:
        search_roots = [candidate] + list(candidate.parents)
        for root in search_roots:
            if root.is_dir() and root.name == "src":
                return root.parent
            if (root / "src").is_dir():
                return root
            if (
                root.is_dir()
                and root.name == "files"
                and (root / "src").is_dir()
            ):
                return root

    return Path.cwd().resolve()


PROJECT_ROOT = resolve_project_root_for_imports()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SRC_DIR = PROJECT_ROOT / "src"
TRANSFORMATIONS_DIR = SRC_DIR / "transformations"
for import_path in (SRC_DIR, TRANSFORMATIONS_DIR):
    if str(import_path) not in sys.path:
        sys.path.insert(0, str(import_path))


def load_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(
            f"Impossible de charger le module {module_name!r} depuis {file_path}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

try:
    from src.transformations import silver as silver_tf
    from src.runtime_env import (
        build_namespace_config,
        initialize_namespace,
        resolve_runtime_environment,
    )
except ModuleNotFoundError:
    silver_tf = load_module_from_path(
        "silver_transformations_fallback",
        TRANSFORMATIONS_DIR / "silver.py",
    )
    runtime_env = load_module_from_path(
        "runtime_env_fallback",
        SRC_DIR / "runtime_env.py",
    )
    build_namespace_config = runtime_env.build_namespace_config
    initialize_namespace = runtime_env.initialize_namespace
    resolve_runtime_environment = runtime_env.resolve_runtime_environment

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except AttributeError:
    pass


# ------------------------------------------------------------------
# Chargement du config.yml
# ------------------------------------------------------------------
def resolve_config_path() -> Path:
    """RÃ©sout config/config.yml en local, en Repo Databricks, ou depuis un notebook."""
    candidates = []

    search_roots = [PROJECT_ROOT] + list(PROJECT_ROOT.parents)
    for root in search_roots:
        candidates.extend([
            root / "config" / "config.yml",
            root / "config.yml",
        ])

    cwd = Path.cwd().resolve()
    candidates.extend([
        cwd / "config" / "config.yml",
        cwd / "config.yml",
        cwd.parent / "config" / "config.yml",
        cwd.parent / "config.yml",
        cwd.parent.parent / "config" / "config.yml",
        cwd.parent.parent / "config.yml",
    ])

    if "dbutils" in globals():
        try:
            notebook_path = (
                dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
            workspace_dir = Path("/Workspace") / notebook_path.lstrip("/")
            candidates.extend([
                workspace_dir.parent / "config" / "config.yml",
                workspace_dir.parent / "config.yml",
                workspace_dir.parent.parent / "config" / "config.yml",
                workspace_dir.parent.parent / "config.yml",
            ])
        except Exception:
            pass

    seen = set()
    for candidate in candidates:
        candidate_str = str(candidate)
        if candidate_str in seen:
            continue
        seen.add(candidate_str)
        if candidate.exists():
            return candidate

    searched = "\n - ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "config.yml introuvable. Emplacements testÃ©s :\n - " + searched
    )


CONFIG_PATH = resolve_config_path()
PROJECT_ROOT = (
    CONFIG_PATH.parent.parent
    if CONFIG_PATH.parent.name == "config"
    else CONFIG_PATH.parent
)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

env = resolve_runtime_environment(cfg)

if env == "community":
    p           = cfg["storage"]["community"]
    BRONZE_PATH = p["bronze"]
    SILVER_PATH = p["silver"]
    STORAGE_FORMAT = "delta"
elif env == "local":
    p = cfg["storage"]["local"]

    def _local_path(path_value: str) -> str:
        return str((PROJECT_ROOT / path_value).resolve())

    BRONZE_PATH = _local_path(p["bronze"]) + "/"
    SILVER_PATH = _local_path(p["silver"]) + "/"
    STORAGE_FORMAT = p.get("format", "parquet")
elif env == "azure":
    az  = cfg["storage"]["azure"]
    acc = az["storage_account"]

    def _abfss(container_name: str) -> str:
        return f"abfss://{container_name}@{acc}.dfs.core.windows.net/"

    BRONZE_PATH = _abfss(az["container_bronze"])
    SILVER_PATH = _abfss(az["container_silver"])
    STORAGE_FORMAT = "delta"
else:
    raise ValueError(f"Environnement inconnu dans config.yml : {env}")

BRONZE_NAMESPACE = build_namespace_config(cfg, "bronze")
NAMESPACE = build_namespace_config(cfg, "silver")
TBL = cfg["database"]
BRONZE_SUFFIX_TO_TABLE_KEY = {
    "resultats_dis": "bronze_resultats",
    "communes_udi": "bronze_communes",
    "geo_communes": "bronze_geo",
}
SILVER_SUFFIX_TO_TABLE_KEY = {
    "stations": "silver_stations",
    "mesures": "silver_mesures",
    "conformite": "silver_conformite",
}
LOCAL_RUN_ID = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
LOCAL_OUTPUT_PATHS: dict[str, str] = {}
LOCAL_OUTPUT_MANIFEST_PATH = (
    (PROJECT_ROOT / "logs" / "silver_local_latest.json")
    if env == "local"
    else None
)

try:
    spark  # type: ignore[name-defined]
except NameError:
    warehouse_dir = str((PROJECT_ROOT / "spark-warehouse").resolve())
    builder = (
        SparkSession.builder
        .appName("water_quality_silver_ingestion")
        .master("local[2]" if env == "local" else "local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "8" if env == "local" else "200")
        .config("spark.default.parallelism", "2" if env == "local" else "8")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if STORAGE_FORMAT == "delta":
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    if env != "local":
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()


def display(df):
    df.show(truncate=False)


def ensure_local_output_base(path_value: str, fallback_name: str) -> str:
    target = Path(path_value)
    try:
        target.mkdir(parents=True, exist_ok=True)
        return str(target.resolve()).replace("\\", "/") + "/"
    except (PermissionError, OSError):
        fallback = Path(tempfile.mkdtemp(prefix=f"{fallback_name}_"))
        print(
            f"âš ï¸ Chemin local non inscriptible: {target}. "
            f"Bascule vers {fallback}"
        )
        return str(fallback.resolve()).replace("\\", "/") + "/"


def read_dataset(base_path: str, suffix: str):
    if env == "local" and base_path == SILVER_PATH and suffix in LOCAL_OUTPUT_PATHS:
        return spark.read.format(STORAGE_FORMAT).load(LOCAL_OUTPUT_PATHS[suffix])
    if env == "azure":
        if base_path == BRONZE_PATH:
            table_key = BRONZE_SUFFIX_TO_TABLE_KEY[suffix]
            return spark.table(BRONZE_NAMESPACE.fq_table(TBL[table_key]))
        if base_path == SILVER_PATH:
            table_key = SILVER_SUFFIX_TO_TABLE_KEY[suffix]
            return spark.table(NAMESPACE.fq_table(TBL[table_key]))
    return spark.read.format(STORAGE_FORMAT).load(f"{base_path}{suffix}/")


def persist_local_output_manifest() -> None:
    if env != "local" or LOCAL_OUTPUT_MANIFEST_PATH is None:
        return

    manifest = {
        "run_id": LOCAL_RUN_ID,
        "generated_at_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "base_path": SILVER_PATH,
        "datasets": LOCAL_OUTPUT_PATHS,
    }
    LOCAL_OUTPUT_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_OUTPUT_MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _resolve_local_write_dest(suffix: str) -> str:
    dest = f"{SILVER_PATH}{suffix}/"
    dest_path = Path(dest)
    actual_dest = dest

    if cfg["ingestion"]["write_mode"] == "overwrite" and dest_path.exists():
        try:
            shutil.rmtree(dest_path, ignore_errors=False)
        except (PermissionError, OSError):
            fallback_root = PROJECT_ROOT / "logs" / "silver_runs" / LOCAL_RUN_ID
            fallback_root.mkdir(parents=True, exist_ok=True)
            actual_dest = str((fallback_root / suffix).resolve()).replace("\\", "/") + "/"
            print(
                f"⚠️ Suppression impossible pour {dest_path}. "
                f"Ecriture locale redirigée vers {actual_dest}"
            )

    try:
        Path(actual_dest).mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError):
        fallback_root = PROJECT_ROOT / "logs" / "silver_runs" / LOCAL_RUN_ID
        fallback_root.mkdir(parents=True, exist_ok=True)
        actual_dest = str((fallback_root / suffix).resolve()).replace("\\", "/") + "/"
        Path(actual_dest).mkdir(parents=True, exist_ok=True)
        print(
            f"⚠️ Creation impossible pour {dest_path}. "
            f"Ecriture locale redirigée vers {actual_dest}"
        )

    return actual_dest


def _partition_value_to_path(value) -> str:
    if value is None:
        return "__HIVE_DEFAULT_PARTITION__"
    if isinstance(value, float) and math.isnan(value):
        return "__HIVE_DEFAULT_PARTITION__"
    return str(value).replace("\\", "_").replace("/", "_")


def _write_local_parquet_dataset(df, dest: str, partition_cols: list[str]) -> None:
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    grouped_rows: dict[tuple, list[dict]] = {}

    for row in df.toLocalIterator():
        row_dict = row.asDict(recursive=True)
        if partition_cols:
            partition_key = tuple(row_dict.get(col_name) for col_name in partition_cols)
        else:
            partition_key = tuple()
        grouped_rows.setdefault(partition_key, []).append(row_dict)

    if not grouped_rows:
        return

    for keys_tuple, rows in grouped_rows.items():
        folder = dest_path
        for col_name, part_value in zip(partition_cols, keys_tuple):
            folder = folder / f"{col_name}={_partition_value_to_path(part_value)}"
        folder.mkdir(parents=True, exist_ok=True)

        payload_rows = []
        for row_dict in rows:
            payload = {
                key: value
                for key, value in row_dict.items()
                if key not in partition_cols
            }
            payload_rows.append(payload)

        table = pa.Table.from_pylist(payload_rows)
        pq.write_table(table, folder / "part-00000.parquet")


def write_dataset(df, suffix: str, partition_cols=None, table_key: str | None = None):
    partition_cols = partition_cols or []
    if env == "local":
        dest = _resolve_local_write_dest(suffix)
        _write_local_parquet_dataset(df, dest, partition_cols)
        LOCAL_OUTPUT_PATHS[suffix] = dest
        persist_local_output_manifest()
        return dest

    if env == "azure":
        if not table_key:
            raise ValueError(f"table_key obligatoire pour Azure (suffix={suffix})")
        table_ref = NAMESPACE.fq_table(TBL[table_key])
        writer = (
            df.write
            .format(STORAGE_FORMAT)
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(table_ref)
        return table_ref

    dest = f"{SILVER_PATH}{suffix}/"

    writer = (
        df.write
        .format(STORAGE_FORMAT)
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(dest)
    return dest


if env != "local":
    initialize_namespace(spark, NAMESPACE)

print(f"âœ… Config chargÃ©e | env={env}")
print(f"   BRONZE : {BRONZE_PATH}")
print(f"   SILVER : {SILVER_PATH}")
print(f"   Namespace : {NAMESPACE.namespace_display}")
if NAMESPACE.external_location:
    print(f"   External location : {NAMESPACE.external_location}")
print(f"   FORMAT : {STORAGE_FORMAT}")

if env == "local":
    SILVER_PATH = ensure_local_output_base(
        SILVER_PATH,
        f"silver_spark_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    )
    print(f"   SILVER effectif : {SILVER_PATH}")

# COMMAND ----------
# MAGIC %md ## 1. Chargement des tables Bronze

# COMMAND ----------

df_bronze_resultats = read_dataset(BRONZE_PATH, "resultats_dis")
df_bronze_communes  = read_dataset(BRONZE_PATH, "communes_udi")
df_bronze_geo       = read_dataset(BRONZE_PATH, "geo_communes")

print(f"ðŸ“¥ Bronze chargÃ© :")
print(f"   bronze_resultats_dis : {df_bronze_resultats.count():>10,} lignes | {len(df_bronze_resultats.columns)} colonnes")
print(f"   bronze_communes_udi  : {df_bronze_communes.count():>10,} lignes | {len(df_bronze_communes.columns)} colonnes")
print(f"   bronze_geo_communes  : {df_bronze_geo.count():>10,} lignes | {len(df_bronze_geo.columns)} colonnes")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 1 â€” Correction des types
# MAGIC
# MAGIC | Colonne | Type Bronze | Type Silver | Traitement |
# MAGIC |---|---|---|---|
# MAGIC | `date_prelevement` | string ISO 8601 `"2026-02-26T12:55:00Z"` | `timestamp` | `to_timestamp` |
# MAGIC | `resultat_numerique` | double (peut Ãªtre string) | `double` | cast |
# MAGIC | `resultat_alphanumerique` | string `"<0,01"` | `double` | regex + extraction |
# MAGIC | `debut_alim` | string `"2010-10-16"` | `date` | `to_date` |
# MAGIC | `annee` | string | `integer` | cast |
# MAGIC | `population` | long | `integer` | cast |
# MAGIC | `longitude` / `latitude` | double | `double` | cast + validation |

# COMMAND ----------

# --- Parsing de resultat_alphanumerique : "<0,01" â†’ 0.01 ---
# Cas gÃ©rÃ©s :
#   "<0,01"   â†’ 0.01   (infÃ©rieur Ã  seuil â†’ on prend la valeur seuil)
#   ">100"    â†’ 100.0  (supÃ©rieur Ã  seuil)
#   "0,5"     â†’ 0.5    (virgule dÃ©cimale franÃ§aise)
#   "1.5"     â†’ 1.5    (point dÃ©cimal)
#   "ND"      â†’ null   (non dÃ©tectÃ©)
#   ""  / " " â†’ null

def parse_resultat_alpha_expr(col_name: str):
    """Expression Spark native pour parser `resultat_alphanumerique`."""
    cleaned = F.upper(F.trim(F.col(col_name)))
    normalized = F.regexp_replace(cleaned, r"[<>\u2264\u2265~]", "")
    normalized = F.regexp_replace(normalized, ",", ".")
    invalid_values = ["ND", "NR", "NA", "N/A", ""]
    return (
        F.when(F.col(col_name).isNull(), F.lit(None).cast(T.DoubleType()))
        .when(cleaned.isin(*invalid_values), F.lit(None).cast(T.DoubleType()))
        .when(normalized.rlike(r"^-?\d+(\.\d+)?$"), normalized.cast(T.DoubleType()))
        .otherwise(F.lit(None).cast(T.DoubleType()))
    )


df_typed = silver_tf.build_typed_resultats(df_bronze_resultats)

n_typed = df_typed.count()
print(f"âœ… Ã‰tape 1 â€” Typage : {n_typed:,} lignes")

# VÃ©rification
n_null_date     = df_typed.filter(F.col("date_prelevement_ts").isNull()).count()
n_null_resultat = df_typed.filter(F.col("resultat_parse").isNull()).count()
print(f"   Dates null aprÃ¨s parsing  : {n_null_date:,} ({100*n_null_date/n_typed:.1f}%)")
print(f"   RÃ©sultats null            : {n_null_resultat:,} ({100*n_null_resultat/n_typed:.1f}%)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 2 â€” Suppression des doublons
# MAGIC
# MAGIC **ClÃ© de dÃ©duplication** :
# MAGIC `code_prelevement` + `code_parametre` + `date_prelevement`
# MAGIC
# MAGIC En cas de doublon, on conserve la ligne avec `_bronze_ingestion_ts` la plus rÃ©cente.

# COMMAND ----------

# FenÃªtre de dÃ©duplication : 1 seule ligne par (prÃ©lÃ¨vement, paramÃ¨tre, date)
df_dedup = silver_tf.deduplicate_resultats(df_typed)

n_dedup    = df_dedup.count()
n_supprimes = n_typed - n_dedup

print(f"âœ… Ã‰tape 2 â€” DÃ©duplication :")
print(f"   Avant : {n_typed:,} lignes")
print(f"   AprÃ¨s : {n_dedup:,} lignes")
print(f"   Doublons supprimÃ©s : {n_supprimes:,} ({100*n_supprimes/n_typed:.2f}%)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 3 â€” Normalisation des libellÃ©s et unitÃ©s
# MAGIC
# MAGIC - Noms de communes en **MAJUSCULES normalisÃ©es** (suppression accents, espaces parasites)
# MAGIC - LibellÃ©s paramÃ¨tres en **MAJUSCULES** (colonne `libelle_parametre_norm`)
# MAGIC - UnitÃ©s standardisÃ©es (ex: `Âµg/L` â†’ `ug/L`, `mg/l` â†’ `mg/L`)
# MAGIC - Codes conformitÃ© normalisÃ©s : `"C"` / `"N"` / `"S"` / `null`

# COMMAND ----------

# Table de mapping des unitÃ©s non standardisÃ©es
UNITE_MAP = {
    "Âµg/l": "Âµg/L",
    "ug/l": "Âµg/L",
    "mg/l": "mg/L",
    "MG/L": "mg/L",
    "ÂµS/cm": "ÂµS/cm",
    "us/cm": "ÂµS/cm",
    "ntu":   "NTU",
    "NFU":   "NTU",
}

# Table de mapping des codes conformitÃ©
CONFORMITE_MAP = {
    "C": "C",     # Conforme
    "N": "N",     # Non conforme
    "S": "S",     # Sans objet
    "c": "C",
    "n": "N",
    "s": "S",
}


def _map_literal(mapping: dict[str, str]):
    pairs = []
    for key, value in mapping.items():
        pairs.extend([F.lit(key), F.lit(value)])
    return F.create_map(*pairs)


UNITE_MAP_EXPR = _map_literal(UNITE_MAP)
CONFORMITE_MAP_EXPR = _map_literal(CONFORMITE_MAP)


def normalise_unite_expr(col_name: str):
    trimmed = F.trim(F.col(col_name))
    return F.when(F.col(col_name).isNull(), F.lit(None)).otherwise(
        F.coalesce(F.element_at(UNITE_MAP_EXPR, trimmed), trimmed)
    )


def normalise_conformite_expr(col_name: str):
    trimmed = F.trim(F.col(col_name))
    upper_trimmed = F.upper(trimmed)
    return F.when(F.col(col_name).isNull(), F.lit(None)).otherwise(
        F.coalesce(F.element_at(CONFORMITE_MAP_EXPR, trimmed), upper_trimmed)
    )


df_norm = silver_tf.normalise_resultats(df_dedup)

print("âœ… Ã‰tape 3 â€” Normalisation libellÃ©s et unitÃ©s")
print("   UnitÃ©s distinctes aprÃ¨s normalisation :")
df_norm.select("libelle_unite_norm").distinct().show(20, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 4 â€” Gestion des valeurs manquantes
# MAGIC
# MAGIC ### StratÃ©gie par colonne
# MAGIC
# MAGIC | Colonne | % null tolÃ©rÃ© | StratÃ©gie si dÃ©passÃ© |
# MAGIC |---|---|---|
# MAGIC | `date_prelevement_ts` | 0% | Suppression de la ligne |
# MAGIC | `code_commune` | 0% | Suppression de la ligne |
# MAGIC | `code_parametre` | 0% | Suppression de la ligne |
# MAGIC | `resultat_parse` | 10% | Conserver avec flag `_resultat_manquant` |
# MAGIC | `libelle_unite_norm` | 5% | Remplir par `"INCONNUE"` |
# MAGIC | `nom_distributeur` | 20% | Conserver null (non bloquant) |
# MAGIC | `reference_qualite_parametre` | 80% | Conserver null (souvent absent) |

# COMMAND ----------

SEUIL_NULL_BLOQUANT = cfg["quality"]["non_null_min_pct"]   # 0.95 â†’ 95%

# --- Colonnes critiques : suppression de la ligne si null ---
cols_critiques = ["date_prelevement_ts", "code_commune", "code_parametre"]

df_null_handled, missing_value_stats = silver_tf.handle_missing_values(
    df_norm,
    SEUIL_NULL_BLOQUANT,
)
n_suppr_critiques = missing_value_stats["n_suppr_critiques"]

# --- Rapport sur les nulls ---
print(f"âœ… Ã‰tape 4 â€” Valeurs manquantes :")
print(f"   Lignes supprimÃ©es (colonnes critiques nulles) : {n_suppr_critiques:,}")
print(f"   Lignes conservÃ©es avec flag _resultat_manquant : "
      f"{df_null_handled.filter(F.col('_resultat_manquant')).count():,}")
print()
print("   Taux de nullitÃ© par colonne :")
n_total = df_null_handled.count()
for col in ["resultat_parse", "libelle_unite_norm", "nom_distributeur",
            "reference_qualite_parametre", "code_reseau"]:
    if col in df_null_handled.columns:
        n_null = df_null_handled.filter(F.col(col).isNull()).count()
        pct    = 100 * n_null / n_total if n_total > 0 else 0
        flag   = "âš ï¸ " if pct > (1 - SEUIL_NULL_BLOQUANT) * 100 else "  "
        print(f"   {flag} {col:<45} {n_null:>8,} nulls ({pct:.1f}%)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 5 â€” DÃ©tection et traitement des outliers
# MAGIC
# MAGIC **MÃ©thode** : IQR (Interquartile Range) par `code_parametre`
# MAGIC - Borne basse  = Q1 âˆ’ 3 Ã— IQR
# MAGIC - Borne haute  = Q3 + 3 Ã— IQR  (seuil conservateur pour donnÃ©es rÃ©glementaires)
# MAGIC
# MAGIC **StratÃ©gie** : on **conserve** les outliers mais on les **flague** (`_est_outlier = true`).
# MAGIC On ne supprime jamais â€” une valeur hors norme peut Ãªtre une vraie non-conformitÃ©.

# COMMAND ----------

# Calcul Q1, Q3, IQR par paramÃ¨tre
df_outliers = silver_tf.flag_outliers(df_null_handled)

n_outliers = df_outliers.filter(F.col("_est_outlier")).count()
n_total    = df_outliers.count()
print(f"âœ… Ã‰tape 5 â€” Outliers (mÃ©thode IQR Ã— 3) :")
print(f"   Outliers dÃ©tectÃ©s : {n_outliers:,} ({100*n_outliers/n_total:.2f}%)")
print(f"   â†’ ConservÃ©s avec flag _est_outlier = true")
print()

# Top 5 paramÃ¨tres avec le plus d'outliers
print("   Top 5 paramÃ¨tres avec le plus d'outliers :")
(
    df_outliers
    .filter(F.col("_est_outlier"))
    .groupBy("libelle_parametre_norm")
    .count()
    .orderBy(F.desc("count"))
    .limit(5)
    .show(truncate=False)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 6 â€” CatÃ©gorisation des paramÃ¨tres
# MAGIC
# MAGIC Les paramÃ¨tres sont classÃ©s en 3 catÃ©gories rÃ©glementaires + 1 "autre" :
# MAGIC
# MAGIC | CatÃ©gorie | Exemples |
# MAGIC |---|---|
# MAGIC | `MICROBIOLOGIE` | E. coli, EntÃ©rocoques, BactÃ©ries coliformes, BactÃ©ries aÃ©robies |
# MAGIC | `CHIMIE` | Nitrates, Nitrites, Pesticides, Aluminium, Plomb, pH, ConductivitÃ© |
# MAGIC | `RADIOACTIVITE` | Tritium, Dose totale indicative |
# MAGIC | `AUTRE` | Tout le reste |

# COMMAND ----------

# Mots-clÃ©s par catÃ©gorie (comparaison sur libelle_parametre_norm en majuscules)
MOTS_MICROBIOLOGIE = [
    "COLI", "COLIFORM", "ENTEROCOQU", "BACTERIE", "BACTERIES",
    "STREPTO", "CLOSTRIDIUM", "LEGIONELLA", "PSEUDOMONAS",
    "GERME", "FLORE", "SPORE", "CAMPYLOBACTER", "CRYPTOSPORIDIUM", "GIARDIA"
]

MOTS_CHIMIE = [
    "NITRATE", "NITRITE", "PESTICIDE", "HERBICIDE", "INSECTICIDE",
    "ALUMINIUM", "PLOMB", "CUIVRE", "FER", "MANGANESE", "ZINC", "NICKEL",
    "CHROME", "ARSENIC", "MERCURE", "CADMIUM", "FLUORURE", "CHLORURE",
    "SULFATE", "AMMONIUM", "PHOSPHATE", "PH", "CONDUCTIVITE", "CONDUCTIV",
    "TURBIDITE", "COULEUR", "ODEUR", "TEMPERATURE", "CHLORE", "BROMATE",
    "TRIHALOMETHANE", "THM", "BENZENE", "TOLUENE", "XYLENE",
    "MCPA", "ATRAZINE", "GLYPHOSATE", "TRIBUTYLTIN", "HYDROCARBURE"
]

MOTS_RADIOACTIVITE = [
    "TRITIUM", "RADIOACTIVITE", "DOSE TOTALE", "DOSE INDICATIVE",
    "CESIUM", "STRONTIUM", "IODE", "RADON", "URANIUM",
    "ALPHA", "BETA", "GAMMA"
]


def _contains_any(col_expr, keywords: list[str]):
    condition = F.lit(False)
    for keyword in keywords:
        condition = condition | F.contains(col_expr, F.lit(keyword))
    return condition


def categoriser_parametre_expr(col_name: str):
    """Retourne la catÃ©gorie rÃ©glementaire du paramÃ¨tre via Spark natif."""
    lib = F.upper(F.coalesce(F.col(col_name), F.lit("")))
    return (
        F.when(_contains_any(lib, MOTS_RADIOACTIVITE), F.lit("RADIOACTIVITE"))
        .when(_contains_any(lib, MOTS_MICROBIOLOGIE), F.lit("MICROBIOLOGIE"))
        .when(_contains_any(lib, MOTS_CHIMIE), F.lit("CHIMIE"))
        .otherwise(F.lit("AUTRE"))
    )


df_cat = silver_tf.categorise_resultats(df_outliers)

print("âœ… Ã‰tape 6 â€” CatÃ©gorisation des paramÃ¨tres :")
df_cat.groupBy("categorie_parametre").count().orderBy(F.desc("count")).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Ã‰TAPE 7 â€” Production des tables Silver
# MAGIC
# MAGIC ## 7a. `silver.stations`
# MAGIC RÃ©fÃ©rentiel des points de distribution : une ligne par rÃ©seau Ã— commune Ã— quartier.
# MAGIC Enrichi avec les coordonnÃ©es gÃ©ographiques (jointure `bronze_geo`).

# COMMAND ----------

# --- GÃ©o : une ligne par commune ---
df_geo_clean = silver_tf.build_geo_clean(df_bronze_geo)

# --- Stations : depuis communes_udi + enrichissement gÃ©o ---
df_stations = silver_tf.build_stations(df_bronze_communes, df_geo_clean)

dest_stations = write_dataset(df_stations, "stations", table_key="silver_stations")
if env not in ("local", "azure"):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {NAMESPACE.fq_table(TBL['silver_stations'])} "
        f"USING DELTA LOCATION '{dest_stations}'"
    )
n_stations = df_stations.count()
print(f"âœ… silver.stations         : {n_stations:,} lignes â†’ {dest_stations}")

# COMMAND ----------
# MAGIC %md ## 7b. `silver.mesures`
# MAGIC Une ligne par mesure analytique.
# MAGIC Contient les rÃ©sultats typÃ©s, normalisÃ©s, catÃ©gorisÃ©s, avec les flags de qualitÃ©.

# COMMAND ----------

# Colonnes de la table mesures
df_mesures = silver_tf.build_mesures(df_cat)

dest_mesures = write_dataset(df_mesures, "mesures", ["annee_prelevement", "code_departement"], table_key="silver_mesures")
if env not in ("local", "azure"):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {NAMESPACE.fq_table(TBL['silver_mesures'])} "
        f"USING DELTA LOCATION '{dest_mesures}'"
    )
n_mesures = df_mesures.count()
print(f"âœ… silver.mesures          : {n_mesures:,} lignes â†’ {dest_mesures}")

# COMMAND ----------
# MAGIC %md ## 7c. `silver.conformite`
# MAGIC Une ligne par prÃ©lÃ¨vement (agrÃ©gation des rÃ©sultats de conformitÃ©).
# MAGIC Grain : 1 prÃ©lÃ¨vement = 1 ligne avec tous ses indicateurs de conformitÃ©.

# COMMAND ----------

df_conformite = silver_tf.build_conformite(df_cat)

dest_conformite = write_dataset(df_conformite, "conformite", ["annee_prelevement", "code_departement"], table_key="silver_conformite")
if env not in ("local", "azure"):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {NAMESPACE.fq_table(TBL['silver_conformite'])} "
        f"USING DELTA LOCATION '{dest_conformite}'"
    )
n_conformite = df_conformite.count()
print(f"silver.conformite : {n_conformite:,} lignes -> {dest_conformite}")

# COMMAND ----------
# STEP 8 - Verification du partitionnement

# COMMAND ----------
print("ðŸ“‚ Partitions silver.mesures (annÃ©e Ã— dÃ©partement) :")
display(
    read_dataset(SILVER_PATH, "mesures")
    .groupBy("annee_prelevement", "code_departement")
    .count()
    .orderBy("annee_prelevement", "code_departement")
)

# COMMAND ----------

print("ðŸ“‚ Partitions silver.conformite :")
display(
    read_dataset(SILVER_PATH, "conformite")
    .groupBy("annee_prelevement", "code_departement", "conformite_globale")
    .count()
    .orderBy("annee_prelevement", "code_departement")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Rapport Silver

# COMMAND ----------

tables_silver = [
    (TBL["silver_stations"],   dest_stations),
    (TBL["silver_mesures"],    dest_mesures),
    (TBL["silver_conformite"], dest_conformite),
]

print("=" * 70)
print("ðŸ“Š  RAPPORT DE TRANSFORMATION â€” COUCHE SILVER")
print("=" * 70)
print(f"  Environnement : {env}")
print(f"  Timestamp     : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("-" * 70)
print(f"  {'Ã‰tape':<45} {'RÃ©sultat'}")
print("-" * 70)
print(f"  {'1. Typage (dates, rÃ©sultats)':<45} {n_typed:>10,} lignes")
print(f"  {'2. DÃ©duplication':<45} -{n_supprimes:>9,} doublons supprimÃ©s")
print(f"  {'3. Normalisation libellÃ©s/unitÃ©s':<45} {'OK':>10}")
print(f"  {'4. Valeurs manquantes':<45} -{n_suppr_critiques:>9,} lignes critiques nulles")
print(f"  {'5. Outliers flaguÃ©s (IQRÃ—3)':<45} {n_outliers:>10,} outliers conservÃ©s")
print(f"  {'6. CatÃ©gorisation paramÃ¨tres':<45} {'OK':>10}")
print("-" * 70)
print(f"  {'Table':<45} {'Lignes':>10}  Partition")
print("-" * 70)

total = 0
partitions_info = {
    TBL["silver_stations"]:   "â€”",
    TBL["silver_mesures"]:    "annee Ã— dept",
    TBL["silver_conformite"]: "annee Ã— dept",
}
for tbl_name, path in tables_silver:
    try:
        if env == "local":
            dataset_name = Path(path.rstrip("/\\")).name
            n = read_dataset(SILVER_PATH, dataset_name).count()
        elif env == "azure":
            n = spark.table(path).count()
        else:
            n = spark.read.format(STORAGE_FORMAT).load(path).count()
        total += n
        print(f"  âœ…  {tbl_name:<45} {n:>10,}  {partitions_info.get(tbl_name,'')}")
    except Exception as e:
        print(f"  âŒ  {tbl_name:<45} ERREUR : {e}")

print("-" * 70)
print(f"  {'TOTAL Silver':<45} {total:>10,}")
print("=" * 70)
print()
print("  Prochaine Ã©tape â†’ notebook Gold : agrÃ©gations & KPIs")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## âœ… Transformation Silver terminÃ©e
# MAGIC
# MAGIC | Table | Grain | Partition | Flags qualitÃ© |
# MAGIC |---|---|---|---|
# MAGIC | `silver.stations`   | 1 ligne / rÃ©seau Ã— commune Ã— quartier | â€” | `_coords_valides` |
# MAGIC | `silver.mesures`    | 1 ligne / mesure analytique           | annÃ©e Ã— dÃ©partement | `_resultat_manquant`, `_est_outlier` |
# MAGIC | `silver.conformite` | 1 ligne / prÃ©lÃ¨vement                 | annÃ©e Ã— dÃ©partement | `conformite_globale` |
# MAGIC
# MAGIC ### Champs techniques reportÃ©s en Silver
# MAGIC - `reseaux` reste en **JSON string** â†’ sera explosÃ© en Gold si besoin
# MAGIC - `resultat_alphanumerique` conservÃ© pour traÃ§abilitÃ© (`"<0,01"`)
# MAGIC - `_bronze_ingestion_ts` conservÃ© pour la lignÃ©e des donnÃ©es

