# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ¥‰ Bronze â€” Ingestion via APIs Hub'Eau + GÃ©o API
# MAGIC
# MAGIC | API | Endpoint | Lignes (Pau) | ParticularitÃ© |
# MAGIC |---|---|---|---|
# MAGIC | `hubeau_communes`  | `/communes_udi`    | ~22      | SchÃ©ma plat |
# MAGIC | `hubeau_resultats` | `/resultats_dis`   | ~25 898  | Champ `reseaux` imbriquÃ© â†’ JSON string |
# MAGIC | `geo_communes`     | `geo.api.gouv.fr`  | ~1       | GeoJSON â†’ coordonnÃ©es extraites |
# MAGIC
# MAGIC **Tout le paramÃ©trage est dans `config.yml` â€” aucune valeur en dur ici.**

# COMMAND ----------
# MAGIC %md ## 0. DÃ©pendances

# COMMAND ----------

# MAGIC %pip install pyyaml requests --quiet

# COMMAND ----------
# MAGIC %md ## 1. Chargement du config.yml

# COMMAND ----------

import atexit
import argparse
import datetime
import json
import re
import shutil
import sys
import tempfile
import time
import yaml
from pathlib import Path
import requests
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

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

try:
    from src.transformations import bronze_geo as bronze_geo_tf
    from src.runtime_env import (
        build_namespace_config,
        initialize_namespace,
        resolve_runtime_environment,
    )
except ModuleNotFoundError:
    import bronze_geo as bronze_geo_tf
    from runtime_env import (
        build_namespace_config,
        initialize_namespace,
        resolve_runtime_environment,
    )

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except AttributeError:
    pass

TEMP_DIRS_TO_CLEAN: list[Path] = []
SUPPORTED_APIS = ("all", "geo_communes", "hubeau_communes", "hubeau_resultats")


def parse_runtime_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--api",
        choices=SUPPORTED_APIS,
        default="all",
        help="Exécute une seule API Bronze ou 'all' pour le run complet.",
    )
    args, _ = parser.parse_known_args(argv)
    return args


RUNTIME_ARGS = parse_runtime_args(sys.argv[1:])


def should_run_api(api_key: str) -> bool:
    return RUNTIME_ARGS.api in ("all", api_key)
# ------------------------------------------------------------------
# âš™ï¸  Seul paramÃ¨tre en dur : le chemin vers le fichier de config
# ------------------------------------------------------------------
def resolve_config_path() -> Path:
    """RÃ©sout config.yml en local, en Repo Databricks, ou depuis un notebook."""
    candidates = []

    try:
        script_path = Path(__file__).resolve()
        project_root = script_path.parents[2]
        candidates.extend([
            project_root / "config" / "config.yml",
            project_root / "config.yml",
        ])
    except NameError:
        pass

    cwd = Path.cwd().resolve()
    candidates.extend([
        cwd / "config" / "config.yml",
        cwd / "config.yml",
        cwd.parent / "config" / "config.yml",
        cwd.parent / "config.yml",
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
# --- RÃ©solution des chemins selon l'environnement ---
env = resolve_runtime_environment(cfg)

if env == "community":
    p           = cfg["storage"]["community"]
    BRONZE_PATH = p["bronze"]
    LOGS_PATH   = p["logs"]
    STORAGE_FORMAT = "delta"

elif env == "local":
    p = cfg["storage"]["local"]

    def _local_path(path_value: str) -> str:
        return str((PROJECT_ROOT / path_value).resolve())

    BRONZE_PATH = _local_path(p["bronze"]) + "/"
    LOGS_PATH   = _local_path(p["logs"]) + "/"
    STORAGE_FORMAT = p.get("format", "parquet")

elif env == "azure":
    az = cfg["storage"]["azure"]
    acc = az["storage_account"]
    def _abfss(c): return f"abfss://{c}@{acc}.dfs.core.windows.net/"
    BRONZE_PATH = _abfss(az["container_bronze"])
    LOGS_PATH   = _abfss(az["container_logs"])
    STORAGE_FORMAT = "delta"

else:
    raise ValueError(f"Environnement inconnu dans config.yml : {env}")

NAMESPACE = build_namespace_config(cfg, "bronze")
TBL = cfg["database"]   # raccourci pour accÃ©der aux noms de tables

print(f"âœ… Config chargÃ©e | env={env}")
print(f"   BRONZE : {BRONZE_PATH}")
print(f"   Namespace : {NAMESPACE.namespace_display}")
if NAMESPACE.external_location:
    print(f"   External location : {NAMESPACE.external_location}")
print(f"   FORMAT : {STORAGE_FORMAT}")

# ------------------------------------------------------------------
# SparkSession locale si le script n'est pas exÃ©cutÃ© dans Databricks
# ------------------------------------------------------------------
try:
    spark  # type: ignore[name-defined]
except NameError:
    warehouse_dir = str((PROJECT_ROOT / "spark-warehouse").resolve())
    builder = (
        SparkSession.builder
        .appName("water_quality_bronze_ingestion")
        .master("local[2]" if env == "local" else "local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "8" if env == "local" else "200")
        .config("spark.default.parallelism", "2" if env == "local" else "8")
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
    """Fallback local du display Databricks."""
    df.show(truncate=False)


def _cleanup_temp_dirs() -> None:
    for temp_dir in TEMP_DIRS_TO_CLEAN:
        shutil.rmtree(temp_dir, ignore_errors=True)


atexit.register(_cleanup_temp_dirs)


def records_to_spark_df(records: list[dict]):
    """CrÃ©e un DataFrame Spark robuste depuis une liste de dictionnaires."""
    if not records:
        return spark.createDataFrame([], schema=T.StructType([]))

    if env == "local":
        temp_dir = Path(tempfile.mkdtemp(prefix="bronze_json_", dir=PROJECT_ROOT))
        TEMP_DIRS_TO_CLEAN.append(temp_dir)
        jsonl_path = temp_dir / "records.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        return spark.read.json(str(jsonl_path))

    json_rows = [json.dumps(record, ensure_ascii=False) for record in records]
    return spark.read.json(spark.sparkContext.parallelize(json_rows))


def read_bronze_dataset(delta_suffix: str):
    """Lecture par chemin, compatible local et Databricks."""
    return spark.read.format(STORAGE_FORMAT).load(f"{BRONZE_PATH}{delta_suffix}/")


def _prepare_local_output_dir(dest: Path) -> None:
    if cfg["ingestion"]["write_mode"] == "overwrite" and dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)


def _write_local_parquet(records: list, dest: Path, source_api: str,
                         partition_col: Optional[str] = None) -> int:
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    pdf = pd.DataFrame.from_records(records)
    pdf["_ingestion_timestamp"] = now
    pdf["_source_api"] = source_api

    _prepare_local_output_dir(dest)

    if partition_col and partition_col in pdf.columns:
        for part_value, part_df in pdf.groupby(partition_col, dropna=False):
            folder = dest / f"{partition_col}={part_value}"
            folder.mkdir(parents=True, exist_ok=True)
            part_df.to_parquet(folder / "part-00000.parquet", index=False)
    else:
        pdf.to_parquet(dest / "part-00000.parquet", index=False)

    return len(pdf)

# COMMAND ----------
# MAGIC %md ## 2. Helpers HTTP

# COMMAND ----------

http = cfg["ingestion"]["http"]


def _clean_params(params: dict) -> dict:
    """Supprime les entrÃ©es None â€” l'API Hub'Eau ignore les paramÃ¨tres vides."""
    return {k: v for k, v in params.items() if v is not None}


def validate_api_scope(api_key: str, params: dict, pagination: dict) -> None:
    """EmpÃªche les appels trop larges qui donnent l'impression d'un blocage."""
    expected_rows = pagination["page_size"] * pagination["max_pages"]

    if api_key == "hubeau_resultats":
        scope_keys = [
            "code_commune",
            "code_departement",
            "nom_commune",
            "nom_distributeur",
            "nom_moa",
            "code_reseau",
            "date_min_prelevement",
            "date_max_prelevement",
        ]
        has_scope = any(params.get(key) not in (None, "") for key in scope_keys)
        if not has_scope:
            raise ValueError(
                "La requete 'hubeau_resultats' est trop large: aucun filtre "
                "geographique ou temporel n'est defini. Pour eviter un run tres long, "
                "renseigne au minimum 'code_departement' ou une plage de dates dans config.yml."
            )
        if expected_rows > 500_000:
            print(
                f"   âš ï¸  Configuration volumineuse pour {api_key}: "
                f"page_size * max_pages = {expected_rows:,} lignes potentielles"
            )

    if api_key == "geo_communes":
        scope_keys = ["codePostal", "nom", "code", "codeDepartement", "codeRegion"]
        has_scope = any(params.get(key) not in (None, "") for key in scope_keys)
        if not has_scope:
            print(
                "   âš ï¸  geo_communes sans filtre: l'appel peut retourner toutes les communes "
                "et prendre plus de temps."
            )


def _get(url: str, params: dict) -> dict:
    """GET avec retry automatique (paramÃ©trÃ© dans config.yml)."""
    for attempt in range(1, http["retry_attempts"] + 1):
        try:
            r = requests.get(url, params=params,
                             headers=http["headers"],
                             timeout=http["timeout_seconds"])
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"   âš ï¸  Tentative {attempt}/{http['retry_attempts']} : {e}")
            if attempt < http["retry_attempts"]:
                time.sleep(http["retry_delay_s"])
            else:
                raise


def _fetch_geo_departments(api_url: str, params: dict) -> list[str]:
    region_codes = bronze_geo_tf.normalize_code_list(params.get("codeRegion"))
    if region_codes:
        codes: list[str] = []
        for region_code in region_codes:
            region_url = bronze_geo_tf.build_region_departements_url(api_url, region_code)
            raw = _get(region_url, {})
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        codes.extend(bronze_geo_tf.normalize_code_list(item.get("code")))
        return sorted(set(codes))

    departements_url = bronze_geo_tf.build_departements_url(api_url)
    raw = _get(departements_url, {})
    if not isinstance(raw, list):
        return []
    codes: list[str] = []
    for item in raw:
        if isinstance(item, dict):
            codes.extend(bronze_geo_tf.normalize_code_list(item.get("code")))
    return sorted(set(codes))


def fetch_geo_communes(api_cfg: dict, extra: Optional[dict] = None) -> list[dict]:
    url = api_cfg["url"]
    nested = api_cfg.get("nested_fields", {})
    params = _clean_params(api_cfg["params"].copy())

    if extra:
        params.update({k: v for k, v in extra.items() if v is not None})

    validate_api_scope("geo_communes", params, api_cfg["pagination"])

    request_params = bronze_geo_tf.strip_geo_request_params(params)
    direct_scope_keys = ["codePostal", "nom", "code"]
    has_direct_scope = any(params.get(key) not in (None, "") for key in direct_scope_keys)
    department_codes = bronze_geo_tf.normalize_code_list(params.get("codeDepartement"))
    if not department_codes and not has_direct_scope:
        department_codes = _fetch_geo_departments(url, params)

    if not department_codes:
        print(f"\n📡 [geo_communes] {url}")
        print(f"   Params actifs : {request_params}")
        raw = _get(url, request_params)
        if isinstance(raw, dict) and "features" in raw:
            records = bronze_geo_tf.extract_geojson_records(raw, nested)
        elif isinstance(raw, list):
            records = bronze_geo_tf.serialize_nested_fields(raw, nested)
        else:
            records = []
        print(f"✅ [geo_communes] {len(records):,} enregistrements récupérés\n")
        return records

    all_records: list[dict] = []
    print("\n📡 [geo_communes] chargement par département")
    print(f"   Départements ciblés : {', '.join(department_codes)}")

    for department_code in department_codes:
        department_url = bronze_geo_tf.build_department_communes_url(url, department_code)
        print(f"   ↳ département {department_code} ...", end="  ")
        raw = _get(department_url, request_params)
        records = (
            bronze_geo_tf.extract_geojson_records(raw, nested)
            if isinstance(raw, dict) and "features" in raw
            else []
        )
        all_records.extend(records)
        print(f"{len(records):>5} communes  (cumulé {len(all_records):,})")

    deduped_records = {
        record.get("code"): record
        for record in all_records
        if record.get("code") is not None
    }
    final_records = list(deduped_records.values()) if deduped_records else all_records
    print(f"✅ [geo_communes] {len(final_records):,} enregistrements récupérés\n")
    return final_records


def fetch_all(api_key: str, extra: Optional[dict] = None) -> list[dict]:
    """
    RÃ©cupÃ¨re toutes les pages d'une API dÃ©clarÃ©e dans config.yml.

    GÃ¨re trois formats de rÃ©ponse :
      - Hub'Eau  â†’ {"count": N, "data": [...], "next": "url|null"}
      - GÃ©o API  â†’ GeoJSON FeatureCollection
      - Liste    â†’ rÃ©ponse directement une liste

    Les champs imbriquÃ©s dÃ©clarÃ©s dans nested_fields sont sÃ©rialisÃ©s
    en JSON string pour Ãªtre stockÃ©s dans Delta Lake.
    """
    api_cfg      = cfg["apis"][api_key]
    if api_key == "geo_communes":
        return fetch_geo_communes(api_cfg, extra=extra)

    url          = api_cfg["url"]
    pag          = api_cfg["pagination"]
    nested       = api_cfg.get("nested_fields", {})
    params       = _clean_params(api_cfg["params"].copy())

    if extra:
        params.update({k: v for k, v in extra.items() if v is not None})

    validate_api_scope(api_key, params, pag)

    all_records  = []
    page         = params.get("page", 1)
    max_pages    = pag["max_pages"]

    print(f"\nðŸ“¡ [{api_key}] {url}")
    print(f"   Params actifs : { {k:v for k,v in params.items() if k not in ('page','size')} }")

    while page <= max_pages:
        if pag.get("enabled", False):
            params["page"] = page
            params["size"] = pag["page_size"]
        else:
            params.pop("page", None)
            params.pop("size", None)

        print(f"   â†³ page {page:>3} ...", end="  ")
        raw = _get(url, params)

        # --- Parsing selon le format de rÃ©ponse ---
        if isinstance(raw, list):
            # GÃ©o API sans GeoJSON (format=json)
            records = raw
            total   = len(raw)

        elif "features" in raw:
            # GeoJSON FeatureCollection (geo_communes avec format=geojson)
            records = []
            for feat in raw.get("features", []):
                row = dict(feat.get("properties", {}))
                geo = feat.get("geometry")
                if geo and geo.get("coordinates"):
                    row["longitude"] = geo["coordinates"][0]   # -0.3435
                    row["latitude"]  = geo["coordinates"][1]   # 43.3219
                else:
                    row["longitude"] = None
                    row["latitude"]  = None
                records.append(row)
            total = len(records)

        elif "data" in raw:
            # Hub'Eau standard {"count": N, "data": [...]}
            records = raw["data"]
            total   = raw.get("count", 0)

        else:
            records = []
            total   = 0

        # --- SÃ©rialisation des champs imbriquÃ©s (ex: reseaux, codesPostaux) ---
        for field, strategy in nested.items():
            if strategy == "json_string":
                for rec in records:
                    if field in rec and not isinstance(rec[field], str):
                        rec[field] = json.dumps(rec[field], ensure_ascii=False)

        all_records.extend(records)
        print(f"{len(records):>5} enregistrements  (cumulÃ© {len(all_records):,} / {total:,})")

        # Condition d'arrÃªt
        no_next = raw.get("next") is None if isinstance(raw, dict) else True
        if not pag["enabled"] or len(records) < pag["page_size"] or no_next:
            break
        pause_s = float(http.get("request_pause_s", 0))
        if pause_s > 0:
            time.sleep(pause_s)
        page += 1

    print(f"âœ… [{api_key}] {len(all_records):,} enregistrements rÃ©cupÃ©rÃ©s\n")
    return all_records


print("âœ… Helpers HTTP prÃªts")

# COMMAND ----------
# MAGIC %md ## 3. Helper d'Ã©criture Bronze

# COMMAND ----------

def write_bronze(records: list, table_key: str, delta_suffix: str,
                 api_key: str, partition_col: Optional[str] = None) -> int:
    """
    Ã‰crit une liste de records en Delta Lake (couche Bronze).

    - Ajoute les colonnes de mÃ©tadonnÃ©es : _ingestion_timestamp, _source_api
    - Partitionne si partition_col est fourni et prÃ©sent dans le DataFrame
    - Enregistre la table dans le Metastore Hive

    Args:
        records       : donnÃ©es brutes issues de fetch_all()
        table_key     : clÃ© dans cfg["database"] (ex: "bronze_resultats")
        delta_suffix  : sous-dossier dans BRONZE_PATH (ex: "resultats_dis")
        api_key       : clÃ© dans cfg["apis"] (pour la mÃ©tadonnÃ©e _source_api)
        partition_col : colonne de partitionnement (optionnel)

    Returns:
        Nombre de lignes Ã©crites
    """
    if not records:
        print(f"âš ï¸  [{table_key}] Aucun enregistrement â€” skip")
        return 0

    table_name = TBL[table_key]
    dest = f"{BRONZE_PATH}{delta_suffix}/"
    now  = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    df = (
        records_to_spark_df(records)
        .withColumn("_ingestion_timestamp", F.lit(now).cast("timestamp"))
        .withColumn("_source_api",          F.lit(cfg["apis"][api_key]["url"]))
    )

    writer = (
        df.write
        .format(STORAGE_FORMAT)
        .mode(cfg["ingestion"]["write_mode"])
        .option("overwriteSchema", str(cfg["ingestion"]["overwrite_schema"]).lower())
    )

    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
        print(f"   Partitionnement activÃ© sur '{partition_col}'")

    writer.save(dest)

    if env != "local":
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {NAMESPACE.fq_table(table_name)}
            USING DELTA LOCATION '{dest}'
        """)

    n = spark.read.format(STORAGE_FORMAT).load(dest).count()
    target_name = NAMESPACE.fq_table(table_name) if env != "local" else table_name
    print(f"âœ… {target_name} â†’ {n:,} lignes  [{dest}]")
    return n


print("âœ… Helper write_bronze prÃªt")

# COMMAND ----------
# MAGIC %md ## 4. Initialisation de la base de donnÃ©es

# COMMAND ----------

if env != "local":
    initialize_namespace(spark, NAMESPACE)
    print(f"âœ… Namespace '{NAMESPACE.namespace_display}' prêt")
else:
    print("âœ… Mode local Spark : lecture/Ã©criture parquet par Spark, sans metastore")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ingestion — Données géographiques (GeoJSON)
# MAGIC
# MAGIC **Schéma attendu** (extrait depuis GeoJSON FeatureCollection) :
# MAGIC ```
# MAGIC nom | code | codeDepartement | codeRegion | siren | codeEpci
# MAGIC population | codesPostaux (JSON string) | longitude | latitude
# MAGIC ```

# COMMAND ----------

if should_run_api("geo_communes"):
    records_geo = fetch_all("geo_communes")

    write_bronze(
        records      = records_geo,
        table_key    = "bronze_geo",
        delta_suffix = "geo_communes",
        api_key      = "geo_communes",
    )

# COMMAND ----------
# MAGIC %md ## Aperçu geo

# COMMAND ----------

if should_run_api("geo_communes"):
    display(read_bronze_dataset("geo_communes"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Ingestion — Communes / UDI
# MAGIC
# MAGIC **Schéma attendu** (7 colonnes plates) :
# MAGIC ```
# MAGIC code_commune | nom_commune | nom_quartier | code_reseau | nom_reseau | debut_alim | annee
# MAGIC ```

# COMMAND ----------

if should_run_api("hubeau_communes"):
    records_communes = fetch_all("hubeau_communes")

    write_bronze(
        records      = records_communes,
        table_key    = "bronze_communes",
        delta_suffix = "communes_udi",
        api_key      = "hubeau_communes",
        # Pas de partition : ~22 lignes pour Pau
    )

# COMMAND ----------
# MAGIC %md ## Aperçu communes

# COMMAND ----------

if should_run_api("hubeau_communes"):
    display(read_bronze_dataset("communes_udi"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Ingestion — Résultats des analyses sanitaires
# MAGIC
# MAGIC **Schéma attendu** (26 colonnes) avec :
# MAGIC - `date_prelevement` : format ISO 8601 → `2026-02-26T12:55:00Z`
# MAGIC - `reseaux` : champ imbriqué `[ {"code": "...", "nom": "..."} ]` → sérialisé en JSON string
# MAGIC - Partitionnement par `_partition_annee` (extrait de `date_prelevement`)

# COMMAND ----------

# Surcharges runtime (prioritaires sur config.yml, ne pas modifier config.yml)
extra_resultats = {
    # "date_min_prelevement": "2020-01-01",
    # "code_departement":     "64",
}

if should_run_api("hubeau_resultats"):
    records_resultats = fetch_all("hubeau_resultats", extra=extra_resultats)

    # --- Ajout de la colonne de partition par année ---
    # date_prelevement format : "2026-02-26T12:55:00Z"
    if records_resultats:
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        dest = f"{BRONZE_PATH}resultats_dis/"

        df_res = (
            records_to_spark_df(records_resultats)
            .withColumn("_ingestion_timestamp", F.lit(now).cast("timestamp"))
            .withColumn("_source_api", F.lit(cfg["apis"]["hubeau_resultats"]["url"]))
            .withColumn(
                "_partition_annee",
                F.year(F.to_timestamp(F.col("date_prelevement"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            )
        )

        if env == "local":
            # Limite le nombre de writers Parquet simultanés sur une machine locale.
            df_res = df_res.coalesce(1)

        (
            df_res.write
            .format(STORAGE_FORMAT)
            .mode(cfg["ingestion"]["write_mode"])
            .option("overwriteSchema", str(cfg["ingestion"]["overwrite_schema"]).lower())
            .partitionBy("_partition_annee")
            .save(dest)
        )

        if env != "local":
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {NAMESPACE.fq_table(TBL['bronze_resultats'])}
                USING DELTA LOCATION '{dest}'
            """)

        n = spark.read.format(STORAGE_FORMAT).load(dest).count()
        target_name = (
            NAMESPACE.fq_table(TBL["bronze_resultats"])
            if env != "local"
            else TBL["bronze_resultats"]
        )
        print(f"✅ {target_name} → {n:,} lignes (partitionné par année)")
    else:
        print("⚠️  Aucune donnée retournée par hubeau_resultats")

# COMMAND ----------
# MAGIC %md ## Aperçu résultats — colonnes clés

# COMMAND ----------

if should_run_api("hubeau_resultats"):
    display(
        read_bronze_dataset("resultats_dis")
        .select(
            "date_prelevement", "_partition_annee",
            "code_commune", "nom_commune",
            "libelle_parametre", "resultat_alphanumerique", "resultat_numerique", "libelle_unite",
            "limite_qualite_parametre",
            "conformite_limites_bact_prelevement", "conformite_limites_pc_prelevement",
            "reseaux", "_source_api"
        )
        .limit(10)
    )

# COMMAND ----------
# MAGIC %md ## Distribution par année

# COMMAND ----------

if should_run_api("hubeau_resultats"):
    display(
        read_bronze_dataset("resultats_dis")
        .groupBy("_partition_annee")
        .count()
        .orderBy("_partition_annee")
    )

# COMMAND ----------
# MAGIC %md ## 8. Rapport d'ingestion Bronze

# COMMAND ----------

print("=" * 70)
print("📊  RAPPORT D'INGESTION — COUCHE BRONZE")
print("=" * 70)
print(f"  Environnement : {env}")
print(f"  Timestamp     : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("-" * 70)
print(f"  {'Table':<38} {'Lignes':>10}  {'API'}")
print("-" * 70)

tables_check = [
    (TBL["bronze_geo"],       f"{BRONZE_PATH}geo_communes/", "geo_communes"),
    (TBL["bronze_communes"],  f"{BRONZE_PATH}communes_udi/", "hubeau_communes"),
    (TBL["bronze_resultats"], f"{BRONZE_PATH}resultats_dis/", "hubeau_resultats"),
]
tables_check = [
    (tbl_name, path, api_key)
    for tbl_name, path, api_key in tables_check
    if should_run_api(api_key)
]

total = 0
for tbl_name, path, api_key in tables_check:
    try:
        n = spark.read.format(STORAGE_FORMAT).load(path).count()
        total += n
        src = cfg["apis"][api_key]["url"].split("/")[-1]
        print(f"  ✅  {tbl_name:<38} {n:>10,}  /{src}")
    except Exception as e:
        print(f"  ❌  {tbl_name:<38} ERREUR : {e}")

print("-" * 70)
print(f"  {'TOTAL':<38} {total:>10,}")
print("=" * 70)
print()
print("  ⚠️  Note champ 'reseaux' :")
print("     Stocké comme JSON string dans bronze_resultats_dis.")
print("     Il sera parsé et explosé dans la couche Silver.")
print()
print("  Prochaine étape → notebook Silver : nettoyage & standardisation")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Ingestion Bronze terminée
# MAGIC
# MAGIC | Table | Source | Champ spécial | Partition |
# MAGIC |---|---|---|---|
# MAGIC | `bronze_communes_udi`  | Hub'Eau `/communes_udi`   | —                            | — |
# MAGIC | `bronze_resultats_dis` | Hub'Eau `/resultats_dis`  | `reseaux` → JSON string      | `_partition_annee` |
# MAGIC | `bronze_geo_communes`  | Géo API GeoJSON           | `codesPostaux` → JSON string | — |
# MAGIC
# MAGIC **Silver traitera** :
# MAGIC - `reseaux` → explode + jointure sur `code_reseau`
# MAGIC - `date_prelevement` → cast en timestamp propre
# MAGIC - `resultat_alphanumerique` (`<0,01`) → parsing numérique
