import datetime
import importlib.util
import json
import shutil
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

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
    from src.transformations import gold as gold_tf
    from src.runtime_env import (
        build_namespace_config,
        initialize_namespace,
        resolve_runtime_environment,
    )
except ModuleNotFoundError:
    gold_tf = load_module_from_path(
        "gold_transformations_fallback",
        TRANSFORMATIONS_DIR / "gold.py",
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


def resolve_config_path() -> Path:
    """Résout config.yml en local, en Repo Databricks, ou depuis un notebook."""
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
        "config.yml introuvable. Emplacements testés :\n - " + searched
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
    p = cfg["storage"]["community"]
    SILVER_PATH = p["silver"]
    GOLD_PATH = p["gold"]
    STORAGE_FORMAT = "delta"
elif env == "local":
    p = cfg["storage"]["local"]

    def _local_path(path_value: str) -> str:
        return str((PROJECT_ROOT / path_value).resolve())

    SILVER_PATH = _local_path(p["silver"]) + "/"
    GOLD_PATH = _local_path(p["gold"]) + "/"
    STORAGE_FORMAT = p.get("format", "parquet")
elif env == "azure":
    az = cfg["storage"]["azure"]
    acc = az["storage_account"]

    def _abfss(container_name: str) -> str:
        return f"abfss://{container_name}@{acc}.dfs.core.windows.net/"

    SILVER_PATH = _abfss(az["container_silver"])
    GOLD_PATH = _abfss(az["container_gold"])
    STORAGE_FORMAT = "delta"
else:
    raise ValueError(f"Environnement inconnu dans config.yml : {env}")

SILVER_NAMESPACE = build_namespace_config(cfg, "silver")
NAMESPACE = build_namespace_config(cfg, "gold")
TBL = cfg["database"]
SILVER_SUFFIX_TO_TABLE_KEY = {
    "stations": "silver_stations",
    "mesures": "silver_mesures",
    "conformite": "silver_conformite",
}
GOLD_SUFFIX_TO_TABLE_KEY = {
    "conformite_commune": "gold_conformite_commune",
    "evolution_parametres": "gold_evolution_parametres",
    "qualite_region": "gold_qualite_region",
    "top10_communes": "gold_top10_communes",
    "non_conformites": "gold_non_conformites",
}
LOCAL_SILVER_MANIFEST_PATH = (
    (PROJECT_ROOT / "logs" / "silver_local_latest.json")
    if env == "local"
    else None
)
LOCAL_SILVER_OUTPUTS: dict[str, str] = {}

try:
    spark  # type: ignore[name-defined]
except NameError:
    warehouse_dir = str((PROJECT_ROOT / "spark-warehouse").resolve())
    builder = (
        SparkSession.builder
        .appName("water_quality_gold_ingestion")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.warehouse.dir", warehouse_dir)
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


def ensure_local_output_base(path_value: str, fallback_name: str) -> str:
    target = Path(path_value)
    try:
        target.mkdir(parents=True, exist_ok=True)
        probe = target / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return str(target).replace("\\", "/") + "/"
    except (PermissionError, OSError):
        fallback = PROJECT_ROOT / fallback_name
        fallback.mkdir(parents=True, exist_ok=True)
        print(
            f"⚠️ Chemin local non inscriptible: {target}. "
            f"Bascule vers {fallback}"
        )
        return str(fallback.resolve()).replace("\\", "/") + "/"


def read_dataset(base_path: str, suffix: str):
    if env == "local" and base_path == SILVER_PATH and suffix in LOCAL_SILVER_OUTPUTS:
        return spark.read.format(STORAGE_FORMAT).load(LOCAL_SILVER_OUTPUTS[suffix])
    if env == "azure":
        if base_path == SILVER_PATH:
            return spark.table(SILVER_NAMESPACE.fq_table(TBL[SILVER_SUFFIX_TO_TABLE_KEY[suffix]]))
        if base_path == GOLD_PATH:
            return spark.table(NAMESPACE.fq_table(TBL[GOLD_SUFFIX_TO_TABLE_KEY[suffix]]))
    return spark.read.format(STORAGE_FORMAT).load(f"{base_path}{suffix}/")


def write_dataset(df, suffix: str, partition_cols=None, table_key: str | None = None):
    partition_cols = partition_cols or []
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

    dest = f"{GOLD_PATH}{suffix}/"
    if env == "local":
        dest_path = Path(dest)
        if cfg["ingestion"]["write_mode"] == "overwrite" and dest_path.exists():
            shutil.rmtree(dest_path, ignore_errors=True)

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


def create_table_if_needed(table_key: str, dest: str) -> None:
    if env == "azure":
        return
    if env != "local":
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {NAMESPACE.fq_table(TBL[table_key])} "
            f"USING DELTA LOCATION '{dest}'"
        )


if env != "local":
    initialize_namespace(spark, NAMESPACE)

if env == "local":
    GOLD_PATH = ensure_local_output_base(GOLD_PATH, "gold")
    if LOCAL_SILVER_MANIFEST_PATH and LOCAL_SILVER_MANIFEST_PATH.exists():
        manifest = json.loads(LOCAL_SILVER_MANIFEST_PATH.read_text(encoding="utf-8"))
        LOCAL_SILVER_OUTPUTS = {
            key: value
            for key, value in manifest.get("datasets", {}).items()
            if isinstance(value, str)
        }

print(f"✅ Config chargée | env={env}")
print(f"   SILVER : {SILVER_PATH}")
print(f"   GOLD   : {GOLD_PATH}")
print(f"   Namespace : {NAMESPACE.namespace_display}")
if NAMESPACE.external_location:
    print(f"   External location : {NAMESPACE.external_location}")
print(f"   FORMAT : {STORAGE_FORMAT}")
if env == "local" and LOCAL_SILVER_OUTPUTS:
    print(f"   SILVER manifest local : {LOCAL_SILVER_MANIFEST_PATH}")


df_silver_stations = read_dataset(SILVER_PATH, "stations")
df_silver_mesures = read_dataset(SILVER_PATH, "mesures")
df_silver_conformite = read_dataset(SILVER_PATH, "conformite")

print("📥 Silver chargé :")
print(f"   silver.stations   : {df_silver_stations.count():>10,} lignes")
print(f"   silver.mesures    : {df_silver_mesures.count():>10,} lignes")
print(f"   silver.conformite : {df_silver_conformite.count():>10,} lignes")


df_communes_geo = gold_tf.build_communes_geo(df_silver_stations)
df_region_population = gold_tf.build_region_population(df_communes_geo)


# ------------------------------------------------------------------
# 1. Gold — Conformité par commune
# ------------------------------------------------------------------
df_gold_conformite_commune = gold_tf.build_conformite_commune(
    df_silver_conformite,
    df_communes_geo,
)

dest_conformite_commune = write_dataset(
    df_gold_conformite_commune,
    "conformite_commune",
    ["annee_prelevement", "code_departement"],
    table_key="gold_conformite_commune",
)
create_table_if_needed("gold_conformite_commune", dest_conformite_commune)
print(f"✅ gold.conformite_commune   : {df_gold_conformite_commune.count():,} lignes → {dest_conformite_commune}")


# ------------------------------------------------------------------
# 2. Gold — Evolution temporelle des paramètres
# ------------------------------------------------------------------
df_gold_evolution_parametres = gold_tf.build_evolution_parametres(
    df_silver_mesures,
    df_communes_geo,
)

dest_evolution_parametres = write_dataset(
    df_gold_evolution_parametres,
    "evolution_parametres",
    ["annee_prelevement", "code_departement"],
    table_key="gold_evolution_parametres",
)
create_table_if_needed("gold_evolution_parametres", dest_evolution_parametres)
print(f"✅ gold.evolution_parametres : {df_gold_evolution_parametres.count():,} lignes → {dest_evolution_parametres}")


# ------------------------------------------------------------------
# 3. Gold — Carte de qualité par région
# ------------------------------------------------------------------
df_gold_qualite_region = gold_tf.build_qualite_region(
    df_silver_conformite,
    df_communes_geo,
    df_region_population,
)

dest_qualite_region = write_dataset(
    df_gold_qualite_region,
    "qualite_region",
    ["annee_prelevement"],
    table_key="gold_qualite_region",
)
create_table_if_needed("gold_qualite_region", dest_qualite_region)
print(f"✅ gold.qualite_region       : {df_gold_qualite_region.count():,} lignes → {dest_qualite_region}")


# ------------------------------------------------------------------
# 4. Gold — Top 10 communes les plus / moins conformes
# ------------------------------------------------------------------
df_score_communes = gold_tf.build_score_communes(df_gold_conformite_commune)
df_gold_top10_communes = gold_tf.build_top10_communes(df_score_communes)

dest_top10_communes = write_dataset(df_gold_top10_communes, "top10_communes", table_key="gold_top10_communes")
create_table_if_needed("gold_top10_communes", dest_top10_communes)
print(f"✅ gold.top10_communes       : {df_gold_top10_communes.count():,} lignes → {dest_top10_communes}")


# ------------------------------------------------------------------
# 5. Gold — Analyse des non-conformités
# ------------------------------------------------------------------
df_non_conformes = gold_tf.build_non_conformes(df_silver_conformite)
df_gold_non_conformites = gold_tf.build_non_conformites(
    df_non_conformes,
    df_silver_mesures,
    df_communes_geo,
)

dest_non_conformites = write_dataset(
    df_gold_non_conformites,
    "non_conformites",
    ["annee_prelevement", "code_departement"],
    table_key="gold_non_conformites",
)
create_table_if_needed("gold_non_conformites", dest_non_conformites)
print(f"✅ gold.non_conformites      : {df_gold_non_conformites.count():,} lignes → {dest_non_conformites}")


gold_tables = [
    (TBL["gold_conformite_commune"], dest_conformite_commune),
    (TBL["gold_evolution_parametres"], dest_evolution_parametres),
    (TBL["gold_qualite_region"], dest_qualite_region),
    (TBL["gold_top10_communes"], dest_top10_communes),
    (TBL["gold_non_conformites"], dest_non_conformites),
]

print("=" * 70)
print("📊 RAPPORT DE TRANSFORMATION — COUCHE GOLD")
print("=" * 70)
print(f"  Environnement : {env}")
print(f"  Timestamp     : {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("-" * 70)
print(f"  {'Table':<38} {'Lignes':>12}")
print("-" * 70)

total = 0
for tbl_name, path in gold_tables:
    try:
        if env == "azure":
            n = spark.table(path).count()
        else:
            n = read_dataset(GOLD_PATH, Path(path.rstrip("/\\")).name).count()
        total += n
        print(f"  ✅  {tbl_name:<38} {n:>12,}")
    except Exception as e:
        print(f"  ❌  {tbl_name:<38} ERREUR : {e}")

print("-" * 70)
print(f"  {'TOTAL Gold':<38} {total:>12,}")
print("=" * 70)
print()
print("  Cas d'usage couverts :")
print("   - conformité par commune")
print("   - évolution temporelle des paramètres")
print("   - carte de qualité par région")
print("   - top 10 communes les plus / moins conformes")
print("   - analyse des non-conformités")
