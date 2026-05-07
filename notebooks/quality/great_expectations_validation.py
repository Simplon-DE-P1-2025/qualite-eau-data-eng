# Databricks notebook source
# MAGIC %md
# MAGIC # Great Expectations - Validation qualite Silver
# MAGIC
# MAGIC Installation sur Databricks :
# MAGIC
# MAGIC ```python
# MAGIC %pip install great-expectations==0.18.21
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC Ce notebook valide les tables `silver.stations`, `silver.mesures` et
# MAGIC `silver.conformite`, puis archive les rapports HTML dans `docs/quality/`.

# COMMAND ----------

import datetime
import json
import shutil
import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    import great_expectations as gx
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.data_context import FileDataContext
except ImportError as exc:  # pragma: no cover - message utile en notebook
    raise ImportError(
        "Great Expectations n'est pas installe. "
        "Sur Databricks, executez d'abord "
        "`%pip install great-expectations==0.18.21` puis "
        "`dbutils.library.restartPython()`."
    ) from exc

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except AttributeError:
    pass


def resolve_project_root() -> Path:
    candidates = []

    try:
        candidates.append(Path(__file__).resolve().parents[2])
    except NameError:
        pass

    cwd = Path.cwd().resolve()
    candidates.extend([cwd, cwd.parent, cwd.parent.parent])

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
            candidates.extend([workspace_dir.parent, workspace_dir.parent.parent])
        except Exception:
            pass

    seen = set()
    for candidate in candidates:
        root = candidate.resolve()
        if str(root) in seen:
            continue
        seen.add(str(root))
        if (root / "config" / "config.yml").exists() or (root / "config.yml").exists():
            return root

    searched = "\n - ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "Impossible de retrouver la racine du projet. "
        "config.yml introuvable dans :\n - " + searched
    )


PROJECT_ROOT = resolve_project_root()
CONFIG_PATH = (
    PROJECT_ROOT / "config" / "config.yml"
    if (PROJECT_ROOT / "config" / "config.yml").exists()
    else PROJECT_ROOT / "config.yml"
)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)


def build_storage_paths():
    env = cfg["environment"]

    if env == "community":
        p = cfg["storage"]["community"]
        return env, p["silver"], "delta"

    if env == "local":
        p = cfg["storage"]["local"]
        return env, str((PROJECT_ROOT / p["silver"]).resolve()).replace("\\", "/") + "/", p.get("format", "parquet")

    if env == "azure":
        az = cfg["storage"]["azure"]
        acc = az["storage_account"]
        silver_path = f"abfss://{az['container_silver']}@{acc}.dfs.core.windows.net/"
        return env, silver_path, "delta"

    raise ValueError(f"Environnement inconnu dans config.yml : {env}")


ENV, SILVER_PATH, STORAGE_FORMAT = build_storage_paths()
NON_NULL_MIN_PCT = float(cfg["quality"]["non_null_min_pct"])
VALID_CONFORMITE_VALUES = list(cfg["quality"]["valeurs_conformite"])
PARAM_RANGES = dict(cfg["quality"]["parametres_plages"])

QUALITY_DIR = PROJECT_ROOT / "notebooks" / "quality"
GX_RUNTIME_ROOT = QUALITY_DIR / ".gx_runtime"
DOCS_QUALITY_DIR = PROJECT_ROOT / "docs" / "quality"
DOCS_LATEST_DIR = DOCS_QUALITY_DIR / "latest"
DOCS_ARCHIVE_DIR = DOCS_QUALITY_DIR / "archive"
RUN_TS = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

QUALITY_DIR.mkdir(parents=True, exist_ok=True)
DOCS_LATEST_DIR.mkdir(parents=True, exist_ok=True)
DOCS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

try:
    spark  # type: ignore[name-defined]
except NameError:
    warehouse_dir = str((PROJECT_ROOT / "spark-warehouse").resolve())
    builder = (
        SparkSession.builder
        .appName("water_quality_great_expectations_validation")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )
    spark = builder.getOrCreate()


def read_dataset(suffix: str):
    return spark.read.format(STORAGE_FORMAT).load(f"{SILVER_PATH}{suffix}/")


def reset_gx_context() -> Path:
    shutil.rmtree(GX_RUNTIME_ROOT, ignore_errors=True)
    GX_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    FileDataContext.create(project_root_dir=str(GX_RUNTIME_ROOT))
    return GX_RUNTIME_ROOT


def build_context():
    context_root_dir = reset_gx_context()
    return gx.get_context(project_root_dir=str(context_root_dir))


def type_list(*spark_types: str) -> list[str]:
    return list(spark_types)


EXPECTED_SCHEMAS = {
    "stations": [
        ("code_commune", type_list("StringType")),
        ("nom_commune", type_list("StringType")),
        ("nom_quartier", type_list("StringType")),
        ("code_reseau", type_list("StringType")),
        ("nom_reseau", type_list("StringType")),
        ("debut_alim", type_list("DateType")),
        ("annee", type_list("IntegerType")),
        ("nom_commune_norm", type_list("StringType")),
        ("code_departement_geo", type_list("StringType")),
        ("code_region_geo", type_list("StringType")),
        ("population", type_list("LongType", "IntegerType")),
        ("longitude", type_list("DoubleType", "FloatType")),
        ("latitude", type_list("DoubleType", "FloatType")),
        ("_coords_valides", type_list("BooleanType")),
        ("_silver_timestamp", type_list("TimestampType")),
    ],
    "mesures": [
        ("code_prelevement", type_list("StringType")),
        ("reference_analyse", type_list("StringType")),
        ("code_commune", type_list("StringType")),
        ("nom_commune_norm", type_list("StringType")),
        ("nom_departement_norm", type_list("StringType")),
        ("date_prelevement_ts", type_list("TimestampType")),
        ("mois_prelevement", type_list("IntegerType")),
        ("code_parametre", type_list("StringType")),
        ("code_parametre_se", type_list("StringType")),
        ("code_parametre_cas", type_list("StringType")),
        ("libelle_parametre_norm", type_list("StringType")),
        ("code_type_parametre", type_list("StringType")),
        ("categorie_parametre", type_list("StringType")),
        ("code_lieu_analyse", type_list("StringType")),
        ("resultat_parse", type_list("DoubleType", "FloatType")),
        ("resultat_alphanumerique", type_list("StringType")),
        ("est_sous_seuil", type_list("BooleanType")),
        ("est_sur_seuil", type_list("BooleanType")),
        ("libelle_unite_norm", type_list("StringType")),
        ("limite_qualite_parametre", type_list("StringType")),
        ("reference_qualite_parametre", type_list("StringType")),
        ("nom_distributeur", type_list("StringType")),
        ("nom_uge", type_list("StringType")),
        ("nom_moa", type_list("StringType")),
        ("reseaux", type_list("StringType")),
        ("_resultat_manquant", type_list("BooleanType")),
        ("_est_outlier", type_list("BooleanType")),
        ("_bronze_ingestion_ts", type_list("TimestampType")),
        ("_silver_timestamp", type_list("TimestampType")),
        ("annee_prelevement", type_list("IntegerType")),
        ("code_departement", type_list("IntegerType")),
    ],
    "conformite": [
        ("code_prelevement", type_list("StringType")),
        ("date_prelevement_ts", type_list("TimestampType")),
        ("code_commune", type_list("StringType")),
        ("nom_commune_norm", type_list("StringType")),
        ("nom_departement_norm", type_list("StringType")),
        ("nom_distributeur", type_list("StringType")),
        ("nom_uge", type_list("StringType")),
        ("conformite_bact_limite_norm", type_list("StringType")),
        ("conformite_pc_limite_norm", type_list("StringType")),
        ("conformite_bact_ref_norm", type_list("StringType")),
        ("conformite_pc_ref_norm", type_list("StringType")),
        ("conclusion_conformite_prelevement", type_list("StringType")),
        ("reseaux", type_list("StringType")),
        ("conformite_globale", type_list("StringType")),
        ("_silver_timestamp", type_list("TimestampType")),
        ("annee_prelevement", type_list("IntegerType")),
        ("code_departement", type_list("IntegerType")),
    ],
}

NON_NULL_RULES = {
    "stations": [
        "code_commune",
        "code_reseau",
        "nom_commune_norm",
        "annee",
        "code_departement_geo",
        "code_region_geo",
    ],
    "mesures": [
        "code_prelevement",
        "code_commune",
        "nom_commune_norm",
        "date_prelevement_ts",
        "code_parametre",
        "libelle_parametre_norm",
        "annee_prelevement",
        "code_departement",
    ],
    "conformite": [
        "code_prelevement",
        "date_prelevement_ts",
        "code_commune",
        "nom_commune_norm",
        "conformite_globale",
        "annee_prelevement",
        "code_departement",
    ],
}


def create_validator(context, dataframe, suite_name: str, asset_name: str):
    datasource = context.sources.add_or_update_spark(name=f"{asset_name}_datasource")
    asset = datasource.add_dataframe_asset(name=asset_name, dataframe=dataframe)
    batch_request = asset.build_batch_request()
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    return validator, batch_request


def add_schema_expectations(validator, schema_name: str):
    expected_schema = EXPECTED_SCHEMAS[schema_name]
    expected_columns = [name for name, _ in expected_schema]

    validator.expect_table_columns_to_match_ordered_list(column_list=expected_columns)
    validator.expect_table_column_count_to_equal(value=len(expected_columns))
    validator.expect_table_row_count_to_be_between(min_value=1)

    for column_name, allowed_types in expected_schema:
        validator.expect_column_to_exist(column=column_name)
        validator.expect_column_values_to_be_in_type_list(
            column=column_name,
            type_list=allowed_types,
        )


def add_non_null_expectations(validator, schema_name: str):
    for column_name in NON_NULL_RULES[schema_name]:
        validator.expect_column_values_to_not_be_null(
            column=column_name,
            mostly=NON_NULL_MIN_PCT,
        )


def add_station_expectations(validator):
    validator.expect_column_values_to_not_be_null(column="code_reseau")
    validator.expect_column_values_to_be_between(
        column="latitude",
        min_value=-90.0,
        max_value=90.0,
        mostly=1.0,
    )
    validator.expect_column_values_to_be_between(
        column="longitude",
        min_value=-180.0,
        max_value=180.0,
        mostly=1.0,
    )


def build_station_uniqueness_view(df_stations):
    return df_stations.withColumn(
        "_station_id",
        F.concat_ws(
            "||",
            F.coalesce(F.col("code_commune").cast("string"), F.lit("")),
            F.coalesce(F.col("code_reseau").cast("string"), F.lit("")),
            F.coalesce(F.col("nom_quartier").cast("string"), F.lit("")),
        ),
    )


def add_conformite_expectations(validator):
    validator.expect_column_values_to_be_in_set(
        column="conformite_globale",
        value_set=VALID_CONFORMITE_VALUES,
    )

    for column_name in [
        "conformite_bact_limite_norm",
        "conformite_pc_limite_norm",
        "conformite_bact_ref_norm",
        "conformite_pc_ref_norm",
    ]:
        validator.expect_column_values_to_be_in_set(
            column=column_name,
            value_set=VALID_CONFORMITE_VALUES,
            mostly=0.95,
        )


def build_mesures_quality_views(df_mesures):
    normalized = F.upper(F.coalesce(F.col("libelle_parametre_norm"), F.lit("")))

    return {
        "pH": df_mesures.filter((normalized == "PH") & F.col("resultat_parse").isNotNull()),
        "nitrates_mg_l": df_mesures.filter(
            normalized.contains("NITRATES (EN NO3)") & F.col("resultat_parse").isNotNull()
        ),
        "nitrites_mg_l": df_mesures.filter(
            normalized.contains("NITRITES (EN NO2)") & F.col("resultat_parse").isNotNull()
        ),
        "conductivite": df_mesures.filter(
            normalized.contains("CONDUCTIVIT") & F.col("resultat_parse").isNotNull()
        ),
        "turbidite_ntu": df_mesures.filter(
            normalized.contains("TURBIDIT") & F.col("resultat_parse").isNotNull()
        ),
    }


def run_checkpoint(context, batch_request, suite_name: str, checkpoint_name: str):
    checkpoint = Checkpoint(
        name=checkpoint_name,
        run_name_template=f"{RUN_TS}-{checkpoint_name}",
        data_context=context,
        batch_request=batch_request,
        expectation_suite_name=suite_name,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    )
    context.add_or_update_checkpoint(checkpoint=checkpoint)
    return checkpoint.run()


def result_to_summary(name: str, result):
    success = getattr(result, "success", None)
    if success is None and isinstance(result, dict):
        success = result.get("success")

    payload = {
        "validation_name": name,
        "success": bool(success),
    }

    to_json_dict = getattr(result, "to_json_dict", None)
    if callable(to_json_dict):
        payload["checkpoint_result"] = to_json_dict()
    else:
        payload["checkpoint_result"] = str(result)

    return payload


def archive_data_docs():
    source_site = GX_RUNTIME_ROOT / "great_expectations" / "uncommitted" / "data_docs" / "local_site"
    target_archive = DOCS_ARCHIVE_DIR / RUN_TS

    shutil.rmtree(DOCS_LATEST_DIR, ignore_errors=True)
    if source_site.exists():
        shutil.copytree(source_site, DOCS_LATEST_DIR)
        shutil.copytree(source_site, target_archive)
    else:
        DOCS_LATEST_DIR.mkdir(parents=True, exist_ok=True)
        target_archive.mkdir(parents=True, exist_ok=True)

    return target_archive


def write_summary_files(target_archive: Path, summaries: list[dict]):
    payload = {
        "generated_at_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "environment": ENV,
        "silver_path": SILVER_PATH,
        "storage_format": STORAGE_FORMAT,
        "quality_non_null_min_pct": NON_NULL_MIN_PCT,
        "validations": summaries,
    }

    for target in [DOCS_LATEST_DIR, target_archive]:
        target.mkdir(parents=True, exist_ok=True)
        (target / "validation_summary.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        lines = [
            "# Validation Summary",
            "",
            f"- generated_at_utc: {payload['generated_at_utc']}",
            f"- environment: {ENV}",
            f"- storage_format: {STORAGE_FORMAT}",
            f"- silver_path: {SILVER_PATH}",
            f"- non_null_min_pct: {NON_NULL_MIN_PCT}",
            "",
            "| validation | success |",
            "|---|---|",
        ]
        for item in summaries:
            lines.append(f"| {item['validation_name']} | {item['success']} |")

        (target / "validation_summary.md").write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
        )


print(f"✅ Config qualité chargée | env={ENV}")
print(f"   SILVER : {SILVER_PATH}")
print(f"   FORMAT : {STORAGE_FORMAT}")
print(f"   GX     : {GX_RUNTIME_ROOT}")
print(f"   DOCS   : {DOCS_QUALITY_DIR}")

context = build_context()

df_stations = read_dataset("stations")
df_mesures = read_dataset("mesures")
df_conformite = read_dataset("conformite")

print("📥 Silver chargé pour validation :")
print(f"   stations   : {df_stations.count():>10,} lignes")
print(f"   mesures    : {df_mesures.count():>10,} lignes")
print(f"   conformite : {df_conformite.count():>10,} lignes")

validation_summaries = []

# Stations
stations_validator, stations_batch_request = create_validator(
    context=context,
    dataframe=df_stations,
    suite_name="silver_stations_schema_suite",
    asset_name="silver_stations_asset",
)
add_schema_expectations(stations_validator, "stations")
add_non_null_expectations(stations_validator, "stations")
add_station_expectations(stations_validator)
stations_validator.save_expectation_suite(discard_failed_expectations=False)
stations_result = run_checkpoint(
    context=context,
    batch_request=stations_batch_request,
    suite_name="silver_stations_schema_suite",
    checkpoint_name="silver_stations_checkpoint",
)
validation_summaries.append(result_to_summary("silver_stations_schema_suite", stations_result))

station_uniqueness_validator, station_uniqueness_batch_request = create_validator(
    context=context,
    dataframe=build_station_uniqueness_view(df_stations),
    suite_name="silver_stations_uniqueness_suite",
    asset_name="silver_stations_uniqueness_asset",
)
station_uniqueness_validator.expect_column_to_exist(column="_station_id")
station_uniqueness_validator.expect_column_values_to_not_be_null(column="_station_id")
station_uniqueness_validator.expect_column_values_to_be_unique(column="_station_id")
station_uniqueness_validator.save_expectation_suite(discard_failed_expectations=False)
station_uniqueness_result = run_checkpoint(
    context=context,
    batch_request=station_uniqueness_batch_request,
    suite_name="silver_stations_uniqueness_suite",
    checkpoint_name="silver_stations_uniqueness_checkpoint",
)
validation_summaries.append(
    result_to_summary("silver_stations_uniqueness_suite", station_uniqueness_result)
)

# Mesures - schema + non null
mesures_validator, mesures_batch_request = create_validator(
    context=context,
    dataframe=df_mesures,
    suite_name="silver_mesures_schema_suite",
    asset_name="silver_mesures_asset",
)
add_schema_expectations(mesures_validator, "mesures")
add_non_null_expectations(mesures_validator, "mesures")
mesures_validator.save_expectation_suite(discard_failed_expectations=False)
mesures_result = run_checkpoint(
    context=context,
    batch_request=mesures_batch_request,
    suite_name="silver_mesures_schema_suite",
    checkpoint_name="silver_mesures_checkpoint",
)
validation_summaries.append(result_to_summary("silver_mesures_schema_suite", mesures_result))

# Mesures - plages de valeurs
for parameter_key, df_parameter in build_mesures_quality_views(df_mesures).items():
    if parameter_key not in PARAM_RANGES:
        continue

    bounds = PARAM_RANGES[parameter_key]
    suite_name = f"silver_mesures_range_{parameter_key}_suite"
    asset_name = f"silver_mesures_range_{parameter_key}_asset"

    validator, batch_request = create_validator(
        context=context,
        dataframe=df_parameter,
        suite_name=suite_name,
        asset_name=asset_name,
    )
    validator.expect_table_row_count_to_be_between(min_value=0)
    validator.expect_column_values_to_be_between(
        column="resultat_parse",
        min_value=float(bounds["min"]),
        max_value=float(bounds["max"]),
        mostly=1.0,
    )
    validator.save_expectation_suite(discard_failed_expectations=False)

    result = run_checkpoint(
        context=context,
        batch_request=batch_request,
        suite_name=suite_name,
        checkpoint_name=f"silver_mesures_range_{parameter_key}_checkpoint",
    )
    validation_summaries.append(result_to_summary(suite_name, result))

# Conformite
conformite_validator, conformite_batch_request = create_validator(
    context=context,
    dataframe=df_conformite,
    suite_name="silver_conformite_schema_suite",
    asset_name="silver_conformite_asset",
)
add_schema_expectations(conformite_validator, "conformite")
add_non_null_expectations(conformite_validator, "conformite")
add_conformite_expectations(conformite_validator)
conformite_validator.save_expectation_suite(discard_failed_expectations=False)
conformite_result = run_checkpoint(
    context=context,
    batch_request=conformite_batch_request,
    suite_name="silver_conformite_schema_suite",
    checkpoint_name="silver_conformite_checkpoint",
)
validation_summaries.append(result_to_summary("silver_conformite_schema_suite", conformite_result))

context.build_data_docs()
archive_dir = archive_data_docs()
write_summary_files(archive_dir, validation_summaries)

print("=" * 72)
print("📊 RAPPORT GREAT EXPECTATIONS")
print("=" * 72)
for item in validation_summaries:
    status = "✅" if item["success"] else "❌"
    print(f"{status} {item['validation_name']}")
print("-" * 72)
print(f"Docs latest  : {DOCS_LATEST_DIR}")
print(f"Docs archive : {archive_dir}")
print(f"Index HTML   : {DOCS_LATEST_DIR / 'index.html'}")

if "displayHTML" in globals():
    index_path = DOCS_LATEST_DIR / "index.html"
    if index_path.exists():
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                displayHTML(f.read())  # type: ignore[name-defined]
        except Exception:
            pass
