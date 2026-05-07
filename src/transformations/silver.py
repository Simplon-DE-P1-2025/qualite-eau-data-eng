import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


UNITE_MAP = {
    "ug/l": "ug/L",
    "mg/l": "mg/L",
    "MG/L": "mg/L",
    "us/cm": "uS/cm",
    "ntu": "NTU",
    "NFU": "NTU",
}

CONFORMITE_MAP = {
    "C": "C",
    "N": "N",
    "S": "S",
    "c": "C",
    "n": "N",
    "s": "S",
}

MOTS_MICROBIOLOGIE = [
    "COLI",
    "COLIFORM",
    "ENTEROCOQU",
    "BACTERIE",
    "BACTERIES",
    "STREPTO",
    "CLOSTRIDIUM",
    "LEGIONELLA",
    "PSEUDOMONAS",
    "GERME",
    "FLORE",
    "SPORE",
    "CAMPYLOBACTER",
    "CRYPTOSPORIDIUM",
    "GIARDIA",
]

MOTS_CHIMIE = [
    "NITRATE",
    "NITRITE",
    "PESTICIDE",
    "HERBICIDE",
    "INSECTICIDE",
    "ALUMINIUM",
    "PLOMB",
    "CUIVRE",
    "FER",
    "MANGANESE",
    "ZINC",
    "NICKEL",
    "CHROME",
    "ARSENIC",
    "MERCURE",
    "CADMIUM",
    "FLUORURE",
    "CHLORURE",
    "SULFATE",
    "AMMONIUM",
    "PHOSPHATE",
    "PH",
    "CONDUCTIVITE",
    "CONDUCTIV",
    "TURBIDITE",
    "COULEUR",
    "ODEUR",
    "TEMPERATURE",
    "CHLORE",
    "BROMATE",
    "TRIHALOMETHANE",
    "THM",
    "BENZENE",
    "TOLUENE",
    "XYLENE",
    "MCPA",
    "ATRAZINE",
    "GLYPHOSATE",
    "TRIBUTYLTIN",
    "HYDROCARBURE",
]

MOTS_RADIOACTIVITE = [
    "TRITIUM",
    "RADIOACTIVITE",
    "DOSE TOTALE",
    "DOSE INDICATIVE",
    "CESIUM",
    "STRONTIUM",
    "IODE",
    "RADON",
    "URANIUM",
    "ALPHA",
    "BETA",
    "GAMMA",
]

COLS_MESURES = [
    "code_prelevement",
    "reference_analyse",
    "code_commune",
    "nom_commune_norm",
    "code_departement",
    "nom_departement_norm",
    "date_prelevement_ts",
    "annee_prelevement",
    "mois_prelevement",
    "code_parametre",
    "code_parametre_se",
    "code_parametre_cas",
    "libelle_parametre_norm",
    "code_type_parametre",
    "categorie_parametre",
    "code_lieu_analyse",
    "resultat_parse",
    "resultat_alphanumerique",
    "est_sous_seuil",
    "est_sur_seuil",
    "libelle_unite_norm",
    "limite_qualite_parametre",
    "reference_qualite_parametre",
    "nom_distributeur",
    "nom_uge",
    "nom_moa",
    "reseaux",
    "_resultat_manquant",
    "_est_outlier",
    "_bronze_ingestion_ts",
]


def _map_literal(mapping: dict[str, str]):
    pairs = []
    for key, value in mapping.items():
        pairs.extend([F.lit(key), F.lit(value)])
    return F.create_map(*pairs)


def parse_resultat_alpha_expr(col_name: str):
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


def normalise_unite_expr(col_name: str):
    trimmed = F.trim(F.col(col_name))
    unite_map_expr = _map_literal(UNITE_MAP)
    return F.when(F.col(col_name).isNull(), F.lit(None)).otherwise(
        F.coalesce(F.element_at(unite_map_expr, trimmed), trimmed)
    )


def normalise_conformite_expr(col_name: str):
    trimmed = F.trim(F.col(col_name))
    upper_trimmed = F.upper(trimmed)
    conformite_map_expr = _map_literal(CONFORMITE_MAP)
    return F.when(F.col(col_name).isNull(), F.lit(None)).otherwise(
        F.coalesce(F.element_at(conformite_map_expr, trimmed), upper_trimmed)
    )


def _contains_any(col_expr, keywords: list[str]):
    condition = F.lit(False)
    for keyword in keywords:
        condition = condition | F.contains(col_expr, F.lit(keyword))
    return condition


def categoriser_parametre_expr(col_name: str):
    lib = F.upper(F.coalesce(F.col(col_name), F.lit("")))
    return (
        F.when(_contains_any(lib, MOTS_RADIOACTIVITE), F.lit("RADIOACTIVITE"))
        .when(_contains_any(lib, MOTS_MICROBIOLOGIE), F.lit("MICROBIOLOGIE"))
        .when(_contains_any(lib, MOTS_CHIMIE), F.lit("CHIMIE"))
        .otherwise(F.lit("AUTRE"))
    )


def build_typed_resultats(df_bronze_resultats):
    return (
        df_bronze_resultats
        .withColumn(
            "date_prelevement_ts",
            F.to_timestamp(F.col("date_prelevement"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("annee_prelevement", F.year(F.col("date_prelevement_ts")).cast(T.IntegerType()))
        .withColumn("mois_prelevement", F.month(F.col("date_prelevement_ts")).cast(T.IntegerType()))
        .withColumn("resultat_numerique", F.col("resultat_numerique").cast(T.DoubleType()))
        .withColumn(
            "resultat_parse",
            F.when(F.col("resultat_numerique").isNotNull(), F.col("resultat_numerique")).otherwise(
                parse_resultat_alpha_expr("resultat_alphanumerique")
            ),
        )
        .withColumn("est_sous_seuil", F.col("resultat_alphanumerique").startswith("<").cast(T.BooleanType()))
        .withColumn("est_sur_seuil", F.col("resultat_alphanumerique").startswith(">").cast(T.BooleanType()))
        .withColumn("_bronze_ingestion_ts", F.col("_ingestion_timestamp"))
    )


def deduplicate_resultats(df_typed):
    window_dedup = Window.partitionBy(
        "code_prelevement", "code_parametre", "date_prelevement"
    ).orderBy(F.col("_bronze_ingestion_ts").desc())
    return (
        df_typed
        .withColumn("_row_num", F.row_number().over(window_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def normalise_resultats(df_dedup):
    return (
        df_dedup
        .withColumn("libelle_parametre_norm", F.upper(F.trim(F.col("libelle_parametre"))))
        .withColumn("nom_commune_norm", F.upper(F.trim(F.col("nom_commune"))))
        .withColumn("nom_departement_norm", F.upper(F.trim(F.col("nom_departement"))))
        .withColumn("libelle_unite_norm", normalise_unite_expr("libelle_unite"))
        .withColumn(
            "conformite_bact_limite_norm",
            normalise_conformite_expr("conformite_limites_bact_prelevement"),
        )
        .withColumn(
            "conformite_pc_limite_norm",
            normalise_conformite_expr("conformite_limites_pc_prelevement"),
        )
        .withColumn(
            "conformite_bact_ref_norm",
            normalise_conformite_expr("conformite_references_bact_prelevement"),
        )
        .withColumn(
            "conformite_pc_ref_norm",
            normalise_conformite_expr("conformite_references_pc_prelevement"),
        )
    )


def handle_missing_values(df_norm, seuil_null_bloquant: float):
    cols_critiques = ["date_prelevement_ts", "code_commune", "code_parametre"]
    n_avant_filtre = df_norm.count()
    df_filtre = df_norm
    for col_name in cols_critiques:
        df_filtre = df_filtre.filter(F.col(col_name).isNotNull())

    n_apres_filtre = df_filtre.count()
    df_null_handled = (
        df_filtre
        .withColumn("_resultat_manquant", F.col("resultat_parse").isNull().cast(T.BooleanType()))
        .withColumn(
            "libelle_unite_norm",
            F.when(F.col("libelle_unite_norm").isNull(), F.lit("INCONNUE")).otherwise(F.col("libelle_unite_norm")),
        )
        .withColumn(
            "nom_distributeur",
            F.when(F.col("nom_distributeur").isNull(), F.lit("INCONNU")).otherwise(F.col("nom_distributeur")),
        )
    )
    stats = {
        "n_avant_filtre": n_avant_filtre,
        "n_apres_filtre": n_apres_filtre,
        "n_suppr_critiques": n_avant_filtre - n_apres_filtre,
        "seuil_null_bloquant": seuil_null_bloquant,
    }
    return df_null_handled, stats


def flag_outliers(df_null_handled):
    df_stats = (
        df_null_handled
        .filter(F.col("resultat_parse").isNotNull())
        .groupBy("code_parametre")
        .agg(
            F.percentile_approx("resultat_parse", 0.25).alias("q1"),
            F.percentile_approx("resultat_parse", 0.75).alias("q3"),
            F.count("*").alias("n_mesures"),
        )
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn("borne_basse", F.col("q1") - 3 * F.col("iqr"))
        .withColumn("borne_haute", F.col("q3") + 3 * F.col("iqr"))
    )

    return (
        df_null_handled
        .join(df_stats.select("code_parametre", "borne_basse", "borne_haute"), on="code_parametre", how="left")
        .withColumn(
            "_est_outlier",
            F.when(F.col("resultat_parse").isNull(), F.lit(False))
            .when(
                (F.col("resultat_parse") < F.col("borne_basse")) |
                (F.col("resultat_parse") > F.col("borne_haute")),
                F.lit(True),
            )
            .otherwise(F.lit(False))
            .cast(T.BooleanType()),
        )
        .drop("borne_basse", "borne_haute")
    )


def categorise_resultats(df_outliers):
    return df_outliers.withColumn(
        "categorie_parametre",
        categoriser_parametre_expr("libelle_parametre_norm"),
    )


def build_geo_clean(df_bronze_geo):
    return (
        df_bronze_geo
        .withColumn("longitude", F.col("longitude").cast(T.DoubleType()))
        .withColumn("latitude", F.col("latitude").cast(T.DoubleType()))
        .withColumn(
            "_coords_valides",
            (
                F.col("longitude").between(-5.5, 9.6) &
                F.col("latitude").between(41.0, 51.2)
            ).cast(T.BooleanType()),
        )
        .select("code", "nom", "codeDepartement", "codeRegion", "population", "longitude", "latitude", "_coords_valides")
        .withColumnRenamed("code", "code_commune_geo")
        .withColumnRenamed("nom", "nom_commune_geo")
        .withColumnRenamed("codeDepartement", "code_departement_geo")
        .withColumnRenamed("codeRegion", "code_region_geo")
    )


def build_stations(df_bronze_communes, df_geo_clean, silver_timestamp: str | None = None):
    silver_timestamp = silver_timestamp or datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return (
        df_bronze_communes
        .select("code_commune", "nom_commune", "nom_quartier", "code_reseau", "nom_reseau", "debut_alim", "annee")
        .withColumn("nom_commune_norm", F.upper(F.trim(F.col("nom_commune"))))
        .withColumn("debut_alim", F.to_date(F.col("debut_alim"), "yyyy-MM-dd"))
        .withColumn("annee", F.col("annee").cast(T.IntegerType()))
        .dropDuplicates(["code_commune", "code_reseau", "nom_quartier"])
        .join(df_geo_clean, F.col("code_commune") == df_geo_clean["code_commune_geo"], "left")
        .drop("code_commune_geo", "nom_commune_geo")
        .withColumn("_silver_timestamp", F.lit(silver_timestamp).cast("timestamp"))
    )


def build_mesures(df_cat, silver_timestamp: str | None = None):
    silver_timestamp = silver_timestamp or datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cols_disponibles = [col_name for col_name in COLS_MESURES if col_name in df_cat.columns]
    return (
        df_cat
        .select(*cols_disponibles)
        .withColumn("_silver_timestamp", F.lit(silver_timestamp).cast("timestamp"))
    )


def build_conformite(df_cat, silver_timestamp: str | None = None):
    silver_timestamp = silver_timestamp or datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return (
        df_cat
        .select(
            "code_prelevement",
            "date_prelevement_ts",
            "annee_prelevement",
            "code_commune",
            "nom_commune_norm",
            "code_departement",
            "nom_departement_norm",
            "nom_distributeur",
            "nom_uge",
            "conformite_bact_limite_norm",
            "conformite_pc_limite_norm",
            "conformite_bact_ref_norm",
            "conformite_pc_ref_norm",
            "conclusion_conformite_prelevement",
            "reseaux",
        )
        .dropDuplicates(["code_prelevement"])
        .withColumn(
            "conformite_globale",
            F.when(
                (F.col("conformite_bact_limite_norm") == "N") |
                (F.col("conformite_pc_limite_norm") == "N") |
                (F.col("conformite_bact_ref_norm") == "N") |
                (F.col("conformite_pc_ref_norm") == "N"),
                F.lit("N"),
            )
            .when(
                (F.col("conformite_bact_limite_norm") == "C") &
                (F.col("conformite_pc_limite_norm") == "C") &
                (F.col("conformite_bact_ref_norm") == "C") &
                (F.col("conformite_pc_ref_norm") == "C"),
                F.lit("C"),
            )
            .otherwise(F.lit("S")),
        )
        .withColumn("_silver_timestamp", F.lit(silver_timestamp).cast("timestamp"))
    )
