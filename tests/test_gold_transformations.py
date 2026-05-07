from __future__ import annotations

from pyspark.sql import types as T

from src.transformations import gold as gold_tf


def _build_communes_geo_df(spark):
    schema = T.StructType(
        [
            T.StructField("code_commune", T.StringType(), False),
            T.StructField("nom_commune_norm", T.StringType(), False),
            T.StructField("code_region_geo", T.StringType(), True),
            T.StructField("population", T.IntegerType(), True),
            T.StructField("longitude", T.DoubleType(), True),
            T.StructField("latitude", T.DoubleType(), True),
            T.StructField("_coords_valides", T.BooleanType(), True),
        ]
    )
    return spark.createDataFrame(
        [
            ("64001", "PAU", "75", 1000, -0.37, 43.29, True),
            ("64002", "BIZANOS", "75", 500, -0.35, 43.28, True),
        ],
        schema=schema,
    )


def test_build_conformite_commune_aggregates_metrics(spark):
    conformite_schema = T.StructType(
        [
            T.StructField("annee_prelevement", T.IntegerType(), False),
            T.StructField("code_departement", T.StringType(), True),
            T.StructField("nom_departement_norm", T.StringType(), True),
            T.StructField("code_commune", T.StringType(), False),
            T.StructField("nom_commune_norm", T.StringType(), False),
            T.StructField("code_prelevement", T.StringType(), False),
            T.StructField("conformite_globale", T.StringType(), True),
        ]
    )
    df_conformite = spark.createDataFrame(
        [
            (2025, "64", "PYRENEES-ATLANTIQUES", "64001", "PAU", "P1", "C"),
            (2025, "64", "PYRENEES-ATLANTIQUES", "64001", "PAU", "P2", "N"),
            (2025, "64", "PYRENEES-ATLANTIQUES", "64001", "PAU", "P3", "C"),
            (2025, "64", "PYRENEES-ATLANTIQUES", "64002", "BIZANOS", "P4", "S"),
        ],
        schema=conformite_schema,
    )

    result_df = gold_tf.build_conformite_commune(
        df_conformite,
        _build_communes_geo_df(spark),
        gold_timestamp="2025-01-05 00:00:00",
    )
    rows = {(row.code_commune, row.nom_commune_norm): row.asDict() for row in result_df.collect()}

    pau = rows[("64001", "PAU")]
    bizanos = rows[("64002", "BIZANOS")]

    assert pau["nb_prelevements_total"] == 3
    assert pau["nb_prelevements_conformes"] == 2
    assert pau["nb_prelevements_non_conformes"] == 1
    assert pau["taux_conformite_pct"] == 66.67
    assert pau["taux_non_conformite_pct"] == 33.33
    assert bizanos["nb_prelevements_sans_objet"] == 1


def test_build_evolution_parametres_aggregates_missing_and_outliers(spark):
    schema = T.StructType(
        [
            T.StructField("annee_prelevement", T.IntegerType(), False),
            T.StructField("mois_prelevement", T.IntegerType(), False),
            T.StructField("code_departement", T.StringType(), True),
            T.StructField("nom_departement_norm", T.StringType(), True),
            T.StructField("code_commune", T.StringType(), False),
            T.StructField("nom_commune_norm", T.StringType(), False),
            T.StructField("code_parametre", T.StringType(), False),
            T.StructField("libelle_parametre_norm", T.StringType(), True),
            T.StructField("categorie_parametre", T.StringType(), True),
            T.StructField("libelle_unite_norm", T.StringType(), True),
            T.StructField("_resultat_manquant", T.BooleanType(), True),
            T.StructField("_est_outlier", T.BooleanType(), True),
            T.StructField("resultat_parse", T.DoubleType(), True),
        ]
    )
    df_mesures = spark.createDataFrame(
        [
            (2025, 1, "64", "PYRENEES-ATLANTIQUES", "64001", "PAU", "NIT", "NITRATES", "CHIMIE", "mg/L", False, False, 10.0),
            (2025, 1, "64", "PYRENEES-ATLANTIQUES", "64001", "PAU", "NIT", "NITRATES", "CHIMIE", "mg/L", True, False, None),
            (2025, 1, "64", "PYRENEES-ATLANTIQUES", "64001", "PAU", "NIT", "NITRATES", "CHIMIE", "mg/L", False, True, 14.0),
        ],
        schema=schema,
    )

    result_df = gold_tf.build_evolution_parametres(
        df_mesures,
        _build_communes_geo_df(spark),
        gold_timestamp="2025-01-05 00:00:00",
    )
    row = result_df.collect()[0]

    assert row.nb_mesures == 3
    assert row.nb_resultats_manquants == 1
    assert row.nb_outliers == 1
    assert row.pct_resultats_manquants == 33.33
    assert row.pct_outliers == 33.33
    assert row.valeur_moyenne == 12.0


def test_build_top10_communes_orders_best_and_worst(spark):
    schema = T.StructType(
        [
            T.StructField("code_departement", T.StringType(), True),
            T.StructField("nom_departement_norm", T.StringType(), True),
            T.StructField("code_region_geo", T.StringType(), True),
            T.StructField("code_commune", T.StringType(), False),
            T.StructField("nom_commune_norm", T.StringType(), False),
            T.StructField("population", T.IntegerType(), True),
            T.StructField("longitude", T.DoubleType(), True),
            T.StructField("latitude", T.DoubleType(), True),
            T.StructField("nb_prelevements_total", T.IntegerType(), True),
            T.StructField("nb_prelevements_conformes", T.IntegerType(), True),
            T.StructField("nb_prelevements_non_conformes", T.IntegerType(), True),
            T.StructField("nb_prelevements_sans_objet", T.IntegerType(), True),
            T.StructField("taux_conformite_pct", T.DoubleType(), True),
            T.StructField("taux_non_conformite_pct", T.DoubleType(), True),
        ]
    )
    df_score = spark.createDataFrame(
        [
            ("64", "PYRENEES-ATLANTIQUES", "75", "64001", "PAU", 1000, -0.37, 43.29, 10, 10, 0, 0, 100.0, 0.0),
            ("64", "PYRENEES-ATLANTIQUES", "75", "64002", "BIZANOS", 500, -0.35, 43.28, 10, 6, 4, 0, 60.0, 40.0),
            ("64", "PYRENEES-ATLANTIQUES", "75", "64003", "LESCAR", 900, -0.40, 43.33, 10, 2, 8, 0, 20.0, 80.0),
        ],
        schema=schema,
    )

    result_df = gold_tf.build_top10_communes(df_score, limit=2, gold_timestamp="2025-01-05 00:00:00")
    rows = {(row.classement_type, row.rang): row.nom_commune_norm for row in result_df.collect()}

    assert rows[("PLUS_CONFORME", 1)] == "PAU"
    assert rows[("PLUS_CONFORME", 2)] == "BIZANOS"
    assert rows[("MOINS_CONFORME", 1)] == "LESCAR"
    assert rows[("MOINS_CONFORME", 2)] == "BIZANOS"
