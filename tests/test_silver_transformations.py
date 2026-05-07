from __future__ import annotations

from pyspark.sql import types as T

from src.transformations import silver as silver_tf


def test_handle_missing_values_filters_critical_nulls_and_fills_defaults(spark):
    schema = T.StructType(
        [
            T.StructField("date_prelevement_ts", T.StringType(), True),
            T.StructField("code_commune", T.StringType(), True),
            T.StructField("code_parametre", T.StringType(), True),
            T.StructField("resultat_parse", T.DoubleType(), True),
            T.StructField("libelle_unite_norm", T.StringType(), True),
            T.StructField("nom_distributeur", T.StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            ("2025-01-01 10:00:00", "64001", "P1", 1.2, None, None),
            ("2025-01-01 11:00:00", "64002", "P2", None, "mg/L", "EAU64"),
            ("2025-01-01 12:00:00", None, "P3", 3.4, "mg/L", "EAU64"),
        ],
        schema=schema,
    )

    result_df, stats = silver_tf.handle_missing_values(df, 0.95)
    rows = {row.code_parametre: row.asDict() for row in result_df.collect()}

    assert stats["n_suppr_critiques"] == 1
    assert result_df.count() == 2
    assert rows["P1"]["libelle_unite_norm"] == "INCONNUE"
    assert rows["P1"]["nom_distributeur"] == "INCONNU"
    assert rows["P1"]["_resultat_manquant"] is False
    assert rows["P2"]["_resultat_manquant"] is True


def test_build_conformite_calculates_global_status(spark):
    schema = T.StructType(
        [
            T.StructField("code_prelevement", T.StringType(), False),
            T.StructField("date_prelevement_ts", T.StringType(), True),
            T.StructField("annee_prelevement", T.IntegerType(), True),
            T.StructField("code_commune", T.StringType(), True),
            T.StructField("nom_commune_norm", T.StringType(), True),
            T.StructField("code_departement", T.StringType(), True),
            T.StructField("nom_departement_norm", T.StringType(), True),
            T.StructField("nom_distributeur", T.StringType(), True),
            T.StructField("nom_uge", T.StringType(), True),
            T.StructField("conformite_bact_limite_norm", T.StringType(), True),
            T.StructField("conformite_pc_limite_norm", T.StringType(), True),
            T.StructField("conformite_bact_ref_norm", T.StringType(), True),
            T.StructField("conformite_pc_ref_norm", T.StringType(), True),
            T.StructField("conclusion_conformite_prelevement", T.StringType(), True),
            T.StructField("reseaux", T.StringType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            ("P1", "2025-01-01 10:00:00", 2025, "64001", "PAU", "64", "PYRENEES-ATLANTIQUES", "EAU64", "UGE1", "C", "C", "C", "C", "OK", "[]"),
            ("P2", "2025-01-02 10:00:00", 2025, "64001", "PAU", "64", "PYRENEES-ATLANTIQUES", "EAU64", "UGE1", "N", "C", "C", "C", "KO", "[]"),
            ("P3", "2025-01-03 10:00:00", 2025, "64002", "BIZANOS", "64", "PYRENEES-ATLANTIQUES", "EAU64", "UGE1", "S", "C", None, "C", "SO", "[]"),
        ],
        schema=schema,
    )

    result_df = silver_tf.build_conformite(df, silver_timestamp="2025-01-05 00:00:00")
    statuses = {row.code_prelevement: row.conformite_globale for row in result_df.collect()}

    assert statuses == {"P1": "C", "P2": "N", "P3": "S"}

