import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F


def add_gold_timestamp(df, gold_timestamp: str | None = None):
    gold_timestamp = gold_timestamp or datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return df.withColumn("_gold_timestamp", F.lit(gold_timestamp).cast("timestamp"))


def build_communes_geo(df_silver_stations):
    return (
        df_silver_stations
        .select(
            "code_commune",
            "nom_commune_norm",
            F.col("code_departement_geo").alias("code_departement_geo"),
            F.col("code_region_geo").alias("code_region_geo"),
            "population",
            "longitude",
            "latitude",
            "_coords_valides",
        )
        .dropDuplicates(["code_commune"])
    )


def build_region_population(df_communes_geo):
    return (
        df_communes_geo
        .filter(F.col("code_region_geo").isNotNull())
        .groupBy("code_region_geo")
        .agg(F.sum(F.coalesce("population", F.lit(0))).alias("population_couverte"))
    )


def build_conformite_commune(df_silver_conformite, df_communes_geo, gold_timestamp: str | None = None):
    df = (
        df_silver_conformite
        .join(df_communes_geo, on=["code_commune", "nom_commune_norm"], how="left")
        .groupBy(
            "annee_prelevement",
            "code_departement",
            "nom_departement_norm",
            "code_region_geo",
            "code_commune",
            "nom_commune_norm",
        )
        .agg(
            F.first("population", ignorenulls=True).alias("population"),
            F.first("longitude", ignorenulls=True).alias("longitude"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.first("_coords_valides", ignorenulls=True).alias("_coords_valides"),
            F.countDistinct("code_prelevement").alias("nb_prelevements_total"),
            F.sum(F.when(F.col("conformite_globale") == "C", 1).otherwise(0)).alias("nb_prelevements_conformes"),
            F.sum(F.when(F.col("conformite_globale") == "N", 1).otherwise(0)).alias("nb_prelevements_non_conformes"),
            F.sum(F.when(F.col("conformite_globale") == "S", 1).otherwise(0)).alias("nb_prelevements_sans_objet"),
        )
        .withColumn(
            "taux_conformite_pct",
            F.round(100 * F.col("nb_prelevements_conformes") / F.col("nb_prelevements_total"), 2),
        )
        .withColumn(
            "taux_non_conformite_pct",
            F.round(100 * F.col("nb_prelevements_non_conformes") / F.col("nb_prelevements_total"), 2),
        )
    )
    return add_gold_timestamp(df, gold_timestamp)


def build_evolution_parametres(df_silver_mesures, df_communes_geo, gold_timestamp: str | None = None):
    df = (
        df_silver_mesures
        .join(df_communes_geo, on=["code_commune", "nom_commune_norm"], how="left")
        .groupBy(
            "annee_prelevement",
            "mois_prelevement",
            "code_departement",
            "nom_departement_norm",
            "code_region_geo",
            "code_commune",
            "nom_commune_norm",
            "code_parametre",
            "libelle_parametre_norm",
            "categorie_parametre",
            "libelle_unite_norm",
        )
        .agg(
            F.count("*").alias("nb_mesures"),
            F.sum(F.when(F.col("_resultat_manquant"), 1).otherwise(0)).alias("nb_resultats_manquants"),
            F.sum(F.when(F.col("_est_outlier"), 1).otherwise(0)).alias("nb_outliers"),
            F.avg("resultat_parse").alias("valeur_moyenne"),
            F.expr("percentile_approx(resultat_parse, 0.5)").alias("valeur_mediane"),
            F.min("resultat_parse").alias("valeur_min"),
            F.max("resultat_parse").alias("valeur_max"),
        )
        .withColumn("valeur_moyenne", F.round("valeur_moyenne", 4))
        .withColumn("valeur_mediane", F.round("valeur_mediane", 4))
        .withColumn("valeur_min", F.round("valeur_min", 4))
        .withColumn("valeur_max", F.round("valeur_max", 4))
        .withColumn("pct_resultats_manquants", F.round(100 * F.col("nb_resultats_manquants") / F.col("nb_mesures"), 2))
        .withColumn("pct_outliers", F.round(100 * F.col("nb_outliers") / F.col("nb_mesures"), 2))
    )
    return add_gold_timestamp(df, gold_timestamp)


def build_qualite_region(
    df_silver_conformite,
    df_communes_geo,
    df_region_population,
    gold_timestamp: str | None = None,
):
    df = (
        df_silver_conformite
        .join(df_communes_geo, on=["code_commune", "nom_commune_norm"], how="left")
        .groupBy("annee_prelevement", "code_region_geo")
        .agg(
            F.countDistinct("code_commune").alias("nb_communes"),
            F.countDistinct("code_prelevement").alias("nb_prelevements_total"),
            F.sum(F.when(F.col("conformite_globale") == "C", 1).otherwise(0)).alias("nb_prelevements_conformes"),
            F.sum(F.when(F.col("conformite_globale") == "N", 1).otherwise(0)).alias("nb_prelevements_non_conformes"),
            F.sum(F.when(F.col("conformite_globale") == "S", 1).otherwise(0)).alias("nb_prelevements_sans_objet"),
        )
        .join(df_region_population, on="code_region_geo", how="left")
        .withColumn(
            "taux_conformite_pct",
            F.round(100 * F.col("nb_prelevements_conformes") / F.col("nb_prelevements_total"), 2),
        )
        .withColumn(
            "taux_non_conformite_pct",
            F.round(100 * F.col("nb_prelevements_non_conformes") / F.col("nb_prelevements_total"), 2),
        )
    )
    return add_gold_timestamp(df, gold_timestamp)


def build_score_communes(df_gold_conformite_commune):
    return (
        df_gold_conformite_commune
        .groupBy(
            "code_departement",
            "nom_departement_norm",
            "code_region_geo",
            "code_commune",
            "nom_commune_norm",
        )
        .agg(
            F.first("population", ignorenulls=True).alias("population"),
            F.first("longitude", ignorenulls=True).alias("longitude"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.sum("nb_prelevements_total").alias("nb_prelevements_total"),
            F.sum("nb_prelevements_conformes").alias("nb_prelevements_conformes"),
            F.sum("nb_prelevements_non_conformes").alias("nb_prelevements_non_conformes"),
            F.sum("nb_prelevements_sans_objet").alias("nb_prelevements_sans_objet"),
        )
        .withColumn(
            "taux_conformite_pct",
            F.round(100 * F.col("nb_prelevements_conformes") / F.col("nb_prelevements_total"), 2),
        )
        .withColumn(
            "taux_non_conformite_pct",
            F.round(100 * F.col("nb_prelevements_non_conformes") / F.col("nb_prelevements_total"), 2),
        )
    )


def build_top10_communes(df_score, limit: int = 10, gold_timestamp: str | None = None):
    best_window = Window.orderBy(
        F.col("taux_conformite_pct").desc_nulls_last(),
        F.col("nb_prelevements_total").desc_nulls_last(),
        F.col("nom_commune_norm").asc_nulls_last(),
    )
    worst_window = Window.orderBy(
        F.col("taux_conformite_pct").asc_nulls_last(),
        F.col("nb_prelevements_total").desc_nulls_last(),
        F.col("nom_commune_norm").asc_nulls_last(),
    )

    best_rows = (
        df_score
        .withColumn("rang", F.row_number().over(best_window))
        .filter(F.col("rang") <= limit)
        .withColumn("classement_type", F.lit("PLUS_CONFORME"))
    )

    worst_rows = (
        df_score
        .withColumn("rang", F.row_number().over(worst_window))
        .filter(F.col("rang") <= limit)
        .withColumn("classement_type", F.lit("MOINS_CONFORME"))
    )

    return add_gold_timestamp(best_rows.unionByName(worst_rows), gold_timestamp)


def build_non_conformes(df_silver_conformite):
    return (
        df_silver_conformite
        .filter(F.col("conformite_globale") == "N")
        .select(
            "code_prelevement",
            "annee_prelevement",
            "code_departement",
            "nom_departement_norm",
            "code_commune",
            "nom_commune_norm",
            "conclusion_conformite_prelevement",
        )
    )


def build_non_conformites(
    df_non_conformes,
    df_silver_mesures,
    df_communes_geo,
    gold_timestamp: str | None = None,
):
    df = (
        df_non_conformes
        .join(
            df_silver_mesures.select(
                "code_prelevement",
                "code_parametre",
                "libelle_parametre_norm",
                "categorie_parametre",
                "libelle_unite_norm",
                "resultat_parse",
                "_est_outlier",
                "est_sous_seuil",
                "limite_qualite_parametre",
                "reference_qualite_parametre",
            ),
            on="code_prelevement",
            how="inner",
        )
        .join(df_communes_geo, on=["code_commune", "nom_commune_norm"], how="left")
        .groupBy(
            "annee_prelevement",
            "code_departement",
            "nom_departement_norm",
            "code_region_geo",
            "code_commune",
            "nom_commune_norm",
            "code_parametre",
            "libelle_parametre_norm",
            "categorie_parametre",
            "libelle_unite_norm",
        )
        .agg(
            F.countDistinct("code_prelevement").alias("nb_prelevements_non_conformes"),
            F.count("*").alias("nb_mesures_associees"),
            F.sum(
                F.when(
                    F.col("limite_qualite_parametre").isNotNull() |
                    F.col("reference_qualite_parametre").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("nb_mesures_avec_seuil_reglementaire"),
            F.avg("resultat_parse").alias("valeur_moyenne"),
            F.max("resultat_parse").alias("valeur_max"),
            F.sum(F.when(F.col("_est_outlier"), 1).otherwise(0)).alias("nb_outliers"),
            F.sum(F.when(F.col("est_sous_seuil"), 1).otherwise(0)).alias("nb_mesures_sous_seuil"),
        )
        .withColumn("valeur_moyenne", F.round("valeur_moyenne", 4))
        .withColumn("valeur_max", F.round("valeur_max", 4))
        .withColumn("pct_outliers", F.round(100 * F.col("nb_outliers") / F.col("nb_mesures_associees"), 2))
    )
    return add_gold_timestamp(df, gold_timestamp)
