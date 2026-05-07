# Validation des donnees avec Great Expectations

Cette documentation couvre la validation qualite de la couche Silver par [great_expectations_validation.py](../../notebooks/quality/great_expectations_validation.py).

Le script sert a verifier que les tables Silver produites par le pipeline sont:

- presentes
- correctement structurees
- suffisamment completees
- coherentes sur certains champs metier

## Perimetre controle

La validation porte sur trois jeux Silver:

- `stations`
- `mesures`
- `conformite`

Le script lit ces datasets depuis le chemin Silver resolu via [config/config.yml](../../config/config.yml), puis cree des suites Great Expectations a l'execution.

## Ce qui est verifie

### `silver.stations`

Controles principaux:

- ordre et nombre de colonnes attendus
- types Spark attendus
- presence des colonnes critiques
- taux minimal de non-null selon `quality.non_null_min_pct`
- presence de `code_reseau`
- bornes geographiques de `latitude` et `longitude`
- unicite d'un identifiant technique reconstruit pour une station

### `silver.mesures`

Controles principaux:

- schema et types attendus
- colonnes critiques non nulles
- volume minimum de lignes
- controles de plages pour certains parametres quand `resultat_parse` est renseigne

Parametres controles via la config:

- `pH`
- `nitrates_mg_l`
- `nitrites_mg_l`
- `conductivite`
- `turbidite_ntu`

Les bornes viennent de `quality.parametres_plages` dans [config/config.yml](../../config/config.yml).

### `silver.conformite`

Controles principaux:

- schema et types attendus
- colonnes critiques non nulles
- valeurs autorisees pour `conformite_globale`
- valeurs autorisees pour les colonnes de conformite normalisees

Les valeurs autorisees viennent de `quality.valeurs_conformite` dans la config, par defaut:

- `C`
- `N`
- `S`

## Structure des suites

Le script construit plusieurs suites a la volee, notamment:

- `silver_stations_schema_suite`
- `silver_stations_uniqueness_suite`
- `silver_mesures_schema_suite`
- `silver_mesures_range_<parametre>_suite`
- `silver_conformite_schema_suite`

Les suites sont regenerees a chaque execution dans un runtime Great Expectations local temporaire:

- `notebooks/quality/.gx_runtime/`

Ce dossier est ignore par Git.

## Sorties generees

Les resultats publies sont copies dans [docs/quality](./):

- `latest/`
  la derniere synthese produite
- `archive/<timestamp>/`
  une copie horodatee de chaque run
- `validation_summary.json`
  resume exploitable par machine
- `validation_summary.md`
  resume lisible rapidement

En pratique, le plus utile pour une lecture humaine rapide est:

- [latest/validation_summary.md](./latest/validation_summary.md)

## Execution locale

### Prerequis

Il faut disposer de:

- Python `3.11`
- Java `17`
- PySpark
- Great Expectations `0.18.21`

Exemple d'installation:

```powershell
py -3.11 -m pip install great-expectations==0.18.21 pyspark pyyaml
```

### Lancer la validation seule

Depuis la racine du repo:

```powershell
py -3.11 .\notebooks\quality\great_expectations_validation.py
```

### Lancer la validation depuis l'orchestrateur

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --with-quality
```

Dans ce mode, la validation s'insere juste apres Silver et avant Gold.

## Execution sur Databricks

Le script vient d'un notebook exporte et reste compatible avec Databricks.

Installation suggeree dans un notebook:

```python
%pip install great-expectations==0.18.21
dbutils.library.restartPython()
```

Ensuite, le notebook peut etre joue comme une tache de workflow entre Silver et Gold.

## Lecture des resultats

Tu peux lire les resultats a deux niveaux:

1. `validation_summary.md`
   pour voir rapidement quelles suites passent ou echouent
2. les Data Docs Great Expectations
   pour inspecter le detail expectation par expectation

Le script appelle aussi `context.build_data_docs()`, donc les artefacts HTML sont regeneres a chaque run avant archivage.

## Lien avec la configuration

Les seuils les plus importants se pilotent dans [config/config.yml](../../config/config.yml):

- `quality.non_null_min_pct`
- `quality.valeurs_conformite`
- `quality.parametres_plages`

Cela permet de durcir ou d'assouplir la validation sans retoucher le script.

## Recommandation d'usage

Le bon ordre de travail est:

1. lancer Bronze
2. lancer Silver
3. lancer Great Expectations
4. analyser `docs/quality/latest/validation_summary.md`
5. seulement ensuite lancer Gold si la qualite est acceptable

Pour une utilisation locale courante, le plus simple reste:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage bronze --to-stage gold --with-quality
```
