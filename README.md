# Qualite Eau Data Engineering

Projet de pipeline data pour collecter, transformer, exposer et visualiser des donnees de qualite de l'eau en France.

Le projet suit une architecture medallion:

- `Bronze` pour l'ingestion brute depuis les APIs
- `Silver` pour le nettoyage et la standardisation
- `Gold` pour les agregations metier exploitees par une API et un dashboard

Le code est pense pour fonctionner en local aujourd'hui, tout en restant proche d'une execution Azure Databricks par taches separees.

## Objectif

Le pipeline assemble trois sources:

- `Hub'Eau communes_udi` pour les communes et UDI
- `Hub'Eau resultats_dis` pour les analyses sanitaires
- `Geo API Gouv` pour les communes et leurs coordonnees

Ces sources sont consolidees pour produire:

- des tables Silver normalisees
- des tables Gold pour le pilotage
- une API locale de consultation
- un dashboard local pour l'exploration

## Architecture du projet

```text
qualite-eau-data-eng/
|-- .github/workflows/        # CI tests + release
|-- config/                   # configuration centrale du pipeline
|   `-- config.yml
|-- data/                     # sorties locales bronze/silver/gold/logs (ignorees par Git)
|-- docs/                     # documentation fonctionnelle et technique
|   |-- api_gold.md
|   |-- dashboard_gold.md
|   |-- orchestration.md
|   `-- quality/
|-- notebooks/
|   `-- quality/
|       `-- great_expectations_validation.py
|-- src/
|   |-- ingestion/            # scripts Bronze, Silver, Gold
|   |-- orchestration/        # lanceur de pipeline local
|   |-- serving/              # API Gold + dashboard
|   `-- transformations/      # logique PySpark factorisee et testable
|-- tests/                    # tests unitaires PySpark et helpers pytest
|-- CONTRIBUTING.md
|-- LICENSE
|-- package.json              # semantic-release
`-- .releaserc.json
```

## Fonctionnement par dossier

### `config/`

[config.yml](./config/config.yml) centralise:

- l'environnement cible (`local`, `community`, `azure`)
- les chemins de stockage
- les noms logiques des tables
- les parametres des APIs
- les reglages d'ingestion HTTP
- les seuils de qualite

Le projet lit cette config dans Bronze, Silver, Gold, l'API et l'orchestrateur.

### `src/ingestion/`

- [bronze_ingestion.py](./src/ingestion/bronze_ingestion.py): recupere les donnees depuis les APIs et ecrit la couche Bronze
- [silver_ingestion.py](./src/ingestion/silver_ingestion.py): nettoie, normalise et repartit les donnees en tables Silver
- [gold_ingestion.py](./src/ingestion/gold_ingestion.py): construit les tables analytiques Gold

### `src/transformations/`

La logique metier testable a ete extraite des scripts d'ingestion:

- [silver.py](./src/transformations/silver.py): nettoyage, typage, conformite, categorisation
- [gold.py](./src/transformations/gold.py): agregations et classements Gold
- [bronze_geo.py](./src/transformations/bronze_geo.py): helpers pour la Geo API

Ces fonctions sont utilisees par les scripts d'ingestion et par les tests unitaires.

### `src/orchestration/`

[run_pipeline.py](./src/orchestration/run_pipeline.py) pilote l'execution locale:

- `bronze`
- `silver`
- `quality` en option
- `gold`

Il produit aussi des logs horodates et un resume de run.

L'orchestrateur sait maintenant limiter Bronze a une seule API source via `--bronze-api`.

### `src/serving/`

- [gold_api.py](./src/serving/gold_api.py): API FastAPI qui lit les jeux Gold via DuckDB
- [gold_dashboard.py](./src/serving/gold_dashboard.py): dashboard Flask/Plotly qui consomme l'API Gold

### `tests/`

Le dossier contient:

- [conftest.py](./tests/conftest.py): fixture Spark
- [test_bronze_geo.py](./tests/test_bronze_geo.py): tests de la logique Geo API
- [test_silver_transformations.py](./tests/test_silver_transformations.py): tests des transformations Silver
- [test_gold_transformations.py](./tests/test_gold_transformations.py): tests des agregations Gold

### `docs/`

Documentation complementaire:

- [docs/orchestration.md](./docs/orchestration.md)
- [docs/api_gold.md](./docs/api_gold.md)
- [docs/dashboard_gold.md](./docs/dashboard_gold.md)
- [docs/quality/README.md](./docs/quality/README.md)

## Pipeline de bout en bout

### Bronze

La couche Bronze recupere les donnees brutes des trois APIs:

1. `geo_communes`
2. `hubeau_communes`
3. `hubeau_resultats`

Particularites:

- `hubeau_resultats` est pagine
- `geo_communes` n'utilise pas la pagination classique
- pour `geo_communes`, le projet boucle sur `/departements/{code}/communes` quand il faut charger un ou plusieurs departements, ce qui permet de recuperer proprement les centres geo en `GeoJSON`
- certains champs imbriques (`reseaux`, `codesPostaux`) sont serialises en JSON string en Bronze

Sorties locales attendues:

- `data/bronze/communes_udi`
- `data/bronze/resultats_dis`
- `data/bronze/geo_communes`

### Silver

La couche Silver transforme Bronze en trois tables metier:

- `stations`
- `mesures`
- `conformite`

Le traitement comprend notamment:

- correction des types
- suppression des doublons
- normalisation des libelles et des unites
- gestion des valeurs manquantes
- detection d'outliers
- categorisation des parametres
- calcul des indicateurs de conformite

### Gold

La couche Gold construit les jeux analytiques exposes ensuite par l'API:

- `conformite_commune`
- `evolution_parametres`
- `qualite_region`
- `top10_communes`
- `non_conformites`

Ces tables servent a la carte, aux chiffres cles, aux courbes temporelles et aux classements du dashboard.

## Prerequis locaux

Le projet a ete travaille avec:

- Python `3.11`
- Java `17` pour PySpark
- Node `22` pour `semantic-release` dans le workflow de release

Bibliotheques Python utilisees dans le projet:

- `pyspark`
- `pyyaml`
- `requests`
- `duckdb`
- `fastapi`
- `uvicorn`
- `flask`
- `plotly`
- `pyarrow`
- `pytest`

Exemple d'installation minimale:

```powershell
py -3.11 -m pip install pyspark pyyaml requests duckdb fastapi uvicorn flask plotly pyarrow pytest
```

## Commandes utiles

### Lancer le pipeline complet

```powershell
py -3.11 .\src\orchestration\run_pipeline.py
```

### Lancer une partie du pipeline

Depuis Bronze jusqu'a Silver:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage bronze --to-stage silver
```

Uniquement Gold:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage gold --to-stage gold
```

Mode simulation:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --dry-run
```

Avec validation qualite apres Silver:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --with-quality
```

### Lancer Bronze sur une seule API

Par defaut, l'orchestrateur lance Bronze en mode complet. Si tu veux cibler une seule source, tu peux utiliser `--bronze-api`.

Geo API uniquement:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage bronze --to-stage bronze --bronze-api geo_communes
```

Hub'Eau communes uniquement:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage bronze --to-stage bronze --bronze-api hubeau_communes
```

Hub'Eau resultats uniquement:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage bronze --to-stage bronze --bronze-api hubeau_resultats
```

Tu peux aussi combiner cette option avec `--dry-run` pour verifier la commande sans executer l'ingestion:

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage bronze --to-stage bronze --bronze-api geo_communes --dry-run
```

### Lancer les scripts d'ingestion directement

```powershell
py -3.11 .\src\ingestion\bronze_ingestion.py
py -3.11 .\src\ingestion\silver_ingestion.py
py -3.11 .\src\ingestion\gold_ingestion.py
```

### Lancer l'API Gold

```powershell
py -3.11 -m uvicorn src.serving.gold_api:app --reload --port 8000
```

Documentation interactive:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/openapi.json`

### Lancer le dashboard

```powershell
py -3.11 .\src\serving\gold_dashboard.py
```

Puis ouvrir:

- `http://127.0.0.1:8501`

### Lancer les tests

```powershell
py -3.11 -m pytest -q
```

## Dashboard et API

Le dashboard repose sur l'API Gold locale, et non sur une lecture directe des fichiers Parquet.

Vues principales du dashboard actuel:

- carte de conformite
- chiffres cles par annee et communes
- evolution temporelle d'un parametre
- top 20 des communes pour un parametre

Endpoints principaux exposes par l'API:

- `GET /health`
- `GET /tables`
- `GET /gold/conformite-commune`
- `GET /gold/evolution-parametres`
- `GET /gold/qualite-region`
- `GET /gold/top10-communes`
- `GET /gold/non-conformites`
- `GET /gold/dashboard-meta`
- `GET /gold/top-communes-parametre`

## Tests et qualite

Les tests unitaires couvrent:

- le nettoyage des valeurs manquantes
- le calcul de conformite
- les aggregations Gold
- la logique de construction Geo API

La validation qualite complementaire se trouve dans:

- [notebooks/quality/great_expectations_validation.py](./notebooks/quality/great_expectations_validation.py)

Les derniers tests locaux verifies dans ce repo passent avec `pytest -q`.

## CI/CD GitHub Actions

Deux workflows sont presents dans [`.github/workflows`](./.github/workflows):

- [tests.yml](./.github/workflows/tests.yml): execute les tests PySpark sur `push` vers `main` et sur `pull_request`
- [release.yml](./.github/workflows/release.yml): lance `semantic-release` sur `push` vers `main`

Points utiles:

- le workflow `Tests` utilise Python `3.11` et Java `17`
- le workflow `Release` utilise Node `22`, necessaire pour `semantic-release`
- la release automatique suit les conventions de commits (`feat:`, `fix:`, etc.)

## Stockage local et artefacts

Le dossier `data/` contient les sorties locales du pipeline:

- `raw`
- `bronze`
- `silver`
- `gold`
- `logs`

Ce dossier est ignore par Git. Il sert de zone de travail locale et ne fait pas partie du code source versionne.

## Orientation Azure Databricks

Le projet reste exploitable en local, mais son decoupage est compatible avec une orchestration Databricks:

1. tache Bronze
2. tache Silver
3. tache Quality en option
4. tache Gold

La structure medallion, la configuration d'environnement et la separation des couches ont ete pensees pour faciliter cette transition.

## A lire ensuite

- [CONTRIBUTING.md](./CONTRIBUTING.md)
- [docs/orchestration.md](./docs/orchestration.md)
- [docs/api_gold.md](./docs/api_gold.md)
- [docs/dashboard_gold.md](./docs/dashboard_gold.md)
- [docs/quality/README.md](./docs/quality/README.md)
