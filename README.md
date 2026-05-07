# Pipeline Qualite de l'Eau

Projet reorganise pour se rapprocher d'un Repo Databricks :

- `src/ingestion/` : scripts Bronze, Silver, Gold
- `src/serving/` : API locale Gold et dashboard
- `notebooks/quality/` : validation Great Expectations
- `config/` : configuration centralisee
- `docs/` : documentation et rapports
- `data/` : sorties locales parquet

## Arborescence

```text
brief_qualite_eau_local/
├── config/
│   └── config.yml
├── data/
├── docs/
│   ├── api_gold.md
│   ├── dashboard_gold.md
│   └── quality/
├── notebooks/
│   └── quality/
│       └── great_expectations_validation.py
├── src/
│   ├── ingestion/
│   │   ├── bronze_ingestion.py
│   │   ├── silver_ingestion.py
│   │   └── gold_ingestion.py
│   ├── orchestration/
│   │   └── run_pipeline.py
│   └── serving/
│       ├── gold_api.py
│       └── gold_dashboard.py
├── tests/
│   ├── spark_test.py
│   └── test.py
└── README.md
```

## Scripts principaux

- [bronze_ingestion.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/ingestion/bronze_ingestion.py)
- [silver_ingestion.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/ingestion/silver_ingestion.py)
- [gold_ingestion.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/ingestion/gold_ingestion.py)
- [run_pipeline.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/orchestration/run_pipeline.py)
- [gold_api.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/serving/gold_api.py)
- [gold_dashboard.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/serving/gold_dashboard.py)
- [great_expectations_validation.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/notebooks/quality/great_expectations_validation.py)
- [config.yml](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/config/config.yml)

## Execution locale

### Orchestration complete

```powershell
py -3.11 .\src\orchestration\run_pipeline.py
```

Avec validation qualite :

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --with-quality
```

Simulation :

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --dry-run
```

### Bronze

```powershell
py -3.11 .\src\ingestion\bronze_ingestion.py
```

### Silver

```powershell
py -3.11 .\src\ingestion\silver_ingestion.py
```

### Gold

```powershell
py -3.11 .\src\ingestion\gold_ingestion.py
```

### Great Expectations

```powershell
py -3.11 .\notebooks\quality\great_expectations_validation.py
```

### API Gold

```powershell
& "C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe" -m uvicorn src.serving.gold_api:app --reload --port 8000
```

### Dashboard Gold

```powershell
& "C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe" .\src\serving\gold_dashboard.py
```

## Couches de donnees

- `bronze` : ingestion brute Hub'Eau et Geo API
- `silver` : typage, normalisation, deduplication, flags qualite
- `gold` : tables agregeses par cas d'usage

## Tables Gold

### `gold_conformite_commune`

- usage : suivre la conformite par commune
- colonnes clefs : `annee_prelevement`, `code_commune`, `nom_commune_norm`, `taux_conformite_pct`, `nb_prelevements_total`

### `gold_evolution_parametres`

- usage : suivre l'evolution temporelle d'un parametre
- colonnes clefs : `annee_prelevement`, `mois_prelevement`, `libelle_parametre_norm`, `valeur_moyenne`, `nb_mesures`

### `gold_qualite_region`

- usage : carte de qualite par region
- colonnes clefs : `annee_prelevement`, `code_region_geo`, `taux_conformite_pct`, `population_couverte`

### `gold_top10_communes`

- usage : top 10 communes les plus ou moins conformes
- colonnes clefs : `classement_type`, `rang`, `nom_commune_norm`, `taux_conformite_pct`, `taux_non_conformite_pct`

### `gold_non_conformites`

- usage : analyser les non-conformites par commune et parametre
- colonnes clefs : `annee_prelevement`, `nom_commune_norm`, `libelle_parametre_norm`, `nb_prelevements_non_conformes`, `pct_outliers`

## Requetes SQL d'exemple

### Conformite par commune

```sql
SELECT
  annee_prelevement,
  nom_commune_norm,
  taux_conformite_pct,
  nb_prelevements_total
FROM water_quality.gold_conformite_commune
ORDER BY annee_prelevement DESC, taux_conformite_pct DESC;
```

### Evolution d'un parametre

```sql
SELECT
  annee_prelevement,
  mois_prelevement,
  libelle_parametre_norm,
  valeur_moyenne
FROM water_quality.gold_evolution_parametres
WHERE libelle_parametre_norm = 'NITRATES (EN NO3)'
ORDER BY annee_prelevement, mois_prelevement;
```

### Qualite par region

```sql
SELECT
  annee_prelevement,
  code_region_geo,
  taux_conformite_pct
FROM water_quality.gold_qualite_region
ORDER BY annee_prelevement DESC, taux_conformite_pct DESC;
```

### Top 10 communes

```sql
SELECT
  classement_type,
  rang,
  nom_commune_norm,
  taux_conformite_pct,
  taux_non_conformite_pct
FROM water_quality.gold_top10_communes
ORDER BY classement_type, rang;
```

### Non-conformites

```sql
SELECT
  annee_prelevement,
  nom_commune_norm,
  libelle_parametre_norm,
  nb_prelevements_non_conformes
FROM water_quality.gold_non_conformites
ORDER BY nb_prelevements_non_conformes DESC;
```

## Databricks

Cette structure est plus proche d'un Repo Databricks :

- `src/ingestion/` pour les scripts Python executables en Job
- `notebooks/quality/` pour le notebook de validation
- `config/config.yml` pour la configuration partagee
- `docs/` pour les rapports exportes

Pour Databricks / Azure Databricks :

1. importer le projet dans un Repo
2. ajuster `environment` dans [config.yml](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/config/config.yml)
3. creer un Workflow avec les dependances `bronze -> silver -> gold`
4. ajouter `notebooks/quality/great_expectations_validation.py` entre `silver` et `gold` si besoin
5. lancer les scripts `src/ingestion/*.py` comme taches Python

## Documentation complementaire

- [docs/api_gold.md](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/docs/api_gold.md)
- [docs/dashboard_gold.md](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/docs/dashboard_gold.md)
- [docs/orchestration.md](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/docs/orchestration.md)
- [docs/quality/README.md](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/docs/quality/README.md)
