# API Gold

Cette documentation couvre :

- l'API locale exposee par [gold_api.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/serving/gold_api.py)
- l'exposition des tables Gold via Databricks SQL Statement Execution API

## API locale

### Demarrage

```powershell
& "C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe" -m uvicorn src.serving.gold_api:app --reload --port 8000
```

Documentation interactive :

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/openapi.json`

### Endpoints

- `GET /health`
- `GET /tables`
- `GET /gold/conformite-commune`
- `GET /gold/evolution-parametres`
- `GET /gold/qualite-region`
- `GET /gold/top10-communes`
- `GET /gold/non-conformites`

### Parametres par endpoint

`GET /gold/conformite-commune`

- `annee_prelevement`
- `code_departement`
- `code_commune`
- `code_region_geo`
- `min_taux_conformite_pct`
- `limit`
- `offset`

`GET /gold/evolution-parametres`

- `annee_prelevement`
- `mois_prelevement`
- `code_departement`
- `code_commune`
- `code_parametre`
- `libelle_parametre_norm`
- `categorie_parametre`
- `limit`
- `offset`

`GET /gold/qualite-region`

- `annee_prelevement`
- `code_region_geo`
- `min_taux_conformite_pct`
- `limit`
- `offset`

`GET /gold/top10-communes`

- `classement_type`
- `code_region_geo`
- `limit`
- `offset`

`GET /gold/non-conformites`

- `annee_prelevement`
- `code_departement`
- `code_commune`
- `code_parametre`
- `categorie_parametre`
- `limit`
- `offset`

### Format JSON

```json
{
  "table": "gold_conformite_commune",
  "gold_root": "C:/.../data/gold",
  "filters": {
    "annee_prelevement": 2026
  },
  "limit": 100,
  "offset": 0,
  "row_count": 11,
  "rows": []
}
```

## Databricks SQL Warehouse

Sources officielles :

- https://docs.databricks.com/aws/en/compute/sql-warehouse/create
- https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial

### Mise en place

1. creer un SQL Warehouse
2. donner les droits `Can use`
3. verifier l'acces aux tables Gold avec `SHOW TABLES IN water_quality`
4. appeler l'API REST Databricks SQL

### Endpoints REST Databricks

- `POST /api/2.0/sql/statements`
- `GET /api/2.0/sql/statements/{statement_id}`
- `GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}`
- `POST /api/2.0/sql/statements/{statement_id}/cancel`

### Payload type

```json
{
  "warehouse_id": "<warehouse-id>",
  "catalog": "hive_metastore",
  "schema": "water_quality",
  "statement": "SELECT * FROM gold_conformite_commune LIMIT :row_limit",
  "parameters": [
    { "name": "row_limit", "value": "100", "type": "INT" }
  ],
  "format": "JSON_ARRAY",
  "disposition": "INLINE",
  "wait_timeout": "30s"
}
```

## Requetes SQL d'exemple

```sql
SELECT annee_prelevement, nom_commune_norm, taux_conformite_pct
FROM water_quality.gold_conformite_commune
ORDER BY annee_prelevement DESC, taux_conformite_pct DESC
LIMIT 100;
```

```sql
SELECT annee_prelevement, mois_prelevement, libelle_parametre_norm, valeur_moyenne
FROM water_quality.gold_evolution_parametres
WHERE libelle_parametre_norm = 'NITRATES (EN NO3)'
ORDER BY annee_prelevement, mois_prelevement
LIMIT 100;
```

```sql
SELECT annee_prelevement, code_region_geo, taux_conformite_pct
FROM water_quality.gold_qualite_region
ORDER BY annee_prelevement DESC, taux_conformite_pct DESC
LIMIT 100;
```

```sql
SELECT classement_type, rang, nom_commune_norm, taux_conformite_pct
FROM water_quality.gold_top10_communes
ORDER BY classement_type, rang;
```

```sql
SELECT annee_prelevement, nom_commune_norm, libelle_parametre_norm, nb_prelevements_non_conformes
FROM water_quality.gold_non_conformites
ORDER BY nb_prelevements_non_conformes DESC
LIMIT 100;
```
