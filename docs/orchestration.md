# Orchestration Bronze vers Gold

Le point d'entree local est :

- [run_pipeline.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/orchestration/run_pipeline.py)

## Objectif

Executer le pipeline dans l'ordre :

- `bronze`
- `silver`
- `gold`

Optionnellement, la validation Great Expectations peut etre inseree juste apres `silver`.

## Execution locale

### Pipeline complet

```powershell
py -3.11 .\src\orchestration\run_pipeline.py
```

Le script affiche maintenant les logs Bronze, Silver et Gold en direct dans le terminal.

### A partir de Silver

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --from-stage silver --to-stage gold
```

### Avec validation qualite

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --with-quality
```

### Simulation sans execution

```powershell
py -3.11 .\src\orchestration\run_pipeline.py --dry-run
```

## Logs et rapports

Chaque run cree un dossier horodate dans :

- `data/logs/orchestration/<timestamp>/`

Si `data/logs/` n'est pas inscriptible sur Windows, le script bascule automatiquement vers :

- `logs/orchestration/<timestamp>/`

Contenu :

- `bronze.log`
- `silver.log`
- `gold.log`
- `quality.log` si active
- `run_summary.json`
- `run_summary.md`

## Traduction Databricks Workflow

Pour un Workflow Databricks, la structure conseillee est :

1. tache `bronze_ingestion`
   script : `src/ingestion/bronze_ingestion.py`
2. tache `silver_ingestion`
   depend de `bronze_ingestion`
   script : `src/ingestion/silver_ingestion.py`
3. tache `quality_validation`
   optionnelle
   depend de `silver_ingestion`
   notebook : `notebooks/quality/great_expectations_validation.py`
4. tache `gold_ingestion`
   depend de `silver_ingestion` ou de `quality_validation`
   script : `src/ingestion/gold_ingestion.py`

## Recommendation

- en local : utiliser `run_pipeline.py`
- sur Databricks : preferer un Workflow avec une tache par couche pour une meilleure observabilite
