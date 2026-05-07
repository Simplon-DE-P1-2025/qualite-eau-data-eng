# Dashboard Gold local

Le dashboard local consomme l'API Gold HTTP exposee par [gold_api.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/serving/gold_api.py).

Fichier principal :

- [gold_dashboard.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/src/serving/gold_dashboard.py)

## Demarrer l'API locale

```powershell
& "C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe" -m uvicorn src.serving.gold_api:app --reload --port 8000
```

## Demarrer le dashboard

```powershell
& "C:\Users\DELL\AppData\Local\Programs\Python\Python311\python.exe" .\src\serving\gold_dashboard.py
```

Puis ouvrir :

- `http://127.0.0.1:8501`

## Organisation

- une carte de France en ouverture
- un choix de metrique cartographique ou de parametre
- la liste des parametres de la carte est maintenant chargee depuis tout le catalogue `gold_evolution_parametres`, pas seulement depuis le filtre courant
- un bloc KPI de synthese
- une vue regionale et volumetrique
- une vue temporelle par parametre
- les tops communes les plus et moins conformes
- une analyse detaillee des non-conformites

## Endpoints consommes

- `GET /health`
- `GET /tables`
- `GET /gold/conformite-commune`
- `GET /gold/evolution-parametres`
- `GET /gold/qualite-region`
- `GET /gold/top10-communes`
- `GET /gold/non-conformites`
