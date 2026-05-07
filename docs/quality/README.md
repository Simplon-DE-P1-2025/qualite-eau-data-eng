# Rapports Great Expectations

Ce dossier est alimente par [great_expectations_validation.py](/c:/Users/DELL/Documents/vscode_simplon/brief_qualite_eau_local/notebooks/quality/great_expectations_validation.py).

Structure attendue apres execution :

- `latest/` : derniere version des Data Docs HTML
- `archive/<timestamp>/` : copie horodatee de chaque execution
- `validation_summary.json` : synthese machine-readable
- `validation_summary.md` : synthese lisible

Le point d'entree HTML principal est `index.html` dans `latest/` ou dans un dossier `archive/<timestamp>/`.
