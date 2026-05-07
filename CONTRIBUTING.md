# Conventions de commits

Ce projet utilise [Conventional Commits](https://www.conventionalcommits.org/).
Semantic Release génère automatiquement les versions et le CHANGELOG.

## Format

<type>(<scope>): <description courte>

## Types et impact sur la version

| Type       | Description                          | Version |
|------------|--------------------------------------|---------|
| `feat`     | Nouvelle fonctionnalité              | MINOR → 1.1.0 |
| `fix`      | Correction de bug                    | PATCH → 1.0.1 |
| `chore`    | Maintenance, dépendances             | Aucun |
| `docs`     | Documentation uniquement             | Aucun |
| `refactor` | Refactoring sans bug/feature         | Aucun |
| `test`     | Ajout ou correction de tests         | Aucun |
| `ci`       | Modification CI/CD                   | Aucun |
| `BREAKING CHANGE` | Rupture de compatibilité    | MAJOR → 2.0.0 |

## Exemples

# Nouvelle feature → v1.1.0
feat(bronze): add pagination support for Hub'Eau API

# Correction de bug → v1.0.1
fix(silver): handle null values in resultat_alphanumerique

# Breaking change → v2.0.0
feat(gold)!: restructure conformite table schema

BREAKING CHANGE: conformite_globale column renamed to conformite_status

# Pas de release
chore(deps): update pyyaml to 6.0.1
docs(readme): add setup instructions