# 🏛 Araneus - Défi Data.gouv (Municipales & Enjeux Locaux)

## 🛠 Conventions de Code
- **dbt** : Un modèle = un contrat dbt.
  - Naming : `<layer>_<source>__<entity>` (ex: `stg_dgouv__elections_2020`)
- **Vector DB** : Indexation standardisée.
  - Naming : `idx_<source>_<content>_<model_version>`
- **Python** : Structure fonctionnelle.
  - Fichiers : `<type>_<function>.py` (ex: `tool_fetch_results.py`)
  - Environnement : Utilisation de `uv` pour la gestion du venv et des dépendances (`uv run`, `uv pip`).

## 🌳 Stratégie Git & Workflow PR
- **Branches** : Interdiction de commit sur `main`. Format : `feat/description` ou `fix/description`.
- **Identité Commits** :
  - Author : `spideystreet <dhicham.pro@gmail.com>`
  - Co-author : `spicode-bot <263227865+spicode-bot@users.noreply.github.com>` (via trailer `Co-authored-by:`)
- **Pull Requests** :
  - Ouvertes par `spicode-bot`.
  - Reviewer : `spideystreet`.
- **Checkpointing** : Avant toute modification lourde, utiliser `!gemini checkpoint "description"`.

## 🧠 Contexte & Mémoire
- `/memory list` : Vérifier les fichiers chargés.
- `/memory refresh` : Recharger après modif des `GEMINI.md`.
- Hiérarchie : Global (`~/.gemini/`) > Projet (`./GEMINI.md`) > Local (`./GEMINI.local.md`).

## 🔌 MCP Data.gouv
- Utiliser `/mcp list` pour découvrir les outils.
- Exploration systématique : datasets -> ressources -> métadonnées -> réutilisations.
