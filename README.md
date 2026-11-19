# Mini-projet 5MLDE — Prévoir la demande de vélos en libre-service

**Auteur** : Mohammed Benouijem — SUPINFO, module 5MLDE (Déploiement de modèles / MLOps), projet réalisé seul.

Industrialisation complète d'un modèle de régression qui prévoit le **nombre de locations par heure** (`cnt`)
du service Capital Bikeshare (Washington D.C.), de l'ingestion des données à la supervision du service.
Le modèle est volontairement simple : l'objectif est une chaîne **opérationnelle, reproductible et maintenable**.

| Brique | Outil | Où dans le code |
|---|---|---|
| Code modulaire, configuration, tests | Python 3.12, uv, pytest | `main.py`, `src/`, `config/`, `tests/` |
| Ingestion + contrôles qualité bloquants | httpx, Great Expectations 1.23.2 | `src/ingestion.py`, `src/acquisition.py`, `src/data_validation.py` |
| Orchestration, retries, planification, alertes | Prefect 3.8.7 | `main.py`, `deploy.py`, `scripts/setup_alerting.py` |
| Traçabilité, versions, mise en service, retour arrière | MLflow 3.16.1 | `src/tracking.py`, `src/registry.py`, `src/qualification.py`, `scripts/promote.py` |
| API de prédiction, feedbacks, persistance | FastAPI, PostgreSQL 18.6 | `api_prediction/`, `docker/postgres/init-db.sql` |
| Tests de charge et dimensionnement | Locust 2.46.6 | `locustfile.py`, `scripts/run_load_tests.py` |
| Supervision (traitements + service) | Grafana 13.2.3 | `src/run_tracing.py`, `dashboards/bike_api.json`, `docker/grafana/` |

La synthèse des choix, des preuves et des résultats est dans **[docs/SYNTHESE_OPS.md](docs/SYNTHESE_OPS.md)** ;
le déroulé de démonstration est dans [docs/DEMO.md](docs/DEMO.md).

## Architecture

```text
 Producteur (rejeu mensuel)         Prefect (serve)                                  MLflow
 scripts/simulate_batch.py  ──►  data/landing/batch_AAAA-MM.csv                       ┌──────────────────────┐
                                        │                                             │ runs, snapshots,     │
 UCI (source A) ─┐                      ▼                                             │ rapports GX, modèle, │
 miroir (source B)┴─► acquire-history ─► ingestion ─► preprocessing ─► training ─► evaluation ─► registre :     │
   retries 503/429   (sha256, cache)   GX bloquant    split temporel    HGB + log1p   qualification, alias      │
                                       quarantaine    cache Prefect     baseline      champion, historique      │
                                            │                                             └──────────┬───────────┘
                         échec ─► automation Prefect ─► Teams / webhook                              │ manifeste figé
                                                                                                     ▼ (nom + version)
 Clients / Locust ──► FastAPI /predict, /predict/batch, /feedback ──► PostgreSQL (tracing.*) ──► Grafana
```

## Données

- **Source** : [UCI Bike Sharing](https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset), fichier `hour.csv`
  (17 379 heures, 2011-2012). Licence **CC BY 4.0** — Fanaee-T, H. (2013), doi:10.24432/C5W894.
- **Récupération** : `scripts/download_data.py` télécharge l'archive UCI puis, en cas d'échec, un miroir GitHub ;
  l'empreinte SHA-256 de `data/manifest.json` est vérifiée. Les CSV ne sont **pas** versionnés.
- **Arrivée de nouvelles données (simulée)** : le dataset est historique. On entraîne d'abord sur 2011 → juin 2012
  (`history_until`), puis le producteur **rejoue un mois par lot** dans `data/landing/`. C'est explicitement un rejeu.
- Dictionnaire et règles métier : [data/DICTIONNAIRE.md](data/DICTIONNAIRE.md). `casual` et `registered` sont exclus
  des features (fuite de cible : `cnt = casual + registered`).

## Installation (Windows PowerShell, macOS, Linux)

Prérequis : Docker Desktop (ou Docker Engine + Compose), [uv](https://docs.astral.sh/uv/), Git.

```sh
git clone https://github.com/assurleaddev/5mlde_mini_project_mohammed_benouijem.git
cd 5mlde_mini_project_mohammed_benouijem
uv python install 3.12
uv sync --locked
docker compose up -d --wait          # MLflow :7281, Prefect :4200, PostgreSQL :5432, Grafana :3000
uv run --locked python -m scripts.download_data
```

Variables à définir **dans chaque terminal** qui exécute le code (le `.env` du Compose n'est pas lu par Python) :

```powershell
# PowerShell
$env:MLFLOW_TRACKING_URI = "http://localhost:7281"
$env:PREFECT_API_URL     = "http://localhost:4200/api"
```

```sh
# Linux / macOS
export MLFLOW_TRACKING_URI=http://localhost:7281
export PREFECT_API_URL=http://localhost:4200/api
```

Interfaces : MLflow <http://localhost:7281> · Prefect <http://localhost:4200> ·
Grafana <http://localhost:3000> (admin / `5mlde-local-admin`, dossier « Bike MLOps », dashboard
« Bike MLOps — traitements, usage et performances » : exécutions du pipeline + usage de l'API).
Ports et mots de passe de démonstration modifiables via `.env` (voir `.env.example`).
Les ports sont ceux du Compose des TPs : arrêter la stack des TPs avant de lancer celle-ci.
Si un port reste occupé (ex. 5432 par un autre PostgreSQL) : `POSTGRES_PORT=5433` dans `.env`, puis
`API_DB_PORT=5433` dans chaque terminal Python (API, pipeline, tests). Grafana n'est pas concerné : il joint
PostgreSQL par le réseau Docker (`postgres:5432`).
Le schéma PostgreSQL n'est créé qu'au premier démarrage du volume ; pour le réappliquer sans perte :
`docker compose exec -T postgres psql -U postgres -d bike_api -f /docker-entrypoint-initdb.d/010-tracing.sql`.

## Reproduire le fonctionnement

```sh
# 1. Entraînement initial (historique jusqu'au 30/06/2012) : run MLflow, version 1, qualification, alias champion
uv run --locked python main.py --config config/config.json

# 2. Arrivée d'un nouveau lot mensuel puis réentraînement
uv run --locked python scripts/simulate_batch.py           # dépose batch_2012-07.csv
uv run --locked python main.py --config config/config.json

# 3. Déclenchement automatique : watcher (toutes les 2 min) + producteur de démo (toutes les 10 min)
uv run --locked python deploy.py --team benouijem

# 4. Incident contrôlé : lot invalide -> validation GX en échec, quarantaine, flow Failed, alerte
uv run --locked python scripts/simulate_batch.py --incident negative_cnt
uv run --locked python main.py --config config/config.json   # code de sortie non nul, aucun modèle publié

# 5. Mise en service / retour arrière
uv run --locked python -m scripts.promote --show
uv run --locked python -m scripts.promote --version 3       # refusé si la version n'est pas qualifiée
uv run --locked python -m scripts.promote --rollback

# 6. Servir le champion (version figée dans config/serving_manifest.json)
uv run --locked python -m scripts.make_serving_manifest
uv run --locked uvicorn api_prediction.main:app --host 127.0.0.1 --port 8001 --workers 2
#    documentation OpenAPI : http://127.0.0.1:8001/docs

# 7. Charge et dimensionnement (API arrêtée : le script démarre lui-même 1, 2 puis 4 workers)
uv run --locked python -m scripts.run_load_tests
```

Alerting (secret hors Git) :

```powershell
$env:TEAMS_WEBHOOK_URL = "<URL du flux Workflows « Envoyez des alertes Webhook à un canal »>"
uv run --locked python -m scripts.setup_alerting --test   # message de test
uv run --locked python -m scripts.setup_alerting          # automation sur Failed / Crashed
```

Sans Teams : `ALERT_WEBHOOK_URL` vers un récepteur HTTP joignable **depuis le conteneur Prefect**.

## Tests

```sh
uv run --locked python -m pytest -q
```

46 tests : contrat de données, valeurs invalides, doublons, split sans fuite, rechargement identique, catégorie
inconnue, barrière GX (y compris crash GX bloquant et absence d'appel aux étapes suivantes), retries bornés et
fallback de source, retries Prefect seulement sur panne transitoire, politique de qualification (cas acceptés et
refusés), intégration MLflow (run, version, alias, rechargement dans un nouveau processus, absence de doublon),
contrats HTTP, idempotence des feedbacks, panne de stockage sans faux succès et trace des exécutions du pipeline.
Les tests d'API sont ignorés si PostgreSQL (`docker compose up -d postgres`) n'est pas joignable.

## Structure

```text
main.py                      flow Prefect : enchaîne uniquement les 4 tâches mères
deploy.py                    déploiements flow.serve (watcher + producteur de démonstration)
config/                      config.json, qualification_policy.json (versionnée)
src/                         ingestion, acquisition, data_validation, preprocessing, training, evaluation,
                             tracking (MLflow), registry, qualification, orchestration (Prefect), schema, state
api_prediction/              API FastAPI (schemas, service, settings)
scripts/                     download_data, simulate_batch, promote, make_serving_manifest,
                             setup_alerting, run_load_tests, build_dashboard, predict
dashboards/                  bike_api.json + queries.sql (dashboard Grafana provisionné)
docker/                      init-db.sql (schéma tracing), provisioning Grafana
data/                        manifest.json, DICTIONNAIRE.md, exemples (CSV non versionnés)
docs/                        SYNTHESE_OPS.md, DEMO.md, résultats de charge
tests/                       tests automatisés
```
