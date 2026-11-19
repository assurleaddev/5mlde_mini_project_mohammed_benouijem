# Déroulé de démonstration (soutenance : 20 min de présentation + 10 min de questions)

Préparation (avant la soutenance) : `docker compose up -d --wait`, variables `MLFLOW_TRACKING_URI` /
`PREFECT_API_URL` définies, `uv run --locked python -m scripts.download_data`, un entraînement initial et un
lot mensuel déjà traités pour que MLflow et Grafana aient du contenu. Ouvrir les 3 interfaces dans des onglets.

| Temps | Séquence | Ce qu'on montre | Critère visé |
|---|---|---|---|
| 0-3 min | Besoin et solution | Prévoir la demande horaire pour planifier le rééquilibrage des stations ; schéma d'architecture du README | Introduction et synthèse |
| 3-5 min | Code et reproductibilité | `main.py` (4 tâches), `config/`, `uv.lock`, `pytest -q` vert | Qualité du code, reproductibilité |
| 5-8 min | Nouveau lot nominal | `simulate_batch.py` puis flow dans Prefect : graphe des tâches, logs, tâche `validate-and-split` en `Cached` au second passage | Intégration du cycle |
| 8-11 min | Traçabilité | Run MLflow : paramètres, snapshot + SHA-256, rapports GX, modèle, tags `prefect.flow_run_id` ; registre : versions, tags `qualification` | Fiabilité et traçabilité |
| 11-13 min | **Décision argumentée** | Sur le poste de démo : v2 est **qualifiée mais non promue** (MAE 49,0 alors que le champion v1, réévalué sur le même test de juillet, fait 44,2 ; tolérance 2 %). Autre cas tracé dans la synthèse : une version meilleure en moyenne **rejetée** car sa MAE sous la pluie vaut 2,8 × la MAE globale (seuil 2,5 fixé avant les résultats) | Argumentation |
| 13-15 min | **Incident contrôlé** | `simulate_batch.py --incident negative_cnt` : rapport GX rejeté, lot en quarantaine, flow `Failed`, run MLflow `FAILED`, alerte Teams reçue, aucun nouveau modèle | Fiabilité |
| 15-17 min | API | `/docs`, `/predict`, `/predict/batch`, `/feedback` (201 puis 200 au rejeu, 409 en conflit), retrouver la prédiction en base | Exploitation du service |
| 17-19 min | Charge + dashboard | Tableau `docs/load_tests` et recommandation ; Grafana : débit, p95, erreurs, versions servies, feedbacks | Dimensionnement, supervision |
| 19-20 min | Limites | Rejeu d'un dataset historique, poste unique, labels de feedback artificiels | Recul |

Questions probables et réponses courtes :

- *Pourquoi un split temporel ?* Le modèle sert à prévoir le futur ; un split aléatoire mélangerait des heures
  voisines entre train et test et surestimerait la performance.
- *Pourquoi refuser un modèle meilleur en moyenne ?* La politique (versionnée avant les essais) protège les
  segments à risque : les heures de pluie sont celles où une mauvaise prévision coûte le plus en exploitation.
- *Que se passe-t-il si MLflow est arrêté ?* Le flow échoue explicitement, aucune publication n'est annoncée ; la
  reprise retrouve une version déjà publiée par son tag `publication_id` au lieu d'en créer une seconde.
- *Pourquoi un manifeste plutôt que l'alias dans l'API ?* Un alias est mutable ; la trace d'une prédiction doit
  désigner la version exacte. Changer de modèle = nouveau manifeste + redémarrage des workers.
- *Retry ou pas ?* Seule l'acquisition est retentée (503/429/timeout, 3 tentatives max). Une donnée invalide
  échoue immédiatement : la retenter produirait le même résultat.
