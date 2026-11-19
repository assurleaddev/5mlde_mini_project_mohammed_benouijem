# Synthèse des choix et résultats OPS

**Sujet** : prévoir le nombre de locations de vélos par heure (Capital Bikeshare, UCI Bike Sharing, CC BY 4.0)
pour aider à planifier le rééquilibrage des stations. Le score n'est pas un objectif : les métriques servent
à illustrer le suivi et les décisions de mise en service.

> Les mesures ci-dessous ont été obtenues le 19/11/2026 sur l'environnement de développement (conteneur Linux,
> 2 vCPU, 7 Go de RAM ; MLflow, Prefect et PostgreSQL lancés en local, Locust sur la même machine).
> Elles se rejouent à l'identique avec les commandes indiquées ; les chiffres de capacité dépendent du poste.

## 1. Décisions techniques

| Sujet | Décision | Justification |
|---|---|---|
| Données dynamiques | Historique jusqu'au 30/06/2012, puis **rejeu d'un mois par lot** dans `data/landing/` | Dataset historique : le rejeu par lots, annoncé comme tel, simule l'arrivée de nouvelles données |
| Acquisition | UCI puis miroir GitHub (fallback), empreinte SHA-256 obligatoire | Une source indisponible ne bloque pas ; un contenu différent est refusé (erreur définitive) |
| Qualité des données | 2 suites GX bloquantes : `bike_batch` (chaque lot, avant fusion) et `bike_snapshot` (corpus entraîné) + audit `cnt = casual + registered` | Un lot dégradé ne doit jamais atteindre l'entraînement ; un crash GX vaut échec |
| Lots rejetés | Rapport JSON + **quarantaine** (`data/quarantine/`) | Pas de rejeu en boucle d'un lot invalide inchangé ; correction traçable |
| Split | **Temporel** : 28 derniers jours en test | Le modèle prévoit le futur ; un split aléatoire mélangerait des heures voisines (fuite) |
| Features | 12 variables calendaires et météo ; `casual`/`registered` exclues | Fuite de cible : leur somme vaut `cnt` |
| Modèle | Pipeline sklearn (OneHotEncoder appris sur train + HistGradientBoosting sur `log1p(cnt)`) | Simple, rapide, sérialisé avec son encodage, catégories inconnues tolérées |
| Baseline | Moyenne de `cnt` par (heure, jour ouvré) | Référence métier pour juger l'apport du modèle |
| Traçabilité | 1 run MLflow par tentative ; snapshot, rapports GX, split, modèle, durées, commit Git | Répondre à « avec quelles données, quel code, pourquoi ce modèle ? » |
| Qualification | Politique versionnée `config/qualification_policy.json`, écrite **avant** les essais | Éviter de déplacer les seuils pour faire passer le dernier modèle |
| Mise en service | Alias `champion` déplacé seulement si qualifié **et** pas plus de 2 % moins bon que le champion réévalué sur le même test ; `promote.py --rollback` | Qualification absolue ≠ meilleur que le modèle en service |
| Orchestration | Flow Prefect `bike-demand-training` : 4 tâches mères, 0 retry ; sous-tâche `acquire-history` 2 retries sur panne transitoire uniquement | Une seule couche de retry ; une donnée invalide n'est jamais retentée |
| Déclenchement | `deploy.py` : watcher planifié qui n'entraîne que si l'ensemble des lots a changé | Pas de réentraînement inutile ni de doublon de version |
| Alerting | Automation Prefect créée par SDK (Failed/Crashed) → block Teams Workflows (repli webhook HTTP) | Côté serveur : fonctionne même si le processus du flow a disparu |
| Service | FastAPI, version **figée** dans `config/serving_manifest.json`, chargée une fois par worker | Un alias est mutable ; chaque prédiction doit désigner la version exacte |
| Persistance | PostgreSQL `tracing.*` : 1 ligne de requête + N prédictions par transaction, réponse après commit | Pas de prédiction partielle, pas de faux succès |
| Feedbacks | Un feedback immuable par prédiction, idempotent (201/200/409/404), conflits arbitrés par contraintes SQL | Un retry réseau ne compte pas deux avis |

## 2. Preuves d'intégration (exécutions réelles)

### Cycle nominal et qualification

| Version | Données | Test | MAE | Baseline | R² | Décision |
|---|---|---|---:|---:|---:|---|
| v1 | historique + lot 2012-07 | 04/07 → 31/07/2012 | **49,0** | 111,2 | 0,872 | `production-ready` → alias `champion` |
| v2 | + lot 2012-08 | 04/08 → 31/08/2012 | **32,7** | 118,2 | 0,941 | `rejected` : `no_segment_collapse` |
| v3, v4 | mêmes données (relances manuelle et planifiée) | idem | 32,7098 | 118,2 | 0,941 | `rejected` (même motif) |

- **Décision argumentée** : v2 est meilleure en moyenne, mais sa MAE sous la pluie (`weathersit=3`, 38 heures)
  vaut 92,9, soit **2,84 × la MAE globale** (seuil 2,5 fixé avant les essais). Le champion v1 reste en service :
  ce sont précisément les heures où une mauvaise prévision coûte le plus. Piste d'amélioration : ajouter des
  données de pluie ou pondérer ce segment — pas abaisser le seuil.
- **Reproductibilité** : v2, v3 et v4 (mêmes snapshot, configuration et graine) donnent exactement la même MAE
  (32,7098) ; le snapshot est identifié par son SHA-256 dans le run et dans les tags de version.
- **Mise en service contrôlée** : `promote.py --version 2` est refusé (« v2 est 'rejected' ») ; un tag posé à la
  main ne suffit pas, la politique courante est revérifiée.

### Rejeu complet sur le poste de démonstration (Windows 11, Docker Desktop, 19/11/2026)

Même code, même dataset (SHA-256 vérifié), stack Docker du projet ; séquence : entraînement initial, lot 2012-07,
lot invalide. Les 46 tests passent sur ce poste.

| Run | Données | MAE | Baseline | R² | Qualification | Mise en service |
|---|---|---:|---:|---:|---|---|
| v1 | historique jusqu'au 30/06/2012 (13 003 lignes) | **35,7** | 125,1 | 0,944 | `production-ready` | alias `champion` (aucun champion avant) |
| v2 | + lot 2012-07 (13 747 lignes) | **49,0** | 111,2 | 0,872 | `production-ready` | **non promue** : le champion v1, réévalué sur le même test de juillet, obtient 44,2 ; limite = 44,2 × 1,02 = 45,1 < 49,0 |
| — | + lot 2012-08 avec `cnt` négatif | — | — | — | — | flow `Failed` à `ingestion`, lot en quarantaine, run MLflow `FAILED`, ligne `FAILED` dans `pipeline_runs` |

Ce second scénario illustre l'autre règle de mise en service : une version **qualifiée** n'est promue que si
elle n'est pas moins bonne que le modèle en service **sur les mêmes données**. L'API (2 workers) sert la version
exacte v1 du manifeste ; p95 de traitement ≈ 22 ms sur les appels manuels ; les 19 panneaux Grafana renvoient
des données (3 exécutions dont 1 en échec, versions servies, workers, durées).

Les deux scénarios (environnement de développement ci-dessus et poste de démonstration) diffèrent parce que
l'ordre des lots et les périodes de test diffèrent : c'est attendu, et chaque décision est tracée dans MLflow.

### Incidents contrôlés

| Incident | Résultat observé |
|---|---|
| Lot avec `cnt` négatif (`--incident negative_cnt`) | `bike_batch` en échec (`expect_column_values_to_be_between(cnt)` + audit), lot en quarantaine, tâche `ingestion` Failed, flow Failed, run MLflow `FAILED` avec `pipeline.failed_step=ingestion`, **alerte reçue** avec flow run, état, cause et `mlflow_run:<id>` |
| Valeur texte (`temp="chaud"`), colonne absente, doublons, lot tronqué | Rejet avant prétraitement avec cause identifiable (tests automatisés) |
| Crash de Great Expectations | Échec bloquant, jamais un passage implicite (test) |
| Source 503 puis succès / 503 permanent | 2 appels puis succès / 3 tentatives max puis échec ; 404 et mauvaise empreinte sans retry (tests) |
| Lot inchangé au passage suivant du watcher | « Aucun nouveau lot » : pas d'entraînement |
| Même snapshot réentraîné | Tâche `validate-and-split` en état `Cached` (clé = SHA-256 + paramètres + version du code) |
| Lot API invalide (1 heure sur 2 hors bornes) | 422, trace de la requête, **0 prédiction** enregistrée |
| Panne d'écriture PostgreSQL pendant un lot | 503, aucune prédiction partielle, trace d'erreur dans une transaction séparée |
| Feedback rejoué / modifié / sur prédiction inconnue | 200 sans nouvelle ligne / 409 / 404 |

## 3. Tests de charge et dimensionnement

**Objectif de service fixé avant les mesures** : p95 < 300 ms et < 1 % d'erreurs pour `/predict` ;
p95 < 800 ms pour `/predict/batch` (20 heures). Scénario : utilisateurs Locust avec 0,5 à 1,5 s d'attente,
réponses vérifiées, traçabilité PostgreSQL **activée** (c'est le service complet qui est mesuré), 1 thread par
modèle (`OMP_NUM_THREADS=1`), pool de 5 connexions par worker. Tirs de 30 s après une chauffe.

| Workers | Utilisateurs | `/predict` req/s | p50 | p95 | p99 | Erreurs | CPU API | RAM API | SLO |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| 1 | 40 | 38,0 | 30 ms | 200 ms | 460 ms | 0 % | 49 % | 292 Mo | ✅ |
| 1 | 80 | 52,4 | 510 ms | 1 000 ms | 1200 ms | 0 % | 84 % | 294 Mo | ❌ |
| 1 | 160 | 59,8 | 1900 ms | 2 700 ms | 2800 ms | 0 % | 97 % | 298 Mo | ❌ |
| 2 | 40 | 39,1 | 17 ms | 80 ms | 280 ms | 0 % | 46 % | 618 Mo | ✅ |
| 2 | 80 | **75,4** | 32 ms | **150 ms** | 510 ms | 0 % | 88 % | 620 Mo | ✅ |
| 2 | 160 | 114,0 | 290 ms | 1 100 ms | 1400 ms | 0 % | 130 % | 626 Mo | ❌ |
| 4 | 80 | 76,2 | 23 ms | 120 ms | 570 ms | 0 % | 85 % | 1 198 Mo | ✅ |
| 4 | 160 | 139,6 | 59 ms | 630 ms | 810 ms | 0 % | 136 % | 1 204 Mo | ❌ |

Lots de 20 heures (`/predict/batch[20]`) à 40 utilisateurs : 36,9 req/s (p95 300 ms) avec 1 worker,
38,6 req/s (p95 120 ms) avec 2 workers, 38,9 req/s (p95 60 ms) avec 4 workers, soit **~770 heures prédites/s**.
Tableaux complets (p50/p99 par palier) : `docs/load_tests/*-summary.md`.

**Lecture**

- 1 worker sature vers 50-60 req/s (CPU d'un cœur à ~100 %, p95 qui explose) : la capacité soutenable mesurée
  est **~38 req/s** (dernier palier conforme).
- 2 workers doublent la capacité soutenable (**~75 req/s** conformes au SLO). 4 workers n'apportent rien de plus
  au palier conforme sur cette machine à 2 vCPU (Locust partage le CPU) mais doublent la mémoire ; ils
  dégradent moins vite en surcharge.
- Le lot est ~20 × plus efficace en heures prédites par requête : à privilégier pour les usages de planification.
- La charge injectée dépend du temps d'attente Locust : des utilisateurs virtuels ne sont pas des requêtes/s.

**Recommandation chiffrée** : **2 workers Uvicorn par vCPU disponible jusqu'à 2 vCPU, pool PostgreSQL de 5 par
worker, ~650 Mo de RAM pour l'API**. Capacité soutenable mesurée : ~75 req/s unitaires (ou ~770 heures/s en lots
de 20) avec p95 ≤ 150 ms. Au-delà, ajouter des instances (scaling horizontal plafonné) plutôt que des workers
sur la même machine. Ajouter 20 connexions PostgreSQL au budget pour 4 workers.

**Limites du protocole** : une seule répétition par palier (mesures exploratoires ; `--repeats 3` pour le
palier candidat et le suivant), générateur et API sur la même machine, base locale, durée de 30 s.

## 4. Supervision

Dashboard Grafana provisionné « Bike MLOps — traitements, usage et performances » (`dashboards/bike_api.json`,
requêtes dans `dashboards/queries.sql`, validées avec le compte en lecture seule `grafana_reader`) :

- **Traitements** : chaque exécution du flow (succès comme échec) est écrite par ses hooks d'état dans
  `tracing.pipeline_runs` : état, étape en échec, cause, durée, lignes du snapshot, lots, MAE modèle / baseline,
  version, qualification, promotion, run MLflow. Panneaux : compteurs (réussies / en échec / promotions),
  MAE par exécution, durées par état, tableau des dernières exécutions. Exemple réel : un run `COMPLETED`
  (14 491 lignes, 2 lots, MAE 32,7, `rejected`) puis un run `FAILED` à l'étape `ingestion` (lot avec `hum` hors bornes).
- **Service** : requêtes/s et heures/s, taux 4xx/5xx et codes, p50/p95/p99 de traitement (percentiles calculés
  sur les requêtes, pas moyennés), inférence séparée, tailles de lots, versions exactes servies, activité par
  worker, feedbacks, couverture et MAE observée. Filtres campagne et endpoint.

Les feedbacks `label_source=loadtest` sont exclus des indicateurs de qualité réelle. Les traces des tirs de charge
(29 000 requêtes, 153 000 prédictions) ont servi à vérifier les requêtes. L'écriture de `pipeline_runs` est
« best effort » : PostgreSQL indisponible ne fait pas échouer le pipeline (Prefect et MLflow restent la référence).

## 5. Limites connues

- Données rejouées depuis un historique 2011-2012 : la dérive réelle n'est pas observée, seulement simulée.
- Le segment pluie est peu représenté (38-45 heures par mois de test) : le contrôle de segment est sensible.
- Le corpus de qualification change à chaque lot (28 derniers jours) ; la comparaison au champion se fait sur le
  même split, mais il n'existe pas de jeu de test final indépendant figé.
- Les feedbacks de démonstration sont artificiels ; une vraie boucle exigerait les comptages réels des stations.
- Le watcher et le worker API tournent sur un seul poste ; aucune haute disponibilité, pas de file de reprise
  pour les traces perdues si PostgreSQL est indisponible (journal local `artifacts/api_errors.jsonl`).
- Les liens `localhost` des alertes ne fonctionnent que sur le poste qui héberge Prefect.
