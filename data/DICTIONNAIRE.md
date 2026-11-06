# Dictionnaire des données — UCI Bike Sharing (hour.csv)

Source : Capital Bikeshare (Washington D.C.), agrégation horaire 2011-2012, 17 379 lignes.
Licence CC BY 4.0 — Fanaee-T, H. (2013), UCI Machine Learning Repository, doi:10.24432/C5W894.
Les CSV ne sont pas versionnés : `uv run python scripts/download_data.py` les récupère et vérifie l'empreinte SHA-256.

| Colonne | Type | Rôle | Description / domaine |
|---|---|---|---|
| instant | entier | identifiant | Index de l'enregistrement, unique |
| dteday | date ISO | temps | Jour de l'observation (sert au split temporel, jamais de feature brute) |
| season | entier 1-4 | feature catégorielle | 1 hiver, 2 printemps, 3 été, 4 automne |
| yr | entier 0-1 | feature | 0 = 2011, 1 = 2012 (tendance de croissance du service) |
| mnth | entier 1-12 | feature catégorielle | Mois |
| hr | entier 0-23 | feature catégorielle | Heure |
| holiday | 0/1 | feature | Jour férié |
| weekday | entier 0-6 | feature catégorielle | Jour de la semaine |
| workingday | 0/1 | feature | Jour ouvré (ni week-end ni férié) |
| weathersit | entier 1-4 | feature catégorielle | 1 clair … 4 très dégradé (pluie forte, neige) |
| temp | réel [0,1] | feature | Température normalisée (÷ 41 °C) |
| atemp | réel [0,1] | feature | Température ressentie normalisée (÷ 50 °C) |
| hum | réel [0,1] | feature | Humidité normalisée (÷ 100) |
| windspeed | réel [0,1] | feature | Vent normalisé (÷ 67) |
| casual | entier ≥ 0 | **exclu** | Locations occasionnels — fuite de cible (cnt = casual + registered) |
| registered | entier ≥ 0 | **exclu** | Locations abonnés — fuite de cible |
| cnt | entier ≥ 0 | **cible** | Nombre total de locations sur l'heure |

Contraintes métier : une seule ligne par couple (`dteday`, `hr`) ; `cnt = casual + registered` ;
les variables météo restent dans [0, 1]. Certaines heures sont absentes (pannes du système en 2011) :
c'est une propriété connue de la source, pas une anomalie bloquante.
