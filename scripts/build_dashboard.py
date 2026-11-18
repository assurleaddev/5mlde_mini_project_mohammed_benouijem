"""Génère dashboards/bike_api.json (requêtes versionnées ; provisionné automatiquement dans Grafana)."""
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "bike-api-postgres"}
F = "$__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint)"
FP = "$__timeFilter(p.created_at) AND r.campaign_id IN ($campaign)"

FR = "$__timeFilter(started_at) AND trigger <> 'test'"  # exécutions des tests automatisés exclues

QUERIES = {
    "pipeline_stats": f"SELECT count(*) AS \"Exécutions\", count(*) FILTER (WHERE status = 'COMPLETED') AS \"Réussies\", count(*) FILTER (WHERE status <> 'COMPLETED') AS \"En échec\", count(*) FILTER (WHERE promoted) AS \"Promotions\" FROM tracing.pipeline_runs WHERE {FR}",
    "pipeline_runs": f"SELECT started_at AS \"Début\", run_name AS \"Run\", trigger AS \"Déclencheur\", status AS \"État\", coalesce(failed_step, '-') AS \"Étape en échec\", round(duration_s::numeric, 1) AS \"Durée (s)\", snapshot_rows AS \"Lignes\", batches AS \"Lots\", round(test_mae::numeric, 2) AS \"MAE test\", round(baseline_mae::numeric, 2) AS \"MAE baseline\", coalesce(model_version, '-') AS \"Version\", coalesce(qualification, '-') AS \"Qualification\", left(coalesce(error, ''), 160) AS \"Erreur\", mlflow_run_id AS \"Run MLflow\" FROM tracing.pipeline_runs WHERE {FR} ORDER BY started_at DESC LIMIT 50",
    "pipeline_mae": f"SELECT started_at AS time, test_mae AS \"MAE modèle\", baseline_mae AS \"MAE baseline\" FROM tracing.pipeline_runs WHERE {FR} AND status = 'COMPLETED' ORDER BY 1",
    "pipeline_duration": f"SELECT started_at AS time, status AS metric, duration_s AS value FROM tracing.pipeline_runs WHERE {FR} ORDER BY 1",
    "requests_total": f"SELECT count(*) AS \"Tentatives\", count(*) FILTER (WHERE r.http_status < 400) AS \"Succès\" FROM tracing.api_requests r WHERE {F}",
    "rps": f"SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint AS metric, count(*) / ($__interval_ms / 1000.0) AS value FROM tracing.api_requests r WHERE {F} GROUP BY 1, 2 ORDER BY 1",
    "hours_per_s": f"SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint AS metric, sum(r.n_items) / ($__interval_ms / 1000.0) AS value FROM tracing.api_requests r WHERE {F} AND r.http_status = 200 AND r.endpoint LIKE '/predict%' GROUP BY 1, 2 ORDER BY 1",
    "errors_rate": f"SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint || ' ' || CASE WHEN r.http_status >= 500 THEN '5xx' ELSE '4xx' END AS metric, 100.0 * count(*) FILTER (WHERE r.http_status >= 400) / count(*) AS value FROM tracing.api_requests r WHERE {F} GROUP BY 1, 2 ORDER BY 1",
    "errors_table": f"SELECT r.endpoint AS \"Endpoint\", r.http_status AS \"HTTP\", coalesce(r.error_code, '-') AS \"Code\", count(*) AS \"Nombre\", round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY r.endpoint), 2) AS \"% endpoint\" FROM tracing.api_requests r WHERE {F} GROUP BY 1, 2, 3 ORDER BY 1, 2",
    "latency": f"SELECT $__timeGroupAlias(r.started_at, $__interval), percentile_cont(0.5) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS p50, percentile_cont(0.95) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS p95, percentile_cont(0.99) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS p99 FROM tracing.api_requests r WHERE {F} AND r.http_status = 200 GROUP BY 1 ORDER BY 1",
    "latency_by_endpoint": f"SELECT r.endpoint AS \"Endpoint\", count(*) AS \"Requêtes\", percentile_cont(0.5) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS \"p50 traitement\", percentile_cont(0.95) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS \"p95 traitement\", percentile_cont(0.99) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS \"p99 traitement\", percentile_cont(0.95) WITHIN GROUP (ORDER BY r.inference_duration_ms) AS \"p95 inférence\" FROM tracing.api_requests r WHERE {F} AND r.http_status = 200 GROUP BY 1 ORDER BY 1",
    "inference": f"SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint AS metric, percentile_cont(0.95) WITHIN GROUP (ORDER BY r.inference_duration_ms) AS value FROM tracing.api_requests r WHERE {F} AND r.inference_duration_ms IS NOT NULL GROUP BY 1, 2 ORDER BY 1",
    "batch_sizes": f"SELECT r.n_items AS \"Taille du lot\", count(*) AS \"Requêtes\", sum(r.n_items) AS \"Heures prédites\" FROM tracing.api_requests r WHERE {F} AND r.endpoint = '/predict/batch' AND r.http_status = 200 GROUP BY 1 ORDER BY 1",
    "models": f"SELECT m.registered_name || ' v' || m.version AS \"Modèle\", m.qualification AS \"Qualification\", m.mlflow_run_id AS \"Run MLflow\", count(*) AS \"Prédictions\", min(p.created_at) AS \"Première\", max(p.created_at) AS \"Dernière\" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.model_versions m USING (model_id) WHERE {FP} GROUP BY 1, 2, 3 ORDER BY 6 DESC",
    "models_ts": f"SELECT $__timeGroupAlias(p.created_at, $__interval), m.registered_name || ' v' || m.version AS metric, count(*) AS value FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.model_versions m USING (model_id) WHERE {FP} GROUP BY 1, 2 ORDER BY 1",
    "workers": f"SELECT $__timeGroupAlias(r.started_at, $__interval), 'pid ' || r.worker_pid || ' / ' || left(r.instance_id::text, 8) AS metric, count(*) AS value FROM tracing.api_requests r WHERE {F} GROUP BY 1, 2 ORDER BY 1",
    "feedback_stats": f"SELECT count(f.feedback_id) AS \"Feedbacks\", round(100.0 * avg(CASE WHEN f.useful THEN 1 WHEN f.useful = false THEN 0 END), 1) AS \"% utiles\", round(100.0 * count(f.feedback_id) / NULLIF(count(*), 0), 2) AS \"Couverture %\" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) LEFT JOIN tracing.feedbacks f USING (prediction_id) WHERE {FP}",
    "quality": f"SELECT m.registered_name || ' v' || m.version AS \"Modèle\", count(*) AS \"Labels observés\", round(avg(abs(p.predicted_cnt - f.observed_cnt))::numeric, 2) AS \"MAE observée\", round(sqrt(avg((p.predicted_cnt - f.observed_cnt) ^ 2))::numeric, 2) AS \"RMSE observée\" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.feedbacks f USING (prediction_id) JOIN tracing.model_versions m USING (model_id) WHERE {FP} AND f.observed_cnt IS NOT NULL AND f.label_source NOT IN ('loadtest', 'synthetic') GROUP BY 1",
    "quality_loadtest": f"SELECT f.label_source AS \"Source\", count(*) AS \"Labels\", round(avg(abs(p.predicted_cnt - f.observed_cnt))::numeric, 2) AS \"MAE (mécanisme uniquement)\" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.feedbacks f USING (prediction_id) WHERE {FP} AND f.observed_cnt IS NOT NULL GROUP BY 1",
}


def target(sql, fmt="time_series"):
    return [{"datasource": DS, "refId": "A", "rawQuery": True, "editorMode": "code", "format": fmt, "rawSql": sql}]


def panel(pid, title, kind, sql, x, y, w, h, unit=None, fmt=None, description=""):
    fmt = fmt or ("table" if kind in ("table", "stat") else "time_series")
    p = {"id": pid, "title": title, "type": kind, "datasource": DS, "description": description,
         "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": target(sql, fmt),
         "fieldConfig": {"defaults": {"unit": unit or "short", "noValue": "pas de données"}, "overrides": []},
         "options": {}}
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
    return p


PIPELINE = [
    {"id": 105, "type": "row", "title": "Traitements (pipeline d'entraînement)", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(20, "Exécutions du pipeline", "stat", QUERIES["pipeline_stats"], 0, 1, 8, 6,
          description="Écrit par les hooks du flow Prefect (succès comme échec)."),
    panel(21, "MAE par exécution (modèle vs baseline)", "timeseries", QUERIES["pipeline_mae"], 8, 1, 8, 6),
    panel(22, "Durée des exécutions par état", "timeseries", QUERIES["pipeline_duration"], 16, 1, 8, 6, "s"),
    panel(23, "Dernières exécutions", "table", QUERIES["pipeline_runs"], 0, 7, 24, 8),
]
OFFSET = 15

panels = [
    {"id": 100, "type": "row", "title": "Usage", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(1, "Requêtes (tentatives / succès)", "stat", QUERIES["requests_total"], 0, 1, 6, 6),
    panel(2, "Requêtes par seconde", "timeseries", QUERIES["rps"], 6, 1, 9, 6, "reqps"),
    panel(3, "Heures prédites par seconde", "timeseries", QUERIES["hours_per_s"], 15, 1, 9, 6, "short",
          description="Débit de jeux : un lot de 20 heures compte 20."),
    {"id": 101, "type": "row", "title": "Erreurs", "gridPos": {"x": 0, "y": 7, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(4, "Taux d'erreur 4xx / 5xx par endpoint", "timeseries", QUERIES["errors_rate"], 0, 8, 12, 7, "percent"),
    panel(5, "Codes d'erreur", "table", QUERIES["errors_table"], 12, 8, 12, 7),
    {"id": 102, "type": "row", "title": "Durées", "gridPos": {"x": 0, "y": 15, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(6, "Durée de traitement p50 / p95 / p99", "timeseries", QUERIES["latency"], 0, 16, 12, 7, "ms",
          description="processing_duration_ms : validation + inférence, hors commit et réseau. Percentiles calculés sur les requêtes, jamais moyennés entre workers."),
    panel(7, "Inférence p95 par endpoint", "timeseries", QUERIES["inference"], 12, 16, 12, 7, "ms"),
    panel(8, "Durées par endpoint", "table", QUERIES["latency_by_endpoint"], 0, 23, 16, 6, "ms"),
    panel(9, "Tailles de lots", "table", QUERIES["batch_sizes"], 16, 23, 8, 6),
    {"id": 103, "type": "row", "title": "Modèles et workers", "gridPos": {"x": 0, "y": 29, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(10, "Versions servies", "table", QUERIES["models"], 0, 30, 12, 6),
    panel(11, "Prédictions par version", "timeseries", QUERIES["models_ts"], 12, 30, 12, 6),
    panel(12, "Activité par worker (instance)", "timeseries", QUERIES["workers"], 0, 36, 24, 6, "short",
          description="Nombre de requêtes par processus : ce n'est pas une mesure CPU."),
    {"id": 104, "type": "row", "title": "Feedbacks et qualité observée", "gridPos": {"x": 0, "y": 42, "w": 24, "h": 1}, "collapsed": False, "panels": []},
    panel(13, "Feedbacks (cohorte des prédictions de la période)", "stat", QUERIES["feedback_stats"], 0, 43, 8, 6),
    panel(14, "Qualité observée (labels réels, hors tests de charge)", "table", QUERIES["quality"], 8, 43, 8, 6,
          description="Uniquement les prédictions avec observed_cnt ; population auto-sélectionnée."),
    panel(15, "Labels par source (dont artificiels)", "table", QUERIES["quality_loadtest"], 16, 43, 8, 6),
]

for p in panels:
    p["gridPos"]["y"] += OFFSET
panels = PIPELINE + panels

dashboard = {
    "uid": "bike-api-usage", "title": "Bike MLOps — traitements, usage et performances", "schemaVersion": 39, "version": 2,
    "refresh": "30s", "time": {"from": "now-1h", "to": "now"}, "timezone": "browser", "tags": ["bike", "mlops"],
    "templating": {"list": [
        {"name": "campaign", "label": "Campagne", "type": "query", "datasource": DS, "multi": True, "includeAll": True,
         "current": {"selected": True, "text": "All", "value": "$__all"}, "refresh": 2, "sort": 1,
         "query": "SELECT DISTINCT campaign_id FROM tracing.api_requests WHERE $__timeFilter(started_at) AND campaign_id <> 'pytest'"},
        {"name": "endpoint", "label": "Endpoint", "type": "custom", "multi": True, "includeAll": True,
         "current": {"selected": True, "text": "All", "value": "$__all"},
         "query": "/predict,/predict/batch,/feedback",
         "options": [{"text": e, "value": e, "selected": False} for e in ("/predict", "/predict/batch", "/feedback")]},
    ]},
    "panels": panels,
}

out = Path(__file__).resolve().parents[1] / "dashboards" / "bike_api.json"
out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False), encoding="utf-8")
(out.parent / "queries.sql").write_text("\n\n".join(f"-- {k}\n{v};" for k, v in QUERIES.items()) + "\n", encoding="utf-8")
print(f"Dashboard écrit : {out}")
