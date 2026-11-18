-- pipeline_stats
SELECT count(*) AS "Exécutions", count(*) FILTER (WHERE status = 'COMPLETED') AS "Réussies", count(*) FILTER (WHERE status <> 'COMPLETED') AS "En échec", count(*) FILTER (WHERE promoted) AS "Promotions" FROM tracing.pipeline_runs WHERE $__timeFilter(started_at) AND trigger <> 'test';

-- pipeline_runs
SELECT started_at AS "Début", run_name AS "Run", trigger AS "Déclencheur", status AS "État", coalesce(failed_step, '-') AS "Étape en échec", round(duration_s::numeric, 1) AS "Durée (s)", snapshot_rows AS "Lignes", batches AS "Lots", round(test_mae::numeric, 2) AS "MAE test", round(baseline_mae::numeric, 2) AS "MAE baseline", coalesce(model_version, '-') AS "Version", coalesce(qualification, '-') AS "Qualification", left(coalesce(error, ''), 160) AS "Erreur", mlflow_run_id AS "Run MLflow" FROM tracing.pipeline_runs WHERE $__timeFilter(started_at) AND trigger <> 'test' ORDER BY started_at DESC LIMIT 50;

-- pipeline_mae
SELECT started_at AS time, test_mae AS "MAE modèle", baseline_mae AS "MAE baseline" FROM tracing.pipeline_runs WHERE $__timeFilter(started_at) AND trigger <> 'test' AND status = 'COMPLETED' ORDER BY 1;

-- pipeline_duration
SELECT started_at AS time, status AS metric, duration_s AS value FROM tracing.pipeline_runs WHERE $__timeFilter(started_at) AND trigger <> 'test' ORDER BY 1;

-- requests_total
SELECT count(*) AS "Tentatives", count(*) FILTER (WHERE r.http_status < 400) AS "Succès" FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint);

-- rps
SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint AS metric, count(*) / ($__interval_ms / 1000.0) AS value FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) GROUP BY 1, 2 ORDER BY 1;

-- hours_per_s
SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint AS metric, sum(r.n_items) / ($__interval_ms / 1000.0) AS value FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) AND r.http_status = 200 AND r.endpoint LIKE '/predict%' GROUP BY 1, 2 ORDER BY 1;

-- errors_rate
SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint || ' ' || CASE WHEN r.http_status >= 500 THEN '5xx' ELSE '4xx' END AS metric, 100.0 * count(*) FILTER (WHERE r.http_status >= 400) / count(*) AS value FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) GROUP BY 1, 2 ORDER BY 1;

-- errors_table
SELECT r.endpoint AS "Endpoint", r.http_status AS "HTTP", coalesce(r.error_code, '-') AS "Code", count(*) AS "Nombre", round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY r.endpoint), 2) AS "% endpoint" FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) GROUP BY 1, 2, 3 ORDER BY 1, 2;

-- latency
SELECT $__timeGroupAlias(r.started_at, $__interval), percentile_cont(0.5) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS p50, percentile_cont(0.95) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS p95, percentile_cont(0.99) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS p99 FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) AND r.http_status = 200 GROUP BY 1 ORDER BY 1;

-- latency_by_endpoint
SELECT r.endpoint AS "Endpoint", count(*) AS "Requêtes", percentile_cont(0.5) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS "p50 traitement", percentile_cont(0.95) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS "p95 traitement", percentile_cont(0.99) WITHIN GROUP (ORDER BY r.processing_duration_ms) AS "p99 traitement", percentile_cont(0.95) WITHIN GROUP (ORDER BY r.inference_duration_ms) AS "p95 inférence" FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) AND r.http_status = 200 GROUP BY 1 ORDER BY 1;

-- inference
SELECT $__timeGroupAlias(r.started_at, $__interval), r.endpoint AS metric, percentile_cont(0.95) WITHIN GROUP (ORDER BY r.inference_duration_ms) AS value FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) AND r.inference_duration_ms IS NOT NULL GROUP BY 1, 2 ORDER BY 1;

-- batch_sizes
SELECT r.n_items AS "Taille du lot", count(*) AS "Requêtes", sum(r.n_items) AS "Heures prédites" FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) AND r.endpoint = '/predict/batch' AND r.http_status = 200 GROUP BY 1 ORDER BY 1;

-- models
SELECT m.registered_name || ' v' || m.version AS "Modèle", m.qualification AS "Qualification", m.mlflow_run_id AS "Run MLflow", count(*) AS "Prédictions", min(p.created_at) AS "Première", max(p.created_at) AS "Dernière" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.model_versions m USING (model_id) WHERE $__timeFilter(p.created_at) AND r.campaign_id IN ($campaign) GROUP BY 1, 2, 3 ORDER BY 6 DESC;

-- models_ts
SELECT $__timeGroupAlias(p.created_at, $__interval), m.registered_name || ' v' || m.version AS metric, count(*) AS value FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.model_versions m USING (model_id) WHERE $__timeFilter(p.created_at) AND r.campaign_id IN ($campaign) GROUP BY 1, 2 ORDER BY 1;

-- workers
SELECT $__timeGroupAlias(r.started_at, $__interval), 'pid ' || r.worker_pid || ' / ' || left(r.instance_id::text, 8) AS metric, count(*) AS value FROM tracing.api_requests r WHERE $__timeFilter(r.started_at) AND r.campaign_id IN ($campaign) AND r.endpoint IN ($endpoint) GROUP BY 1, 2 ORDER BY 1;

-- feedback_stats
SELECT count(f.feedback_id) AS "Feedbacks", round(100.0 * avg(CASE WHEN f.useful THEN 1 WHEN f.useful = false THEN 0 END), 1) AS "% utiles", round(100.0 * count(f.feedback_id) / NULLIF(count(*), 0), 2) AS "Couverture %" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) LEFT JOIN tracing.feedbacks f USING (prediction_id) WHERE $__timeFilter(p.created_at) AND r.campaign_id IN ($campaign);

-- quality
SELECT m.registered_name || ' v' || m.version AS "Modèle", count(*) AS "Labels observés", round(avg(abs(p.predicted_cnt - f.observed_cnt))::numeric, 2) AS "MAE observée", round(sqrt(avg((p.predicted_cnt - f.observed_cnt) ^ 2))::numeric, 2) AS "RMSE observée" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.feedbacks f USING (prediction_id) JOIN tracing.model_versions m USING (model_id) WHERE $__timeFilter(p.created_at) AND r.campaign_id IN ($campaign) AND f.observed_cnt IS NOT NULL AND f.label_source NOT IN ('loadtest', 'synthetic') GROUP BY 1;

-- quality_loadtest
SELECT f.label_source AS "Source", count(*) AS "Labels", round(avg(abs(p.predicted_cnt - f.observed_cnt))::numeric, 2) AS "MAE (mécanisme uniquement)" FROM tracing.predictions p JOIN tracing.api_requests r USING (request_id) JOIN tracing.feedbacks f USING (prediction_id) WHERE $__timeFilter(p.created_at) AND r.campaign_id IN ($campaign) AND f.observed_cnt IS NOT NULL GROUP BY 1;
