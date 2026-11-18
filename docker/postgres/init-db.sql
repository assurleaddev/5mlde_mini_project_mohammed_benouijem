-- Schéma de traçabilité de l'API de prédiction (exécuté automatiquement au premier démarrage du volume).
-- Réexécutable : psql -U postgres -d bike_api -f /docker-entrypoint-initdb.d/010-tracing.sql
\set ON_ERROR_STOP on
\getenv api_password API_DB_PASSWORD
\getenv grafana_password GRAFANA_DB_PASSWORD

BEGIN;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'api_writer') THEN CREATE ROLE api_writer LOGIN; END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN CREATE ROLE grafana_reader LOGIN; END IF;
END
$roles$;

ALTER ROLE api_writer PASSWORD :'api_password';
ALTER ROLE grafana_reader PASSWORD :'grafana_password';
ALTER ROLE grafana_reader SET statement_timeout = '10s';
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS tracing;
REVOKE ALL ON SCHEMA tracing FROM PUBLIC;
GRANT USAGE ON SCHEMA tracing TO api_writer, grafana_reader;

-- Une version exacte de modèle chargée depuis MLflow (jamais un simple alias mutable).
CREATE TABLE IF NOT EXISTS tracing.model_versions (
    model_id        uuid PRIMARY KEY,
    tracking_uri    text NOT NULL CHECK (length(tracking_uri) > 0),
    registered_name text NOT NULL CHECK (length(registered_name) > 0),
    version         bigint NOT NULL CHECK (version > 0),
    mlflow_run_id   text NOT NULL,
    model_uri       text NOT NULL,
    qualification   text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tracking_uri, registered_name, version)
);

-- Une tentative HTTP sur un endpoint métier (les endpoints de santé ne sont pas tracés).
CREATE TABLE IF NOT EXISTS tracing.api_requests (
    request_id             uuid PRIMARY KEY,
    started_at             timestamptz NOT NULL,
    endpoint               text NOT NULL CHECK (endpoint IN ('/predict', '/predict/batch', '/feedback')),
    http_status            integer NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    error_code             text,
    n_items                integer CHECK (n_items >= 0),
    processing_duration_ms double precision NOT NULL CHECK (processing_duration_ms >= 0),
    inference_duration_ms  double precision CHECK (inference_duration_ms >= 0),
    worker_pid             integer NOT NULL,
    instance_id            uuid NOT NULL,
    api_revision           text NOT NULL,
    campaign_id            text NOT NULL DEFAULT 'manual' CHECK (length(campaign_id) BETWEEN 1 AND 64)
);
CREATE INDEX IF NOT EXISTS api_requests_time_idx ON tracing.api_requests (started_at);
CREATE INDEX IF NOT EXISTS api_requests_campaign_idx ON tracing.api_requests (campaign_id, started_at);

-- Un résultat de modèle pour une heure demandée (N lignes pour un lot de N éléments).
CREATE TABLE IF NOT EXISTS tracing.predictions (
    prediction_id uuid PRIMARY KEY,
    request_id    uuid NOT NULL REFERENCES tracing.api_requests (request_id),
    item_index    integer NOT NULL CHECK (item_index >= 0),
    model_id      uuid NOT NULL REFERENCES tracing.model_versions (model_id),
    features      jsonb NOT NULL,
    predicted_cnt double precision NOT NULL CHECK (predicted_cnt >= 0),
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (request_id, item_index)
);
CREATE INDEX IF NOT EXISTS predictions_time_idx ON tracing.predictions (created_at);
CREATE INDEX IF NOT EXISTS predictions_model_idx ON tracing.predictions (model_id);

-- Un retour immuable par prédiction : avis subjectif et/ou demande réellement observée.
CREATE TABLE IF NOT EXISTS tracing.feedbacks (
    feedback_id   uuid PRIMARY KEY,
    prediction_id uuid NOT NULL UNIQUE REFERENCES tracing.predictions (prediction_id),
    request_id    uuid NOT NULL REFERENCES tracing.api_requests (request_id),
    useful        boolean,
    observed_cnt  integer CHECK (observed_cnt >= 0),
    observed_at   timestamptz,
    label_source  text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CHECK (useful IS NOT NULL OR observed_cnt IS NOT NULL),
    CHECK ((observed_cnt IS NULL) = (observed_at IS NULL) AND (observed_cnt IS NULL) = (label_source IS NULL))
);
CREATE INDEX IF NOT EXISTS feedbacks_time_idx ON tracing.feedbacks (created_at);

-- Une exécution du pipeline d'entraînement (écrite par les hooks du flow Prefect, succès comme échec).
CREATE TABLE IF NOT EXISTS tracing.pipeline_runs (
    run_name        text PRIMARY KEY,
    flow_run_id     text,
    mlflow_run_id   text,
    trigger         text NOT NULL DEFAULT 'manual',
    status          text NOT NULL CHECK (status IN ('COMPLETED', 'FAILED', 'CRASHED', 'CANCELLED')),
    failed_step     text,
    error           text,
    started_at      timestamptz NOT NULL,
    ended_at        timestamptz NOT NULL,
    duration_s      double precision CHECK (duration_s >= 0),
    snapshot_rows   integer,
    batches         integer,
    test_mae        double precision,
    baseline_mae    double precision,
    model_name      text,
    model_version   text,
    qualification   text,
    promoted        boolean
);
CREATE INDEX IF NOT EXISTS pipeline_runs_time_idx ON tracing.pipeline_runs (started_at);

GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA tracing TO api_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA tracing TO grafana_reader;

COMMIT;
