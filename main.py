"""Orchestration uniquement : le flow Prefect chaîne les quatre tâches mères, la logique métier est dans src/."""
from pathlib import Path

from prefect import flow

from src.ingestion import load_data
from src.orchestration import on_flow_completion, on_flow_failure
from src.preprocessing import preprocess_data
from src.training import train_models
from src.evaluation import evaluate_models


@flow(name="bike-demand-training", retries=0, log_prints=True, timeout_seconds=3600,
      on_failure=[on_flow_failure], on_crashed=[on_flow_failure], on_cancellation=[on_flow_failure],
      on_completion=[on_flow_completion])
def main(config_path: str | Path):
    data = load_data(config_path)
    prepared_data = preprocess_data(data)
    trained_models = train_models(prepared_data)
    return evaluate_models(trained_models)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Entraîner et évaluer le modèle de demande de vélos")
    parser.add_argument("--config", required=True, help="Chemin du fichier de configuration JSON")
    print(main(parser.parse_args().config))
