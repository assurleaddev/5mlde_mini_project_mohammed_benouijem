"""Scénarios de charge : chaque réponse est vérifiée (un 200 au corps invalide compte comme un échec).

Variables d'environnement :
    LOCUST_SCENARIO = unit | batch | mixed     (défaut mixed)
    BATCH_SIZE      = taille des lots          (défaut 20)
    CAMPAIGN_ID     = identifiant transmis dans X-Test-Campaign (défaut loadtest-<horodatage>)

    uv run --locked locust -f locustfile.py --host http://127.0.0.1:8001 --headless -u 20 -r 2 -t 2m --csv resultats/w1-u20
"""
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from locust import HttpUser, between, events, task

from src.schema import FEATURES

SCENARIO = os.environ.get("LOCUST_SCENARIO", "mixed")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
CAMPAIGN = os.environ.get("CAMPAIGN_ID", f"loadtest-{int(time.time())}")[:64]

# Données client construites à l'avance à partir de métadonnées valides : aucun appel externe pendant le tir.
_raw = Path(__file__).resolve().parent / "data" / "raw" / "hour.csv"
if _raw.exists():
    HOURS = pd.read_csv(_raw)[FEATURES].sample(2000, random_state=7).to_dict("records")
else:
    HOURS = [{"season": 3, "yr": 1, "mnth": 7, "hr": h, "holiday": 0, "weekday": 3, "workingday": 1, "weathersit": 1,
              "temp": 0.7, "atemp": 0.65, "hum": 0.5, "windspeed": 0.2} for h in range(24)]


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--campaign", default=CAMPAIGN, help="Identifiant de campagne")


def valid_prediction(p) -> bool:
    return (isinstance(p.get("predicted_cnt"), (int, float)) and p["predicted_cnt"] >= 0
            and p.get("prediction_id") and p.get("model", {}).get("version"))


class BikeApiUser(HttpUser):
    wait_time = between(0.5, 1.5)

    def on_start(self):
        self.headers = {"X-Test-Campaign": self.environment.parsed_options.campaign
                        if self.environment.parsed_options else CAMPAIGN}
        self.pending_feedback = []

    def _unit(self):
        with self.client.post("/predict", json={"hour": random.choice(HOURS)}, headers=self.headers,
                              name="/predict", catch_response=True) as r:
            body = r.json() if r.status_code == 200 else {}
            if r.status_code != 200 or not valid_prediction(body.get("prediction", {})):
                r.failure(f"réponse invalide {r.status_code}")
            else:
                self.pending_feedback.append(body["prediction"])

    def _batch(self):
        hours = random.sample(HOURS, BATCH_SIZE)
        with self.client.post("/predict/batch", json={"hours": hours}, headers=self.headers,
                              name=f"/predict/batch[{BATCH_SIZE}]", catch_response=True) as r:
            preds = r.json().get("predictions", []) if r.status_code == 200 else []
            if (r.status_code != 200 or len(preds) != BATCH_SIZE or not all(map(valid_prediction, preds))
                    or [p["item_index"] for p in preds] != list(range(BATCH_SIZE))):
                r.failure(f"lot invalide {r.status_code}")

    def _feedback(self):
        if not self.pending_feedback:
            return
        prediction = self.pending_feedback.pop()  # au plus un feedback par prédiction
        body = {"feedback_id": str(uuid.uuid4()), "prediction_id": prediction["prediction_id"],
                "useful": random.random() < 0.7, "observed_cnt": max(0, int(prediction["predicted_cnt"]
                                                                          * random.uniform(0.7, 1.3))),
                "observed_at": datetime.now(timezone.utc).isoformat(), "label_source": "loadtest"}
        with self.client.post("/feedback", json=body, headers=self.headers, name="/feedback",
                              catch_response=True) as r:
            if r.status_code != 201 or r.json().get("status") != "created":
                r.failure(f"feedback non enregistré {r.status_code}")

    @task
    def scenario(self):
        if SCENARIO == "unit":
            self._unit()
        elif SCENARIO == "batch":
            self._batch()
        else:  # mélange réaliste : 70 % unitaire, 20 % lot, 10 % feedback
            roll = random.random()
            self._unit() if roll < 0.7 else self._batch() if roll < 0.9 else self._feedback()
