"""Automation Prefect créée par SDK : alerte sur flow Failed/Crashed vers Teams (ou un webhook HTTP de repli).

Le secret n'est jamais versionné : l'URL est lue dans l'environnement.
    $env:TEAMS_WEBHOOK_URL = "https://...workflows..."      # PowerShell
    uv run python -m scripts.setup_alerting
    uv run python -m scripts.setup_alerting --test          # message « TEST » via le block
    uv run python -m scripts.setup_alerting --disable
Repli sans Teams : définir ALERT_WEBHOOK_URL (récepteur HTTP joignable depuis le conteneur Prefect).
"""
import argparse
import os

from prefect.automations import Automation
from prefect.blocks.notifications import CustomWebhookNotificationBlock, MicrosoftTeamsWebhook
from prefect.events.actions import SendNotification
from prefect.events.schemas.automations import EventTrigger, Posture

BLOCK_NAME = "bike-demand-alerts"
AUTOMATION_NAME = "bike-demand-flow-failures"
FLOWS = ["bike-demand-training", "bike-demand-watch-and-train"]

SUBJECT = "[bike-demand] Flow {{ flow.name }} : {{ flow_run.state.name }}"
BODY = """Flow : {{ flow.name }}
Flow run : {{ flow_run.name }} ({{ flow_run.id }})
État terminal : {{ flow_run.state.name }} — {{ flow_run.state.message }}
Date : {{ flow_run.state.timestamp }}
Environnement : poste local (UI http://localhost:4200/runs/flow-run/{{ flow_run.id }}, lien valide sur ce poste)
Tags (run MLflow, run, déclencheur) : {{ flow_run.tags | join(', ') if flow_run.tags else 'indisponible' }}
"""


def save_block():
    teams, generic = os.environ.get("TEAMS_WEBHOOK_URL"), os.environ.get("ALERT_WEBHOOK_URL")
    if teams:
        block = MicrosoftTeamsWebhook(url=teams)
    elif generic:
        block = CustomWebhookNotificationBlock(name="bike-demand", url=generic, method="POST",
                                               json_data={"title": "{{subject}}", "text": "{{body}}"})
    else:
        raise SystemExit("Définir TEAMS_WEBHOOK_URL (ou ALERT_WEBHOOK_URL pour le repli HTTP)")
    block.save(BLOCK_NAME, overwrite=True)
    return type(block).load(BLOCK_NAME)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--disable", action="store_true")
    args = parser.parse_args()

    if args.disable:
        Automation.read(name=AUTOMATION_NAME).disable()
        print(f"Automation {AUTOMATION_NAME} désactivée")
        return
    block = save_block()
    if args.test:
        block.notify("TEST — message de vérification du block bike-demand-alerts", subject="TEST")
        print("Message de test envoyé")
        return

    trigger = EventTrigger(
        expect={"prefect.flow-run.Failed", "prefect.flow-run.Crashed"},
        match_related={"prefect.resource.role": "flow", "prefect.resource.name": FLOWS},
        posture=Posture.Reactive, threshold=1,
    )
    action = SendNotification(block_document_id=block._block_document_id, subject=SUBJECT, body=BODY)
    try:
        existing = Automation.read(name=AUTOMATION_NAME)
        existing.trigger, existing.actions, existing.enabled = trigger, [action], True
        existing.update()
        print(f"Automation mise à jour : {existing.id}")
    except ValueError:
        created = Automation(name=AUTOMATION_NAME, description="Alerte sur échec terminal du pipeline bike-demand",
                             trigger=trigger, actions=[action]).create()
        print(f"Automation créée : {created.id}")


if __name__ == "__main__":
    main()
