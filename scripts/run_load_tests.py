"""Campagne de dimensionnement : workers Uvicorn × concurrence, même protocole pour chaque configuration.

    uv run python -m scripts.run_load_tests                                   # protocole complet (~25 min)
    uv run python -m scripts.run_load_tests --workers 1 2 --users 5 10 --duration 30 --repeats 1   # exploratoire

Pour chaque palier : nouveau CAMPAIGN_ID, CSV Locust, CPU/RAM de l'API mesurés pendant le tir.
Résultats : resultats/<date>/summary.csv et summary.md. Objectif de service (défini AVANT les mesures) :
p95 < 300 ms et < 1 % d'échecs pour /predict ; p95 < 800 ms pour /predict/batch[20].
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import httpx
import psutil

ROOT = Path(__file__).resolve().parents[1]
SLO = {"/predict": 300.0, "/predict/batch[20]": 800.0, "/feedback": 300.0}
MAX_ERROR_RATE = 0.01


def start_api(workers: int, port: int) -> subprocess.Popen:
    env = {**os.environ, "OMP_NUM_THREADS": "1"}
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "api_prediction.main:app", "--host", "127.0.0.1",
                             "--port", str(port), "--workers", str(workers)], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health/ready", timeout=2).status_code == 200:
                time.sleep(2 * workers)  # laisser tous les workers terminer leur chargement
                return proc
        except httpx.HTTPError:
            pass
        time.sleep(1)
    stop_api(proc)
    raise RuntimeError("API non prête")


def stop_api(proc: subprocess.Popen) -> None:
    try:
        parent = psutil.Process(proc.pid)
        for child in parent.children(recursive=True):
            child.kill()
        parent.kill()
    except psutil.NoSuchProcess:
        pass
    proc.wait(timeout=30)


class ResourceSampler(threading.Thread):
    """CPU (somme des processus API, en % d'un cœur) et RAM RSS pendant le tir."""
    def __init__(self, pid: int):
        super().__init__(daemon=True)
        self.pid, self.samples, self.stop = pid, [], threading.Event()

    def run(self):
        root = psutil.Process(self.pid)
        procs = [root, *root.children(recursive=True)]
        for p in procs:
            p.cpu_percent(None)
        while not self.stop.wait(1.0):
            cpu = rss = 0.0
            for p in procs:
                try:
                    cpu += p.cpu_percent(None)
                    rss += p.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
            self.samples.append((cpu, rss / 2**20, psutil.cpu_percent(None)))


def run_locust(scenario, users, duration, port, out_prefix: Path, campaign: str) -> list[dict]:
    env = {**os.environ, "LOCUST_SCENARIO": scenario, "CAMPAIGN_ID": campaign, "BATCH_SIZE": "20"}
    subprocess.run([sys.executable, "-m", "locust", "-f", "locustfile.py", "--host", f"http://127.0.0.1:{port}",
                    "--headless", "-u", str(users), "-r", str(max(1, users // 2)), "-t", f"{duration}s",
                    "--csv", str(out_prefix), "--only-summary"], cwd=ROOT, env=env, check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with open(f"{out_prefix}_stats.csv", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--users", type=int, nargs="+", default=[5, 10, 20, 40, 80, 160])
    parser.add_argument("--scenarios", nargs="+", default=["unit", "batch"])
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()

    out_dir = ROOT / "resultats" / datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for workers in args.workers:
        proc = start_api(workers, args.port)
        try:
            run_locust("unit", 2, 10, args.port, out_dir / f"warmup-w{workers}", f"warmup-w{workers}")  # chauffe
            for scenario in args.scenarios:
                for users in args.users:
                    for rep in range(1, args.repeats + 1):
                        campaign = f"lt-w{workers}-{scenario}-u{users}-r{rep}-{out_dir.name}"
                        sampler = ResourceSampler(proc.pid)
                        sampler.start()
                        stats = run_locust(scenario, users, args.duration, args.port,
                                           out_dir / f"w{workers}-{scenario}-u{users}-r{rep}", campaign)
                        sampler.stop.set(); sampler.join()
                        cpu = [s[0] for s in sampler.samples] or [0]
                        ram = [s[1] for s in sampler.samples] or [0]
                        host = [s[2] for s in sampler.samples] or [0]
                        for s in stats:
                            if s["Name"] == "Aggregated" or not s["Request Count"] or s["Request Count"] == "0":
                                continue
                            n, fails = int(s["Request Count"]), int(s["Failure Count"])
                            items = 20 if "batch" in s["Name"] else 1
                            p95 = float(s["95%"])
                            row = {"workers": workers, "scenario": scenario, "users": users, "repeat": rep,
                                   "endpoint": s["Name"], "requests": n, "rps": round(float(s["Requests/s"]), 2),
                                   "items_per_s": round(float(s["Requests/s"]) * items, 1),
                                   "p50_ms": float(s["50%"]), "p95_ms": p95, "p99_ms": float(s["99%"]),
                                   "error_rate": round(fails / n, 4) if n else None,
                                   "api_cpu_pct_mean": round(sum(cpu) / len(cpu), 1),
                                   "host_cpu_pct_mean": round(sum(host) / len(host), 1),
                                   "api_rss_mb_max": round(max(ram), 1), "campaign": campaign}
                            row["slo_ok"] = p95 <= SLO.get(s["Name"], 800.0) and (row["error_rate"] or 0) <= MAX_ERROR_RATE
                            rows.append(row)
                            print(f"w={workers} {scenario:5s} u={users:3d} r{rep} {s['Name']:20s} "
                                  f"{row['rps']:7.1f} req/s p95={p95:6.0f} ms err={row['error_rate']:.2%} "
                                  f"cpu={row['api_cpu_pct_mean']}% rss={row['api_rss_mb_max']} MB "
                                  f"{'OK' if row['slo_ok'] else 'KO'}", flush=True)
        finally:
            stop_api(proc)

    with open(out_dir / "summary.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    lines = ["| workers | scénario | users | endpoint | req/s | heures/s | p50 | p95 | p99 | erreurs | CPU API | RAM API | SLO |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    lines += [f"| {r['workers']} | {r['scenario']} | {r['users']} | {r['endpoint']} | {r['rps']} | {r['items_per_s']} | "
              f"{r['p50_ms']:.0f} | {r['p95_ms']:.0f} | {r['p99_ms']:.0f} | {r['error_rate']:.2%} | "
              f"{r['api_cpu_pct_mean']} % | {r['api_rss_mb_max']} Mo | {'✅' if r['slo_ok'] else '❌'} |" for r in rows]
    (out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Résultats : {out_dir}")


if __name__ == "__main__":
    main()
