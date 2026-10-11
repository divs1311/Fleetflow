"""Command line entry point (no Airflow needed).

    python -m porterflow.cli run --start 2026-09-01 --end 2026-09-07   # full pipeline, day by day
    python -m porterflow.cli task dbt_build --run-date 2026-09-03       # one task
    python -m porterflow.cli simulate --day 2026-09-03                   # inspect the source's ground truth
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date

from . import tasks
from .config import load_settings


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="porterflow")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run the whole pipeline for a date range")
    r.add_argument("--start", type=date.fromisoformat, required=True)
    r.add_argument("--end", type=date.fromisoformat, required=True)
    r.add_argument("--skip", nargs="*", default=[], choices=[n for n, _ in tasks.PIPELINE])

    t = sub.add_parser("task", help="run one task for one date")
    t.add_argument("name", choices=[n for n, _ in tasks.PIPELINE])
    t.add_argument("--run-date", type=date.fromisoformat, required=True)
    t.add_argument("--full-refresh", action="store_true", help="dbt_build only")

    s = sub.add_parser("simulate", help="simulate one day and print the injected-defect ground truth")
    s.add_argument("--day", type=date.fromisoformat, required=True)

    a = p.parse_args(argv)
    settings = load_settings()
    if a.cmd == "run":
        tasks.run_range(a.start, a.end, settings, set(a.skip))
    elif a.cmd == "task":
        fn = dict(tasks.PIPELINE)[a.name]
        result = fn(settings, a.run_date, full_refresh=True) if a.name == "dbt_build" and a.full_refresh \
            else fn(settings, a.run_date)
        if result is not None:
            print(json.dumps(result, indent=2, default=str))
    elif a.cmd == "simulate":
        from .simulator import SimulatorSource
        src = SimulatorSource(settings.state_dir / "sim_cache", settings.sim_seed, settings.sim_trips_per_day)
        print(json.dumps(src.ground_truth(src.day_frames(a.day), a.day), indent=2))


if __name__ == "__main__":
    main()
