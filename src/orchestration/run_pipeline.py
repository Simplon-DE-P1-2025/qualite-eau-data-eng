from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import yaml


STAGE_ORDER = ["bronze", "silver", "gold"]
OPTIONAL_STAGE_ORDER = ["quality"]
BRONZE_API_CHOICES = ["all", "geo_communes", "hubeau_communes", "hubeau_resultats"]


def resolve_project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError as exc:  # pragma: no cover
        raise RuntimeError("Impossible de resoudre la racine du projet.") from exc


PROJECT_ROOT = resolve_project_root()
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yml"


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


cfg = load_config()


def build_stage_paths() -> dict[str, Path]:
    return {
        "bronze": PROJECT_ROOT / "src" / "ingestion" / "bronze_ingestion.py",
        "silver": PROJECT_ROOT / "src" / "ingestion" / "silver_ingestion.py",
        "gold": PROJECT_ROOT / "src" / "ingestion" / "gold_ingestion.py",
        "quality": PROJECT_ROOT / "notebooks" / "quality" / "great_expectations_validation.py",
    }


STAGE_PATHS = build_stage_paths()


def resolve_logs_root() -> Path:
    local_logs = cfg["storage"]["local"]["logs"]
    preferred = (PROJECT_ROOT / local_logs).resolve()
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        probe = preferred / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return preferred
    except (PermissionError, OSError):
        fallback = PROJECT_ROOT / "logs"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback.resolve()


def compute_stage_sequence(
    from_stage: str,
    to_stage: str,
    with_quality: bool,
) -> list[str]:
    if from_stage not in STAGE_ORDER:
        raise ValueError(f"Etape de depart invalide: {from_stage}")
    if to_stage not in STAGE_ORDER:
        raise ValueError(f"Etape de fin invalide: {to_stage}")

    start_idx = STAGE_ORDER.index(from_stage)
    end_idx = STAGE_ORDER.index(to_stage)
    if start_idx > end_idx:
        raise ValueError(
            f"Ordre invalide: from_stage={from_stage} arrive apres to_stage={to_stage}"
        )

    stages = STAGE_ORDER[start_idx : end_idx + 1]
    if with_quality and "silver" in stages:
        silver_idx = stages.index("silver")
        stages.insert(silver_idx + 1, "quality")
    return stages


def format_duration(seconds: float) -> str:
    return f"{seconds:.1f}s"


def run_stage(
    stage_name: str,
    stage_script: Path,
    run_dir: Path,
    python_executable: str,
    dry_run: bool,
    extra_args: list[str] | None = None,
) -> dict:
    started_at = dt.datetime.utcnow()
    log_path = run_dir / f"{stage_name}.log"
    extra_args = extra_args or []

    stage_result = {
        "stage": stage_name,
        "script": str(stage_script),
        "command": [python_executable, "-u", str(stage_script), *extra_args],
        "started_at_utc": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pending",
        "return_code": None,
        "duration_seconds": 0.0,
        "log_path": str(log_path),
    }

    if dry_run:
        stage_result["status"] = "dry_run"
        return stage_result

    start_monotonic = time.perf_counter()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    with open(log_path, "w", encoding="utf-8") as log_stream:
        log_stream.write(f"stage={stage_name}\n")
        log_stream.write(f"script={stage_script}\n")
        log_stream.write(f"started_at_utc={stage_result['started_at_utc']}\n\n")
        log_stream.flush()

        process = subprocess.Popen(
            [python_executable, "-u", str(stage_script), *extra_args],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )

        assert process.stdout is not None
        for line in process.stdout:
            print(f"   | {line.rstrip()}", flush=True)
            log_stream.write(line)
            log_stream.flush()

        return_code = process.wait()
        duration = time.perf_counter() - start_monotonic
        log_stream.write(f"\nreturn_code={return_code}\n")
        log_stream.write(f"duration_seconds={round(duration, 3)}\n")

    stage_result["return_code"] = return_code
    stage_result["duration_seconds"] = round(duration, 3)
    stage_result["status"] = "success" if return_code == 0 else "failed"
    return stage_result


def write_run_summary(run_dir: Path, summary: dict) -> None:
    json_path = run_dir / "run_summary.json"
    md_path = run_dir / "run_summary.md"

    json_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    lines = [
        "# Run Summary",
        "",
        f"- environment: {summary['environment']}",
        f"- from_stage: {summary['from_stage']}",
        f"- to_stage: {summary['to_stage']}",
        f"- with_quality: {summary['with_quality']}",
        f"- dry_run: {summary['dry_run']}",
        f"- started_at_utc: {summary['started_at_utc']}",
        f"- finished_at_utc: {summary['finished_at_utc']}",
        f"- status: {summary['status']}",
        "",
        "| stage | status | duration_seconds | return_code | log |",
        "|---|---:|---:|---:|---|",
    ]

    for item in summary["stages"]:
        log_name = Path(item["log_path"]).name
        lines.append(
            f"| {item['stage']} | {item['status']} | "
            f"{item['duration_seconds']} | {item['return_code']} | {log_name} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestration locale du pipeline Bronze -> Silver -> Gold."
    )
    parser.add_argument(
        "--from-stage",
        choices=STAGE_ORDER,
        default="bronze",
        help="Etape de depart.",
    )
    parser.add_argument(
        "--to-stage",
        choices=STAGE_ORDER,
        default="gold",
        help="Etape de fin.",
    )
    parser.add_argument(
        "--with-quality",
        action="store_true",
        help="Insere la validation Great Expectations juste apres Silver.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche la sequence sans executer les scripts.",
    )
    parser.add_argument(
        "--bronze-api",
        choices=BRONZE_API_CHOICES,
        default="all",
        help="Sous-mode Bronze: limite l'ingestion a une API specifique.",
    )
    return parser.parse_args()


def build_stage_args(args: argparse.Namespace) -> dict[str, list[str]]:
    stage_args: dict[str, list[str]] = {}
    if args.bronze_api != "all":
        stage_args["bronze"] = ["--api", args.bronze_api]
    return stage_args


def main() -> int:
    args = parse_args()
    stage_args = build_stage_args(args)
    logs_root = resolve_logs_root() / "orchestration"
    run_ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = logs_root / run_ts
    run_dir.mkdir(parents=True, exist_ok=True)

    stages_to_run = compute_stage_sequence(
        from_stage=args.from_stage,
        to_stage=args.to_stage,
        with_quality=args.with_quality,
    )

    print("=" * 72)
    print("ORCHESTRATION PIPELINE QUALITE DE L'EAU")
    print("=" * 72)
    print(f"Environment : {cfg['environment']}")
    print(f"Sequence    : {' -> '.join(stages_to_run)}")
    if args.bronze_api != "all":
        print(f"Bronze API  : {args.bronze_api}")
    print(f"Logs        : {run_dir}")
    if args.dry_run:
        print("Mode        : dry-run")
    print("=" * 72)

    stage_results = []
    overall_status = "success"

    for stage_name in stages_to_run:
        stage_script = STAGE_PATHS[stage_name]
        stage_cli_args = stage_args.get(stage_name, [])
        suffix = f" {' '.join(stage_cli_args)}" if stage_cli_args else ""
        print(f"\n[{stage_name.upper()}] {stage_script}{suffix}")
        result = run_stage(
            stage_name=stage_name,
            stage_script=stage_script,
            run_dir=run_dir,
            python_executable=sys.executable,
            dry_run=args.dry_run,
            extra_args=stage_cli_args,
        )
        stage_results.append(result)

        print(
            f"   status={result['status']} "
            f"duration={format_duration(result['duration_seconds'])} "
            f"log={Path(result['log_path']).name}"
        )

        if result["status"] == "failed":
            overall_status = "failed"
            break

    finished_at = dt.datetime.utcnow()
    summary = {
        "environment": cfg["environment"],
        "from_stage": args.from_stage,
        "to_stage": args.to_stage,
        "with_quality": args.with_quality,
        "dry_run": args.dry_run,
        "started_at_utc": run_ts,
        "finished_at_utc": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        "status": overall_status if not args.dry_run else "dry_run",
        "stages": stage_results,
    }
    write_run_summary(run_dir, summary)

    print("\n" + "=" * 72)
    print(f"STATUT FINAL : {summary['status']}")
    print(f"SUMMARY      : {run_dir / 'run_summary.md'}")
    print("=" * 72)

    if overall_status == "failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
