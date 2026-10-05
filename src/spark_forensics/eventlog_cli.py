from __future__ import annotations

import argparse
import json
from pathlib import Path

from .diagnose import DiagnosisConfig, diagnose_stage
from .eventlog import parse_event_log


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect a Spark event log for stage-level bottlenecks.")
    parser.add_argument("event_log", type=Path)
    parser.add_argument("--stage-id", type=int, help="Analyze one stage instead of every parsed stage.")
    parser.add_argument("--max-to-median", type=float, default=3.0)
    parser.add_argument("--shuffle-vs-uniform", type=float, default=2.5)
    parser.add_argument("--records-vs-uniform", type=float, default=2.5)
    parser.add_argument("--spill-share", type=float, default=0.10)
    parser.add_argument("--gc-share", type=float, default=0.20)
    args = parser.parse_args()

    config = DiagnosisConfig(
        max_to_median_ratio=args.max_to_median,
        shuffle_vs_uniform_ratio=args.shuffle_vs_uniform,
        records_vs_uniform_ratio=args.records_vs_uniform,
        spill_share_threshold=args.spill_share,
        gc_share_threshold=args.gc_share,
    )

    stages = parse_event_log(args.event_log)
    if args.stage_id is not None:
        stages = tuple(stage for stage in stages if stage.stage_id == args.stage_id)

    if not stages:
        raise SystemExit("No stage data was found for the requested stage(s).")

    for stage in stages:
        print(f"Stage {stage.stage_id}: {stage.name} [{stage.status}] ({len(stage.tasks)} tasks)")
        diagnoses = diagnose_stage(stage, config)
        if not diagnoses:
            print("  No diagnosis from the configured signals.")
            continue

        for diagnosis in diagnoses:
            print(f"  {diagnosis.code} [rank {diagnosis.rank}]")
            print(f"    {diagnosis.explanation}")
            print("    " + json.dumps(diagnosis.evidence, sort_keys=True))


if __name__ == "__main__":
    main()
