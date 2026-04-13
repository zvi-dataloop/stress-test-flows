#!/usr/bin/env python3
"""
List all pipeline executions for a specific node service and find executions
with much higher duration than the rest.

Uses Dataloop RC environment.
Pipeline: 698c7fa7934c52c13c80da43
Service (node): 698c7faa934c52c13c80da45

Run:
  export DTLPY_ENV=rc   # or use dl.setenv('rc') in script
  python list_pipeline_node_executions.py

Optional env: PROJECT_ID (if pipeline get fails without it).
"""

import os
import sys
import argparse
import statistics

try:
    import dtlpy as dl
except ImportError:
    print("Install dtlpy: pip install dtlpy", file=sys.stderr)
    sys.exit(1)

# Defaults from user request (RC env)
DEFAULT_PIPELINE_ID = "698c7fa7934c52c13c80da43"
DEFAULT_SERVICE_ID = "698c7faa934c52c13c80da45"
DEFAULT_ENV = "rc"

# Outlier: consider "much higher" if duration > median + (multiplier * IQR) or > percentile_threshold
OUTLIER_IQR_MULTIPLIER = 2.0
OUTLIER_PERCENTILE = 95  # also flag if above 95th percentile


def _get_duration_seconds(execution):
    """Extract duration in seconds from an execution (SDK entity or dict)."""
    # Prefer explicit duration field (API returns in ms sometimes)
    raw = None
    if hasattr(execution, "duration"):
        raw = getattr(execution, "duration", None)
    if raw is None and hasattr(execution, "to_json"):
        j = execution.to_json()
        raw = j.get("duration") or j.get("durationSeconds")
    if isinstance(execution, dict):
        raw = execution.get("duration") or execution.get("durationSeconds")
    if raw is not None:
        # If value is in milliseconds (e.g. > 1e6 for long runs), treat as ms
        if isinstance(raw, (int, float)):
            if raw > 1e6:
                return raw / 1000.0
            return float(raw)
    # Fallback: duration from createdAt -> updatedAt
    created = None
    updated = None
    if hasattr(execution, "created_at"):
        created = getattr(execution, "created_at", None)
    if hasattr(execution, "updated_at"):
        updated = getattr(execution, "updated_at", None)
    if hasattr(execution, "createdAt"):
        created = getattr(execution, "createdAt", None)
    if hasattr(execution, "updatedAt"):
        updated = getattr(execution, "updatedAt", None)
    if hasattr(execution, "to_json"):
        j = execution.to_json()
        created = created or j.get("createdAt") or j.get("created_at")
        updated = updated or j.get("updatedAt") or j.get("updated_at")
    if isinstance(execution, dict):
        created = created or execution.get("createdAt") or execution.get("created_at")
        updated = updated or execution.get("updatedAt") or execution.get("updated_at")
    if created is not None and updated is not None:
        try:
            if hasattr(created, "timestamp"):
                t0 = created.timestamp()
            elif isinstance(created, (int, float)):
                t0 = float(created)
            else:
                from datetime import datetime
                t0 = datetime.fromisoformat(str(created).replace("Z", "+00:00")).timestamp()
            if hasattr(updated, "timestamp"):
                t1 = updated.timestamp()
            elif isinstance(updated, (int, float)):
                t1 = float(updated)
            else:
                from datetime import datetime
                t1 = datetime.fromisoformat(str(updated).replace("Z", "+00:00")).timestamp()
            return max(0, t1 - t0)
        except Exception:
            pass
    return None


def main():
    parser = argparse.ArgumentParser(description="List pipeline node executions and find high-duration outliers")
    parser.add_argument("--pipeline-id", default=DEFAULT_PIPELINE_ID, help="Pipeline ID")
    parser.add_argument("--service-id", default=DEFAULT_SERVICE_ID, help="Node/Service ID")
    parser.add_argument("--env", default=DEFAULT_ENV, help="Dataloop environment (e.g. rc, prod)")
    parser.add_argument("--project-id", default=os.environ.get("PROJECT_ID") or os.environ.get("DL_PROJECT_ID"), help="Project ID (optional if pipeline is accessible)")
    parser.add_argument("--page-size", type=int, default=100, help="Page size for listing executions")
    parser.add_argument("--max-pages", type=int, default=50, help="Max pages to fetch (0 = no limit)")
    parser.add_argument("--outlier-pct", type=float, default=OUTLIER_PERCENTILE, help="Flag executions above this percentile as high duration")
    args = parser.parse_args()

    pipeline_id = args.pipeline_id.strip()
    service_id = args.service_id.strip()

    print("Dataloop pipeline node executions – list and duration analysis")
    print("=" * 60)
    print(f"  Environment: {args.env}")
    print(f"  Pipeline ID: {pipeline_id}")
    print(f"  Service ID:  {service_id}")
    print("=" * 60)

    # Set environment and ensure login
    dl.setenv(args.env)
    if dl.token_expired():
        dl.login()
    print("  Logged in to Dataloop\n")

    # Resolve project
    project = None
    try:
        pipeline = dl.pipelines.get(pipeline_id=pipeline_id)
        project = pipeline.project
        print(f"  Pipeline: {getattr(pipeline, 'name', pipeline_id)}")
        print(f"  Project:  {project.name} ({project.id})\n")
    except Exception as e:
        if args.project_id:
            project = dl.projects.get(project_id=args.project_id)
            print(f"  Project: {project.name} ({project.id}) (from --project-id)")
            print(f"  (Pipeline get failed: {e})\n")
        else:
            print(f"  Error: Could not get pipeline or project: {e}", file=sys.stderr)
            print("  Tip: Set PROJECT_ID if pipeline get fails.\n", file=sys.stderr)
            sys.exit(1)

    # List executions for this pipeline and service (node)
    filters = dl.Filters(resource=dl.FiltersResource.EXECUTION)
    #filters.add(field="pipeline.id", values=pipeline_id)
    filters.add(field="serviceId", values=service_id)
    #filters.sort_by(field="createdAt", value=dl.FiltersOrderByDirection.DESCENDING)

    executions_with_duration = []
    total_fetched = 0

    print("Fetching executions...")
    try:
        # Use .all() if available to iterate all pages; otherwise single page
        list_result = project.executions.list(
            filters=filters
        )
        if hasattr(list_result, "all"):
            items = list(list_result.all())
        else:
            items = list_result.items if hasattr(list_result, "items") else []
        for execution in items:
            total_fetched += 1
            if args.max_pages and total_fetched > args.max_pages * args.page_size:
                break
            duration_sec = _get_duration_seconds(execution)
            exec_id = getattr(execution, "id", None) or (execution.get("id") if isinstance(execution, dict) else None)
            status = getattr(execution, "status", None) or (execution.get("status") if isinstance(execution, dict) else "?")
            executions_with_duration.append({
                "id": exec_id,
                "status": status,
                "duration_seconds": duration_sec,
                "raw": execution,
            })
    except Exception as e:
        print(f"  Error listing executions: {e}", file=sys.stderr)

    print(f"  Total executions fetched: {total_fetched}\n")

    # Only consider executions with a numeric duration
    with_duration = [e for e in executions_with_duration if e["duration_seconds"] is not None]
    without_duration = [e for e in executions_with_duration if e["duration_seconds"] is None]

    if not with_duration:
        print("No executions with duration field found.")
        if without_duration:
            print(f"  ({len(without_duration)} executions had no duration/createdAt/updatedAt.)")
        return

    durations = [e["duration_seconds"] for e in with_duration]
    n = len(durations)
    mean_sec = statistics.mean(durations)
    median_sec = statistics.median(durations)
    try:
        stdev_sec = statistics.stdev(durations)
    except statistics.StatisticsError:
        stdev_sec = 0.0
    try:
        q75 = statistics.quantiles(durations, n=4)[2]  # 75th percentile
        q95 = statistics.quantiles(durations, n=20)[18]  # 95th percentile
    except Exception:
        sorted_d = sorted(durations)
        q75 = sorted_d[int(0.75 * (n - 1))] if n else 0
        q95 = sorted_d[int(0.95 * (n - 1))] if n else 0
    try:
        q1, q3 = statistics.quantiles(durations, n=4)[0], statistics.quantiles(durations, n=4)[2]
        iqr = q3 - q1
        outlier_threshold_iqr = median_sec + OUTLIER_IQR_MULTIPLIER * iqr
    except Exception:
        iqr = 0
        outlier_threshold_iqr = median_sec + 2 * stdev_sec if stdev_sec else float("inf")

    print("Duration statistics (seconds)")
    print("-" * 40)
    print(f"  Count (with duration): {n}")
    print(f"  Mean:   {mean_sec:.2f}s")
    print(f"  Median: {median_sec:.2f}s")
    print(f"  Stdev:  {stdev_sec:.2f}s")
    print(f"  75th %: {q75:.2f}s")
    print(f"  95th %: {q95:.2f}s")
    print(f"  IQR:    {iqr:.2f}s")
    print(f"  Outlier threshold (median + {OUTLIER_IQR_MULTIPLIER}*IQR): {outlier_threshold_iqr:.2f}s")
    print()

    # Flag high-duration executions
    high_duration = []
    for e in with_duration:
        d = e["duration_seconds"]
        if d >= outlier_threshold_iqr or d >= q95:
            high_duration.append(e)

    # Sort by duration descending for listing
    with_duration_sorted = sorted(with_duration, key=lambda x: (x["duration_seconds"] or 0), reverse=True)

    print("All executions (by duration, descending)")
    print("-" * 60)
    for e in with_duration_sorted[:50]:  # Top 50 by duration
        d = e["duration_seconds"]
        mark = "  << HIGH" if e in high_duration else ""
        print(f"  {e['id']}  duration={d:.2f}s  status={e['status']}{mark}")
    if len(with_duration_sorted) > 50:
        print(f"  ... and {len(with_duration_sorted) - 50} more")
    print()

    if high_duration:
        print("Executions with MUCH HIGHER duration (outliers)")
        print("-" * 60)
        for e in sorted(high_duration, key=lambda x: x["duration_seconds"], reverse=True):
            print(f"  id:       {e['id']}")
            print(f"  duration: {e['duration_seconds']:.2f}s")
            print(f"  status:   {e['status']}")
            print()
    else:
        print("No executions flagged as unusually high duration (within IQR/percentile thresholds).")

    if without_duration:
        print(f"\nNote: {len(without_duration)} executions had no duration info (e.g. still running or API field missing).")


if __name__ == "__main__":
    main()
