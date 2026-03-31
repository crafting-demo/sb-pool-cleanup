#!/usr/bin/env python3

import json
import subprocess
import sys
import uuid
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pool-cleanup")

POOL_NAMES = [
    "demo-shop",
    "microservices",
]

DRY_RUN = "--dry" in sys.argv


def run_cs_command(args, capture_output=True):
    """Run a cs CLI command and return (success, output)."""
    cmd = ["cs"] + args
    log.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip() if capture_output else ""
            log.error("Command failed (rc=%d): %s", result.returncode, stderr)
            return False, stderr
        output = result.stdout.strip() if capture_output else ""
        return True, output
    except subprocess.TimeoutExpired:
        log.error("Command timed out: %s", " ".join(cmd))
        return False, "timeout"
    except Exception as e:
        log.error("Command error: %s", e)
        return False, str(e)


def parse_retention_seconds(retention_str):
    """Parse a protobuf duration string like '7200s' into seconds."""
    if not retention_str:
        return None
    return float(retention_str.rstrip("s"))


def get_pool_info(pool_name):
    """Fetch pool JSON and return parsed data."""
    success, output = run_cs_command(["sandbox", "pool", "show", pool_name, "-o", "json"])
    if not success:
        log.error("Failed to get pool info for '%s'", pool_name)
        return None
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        log.error("Failed to parse JSON for pool '%s': %s", pool_name, e)
        return None


def take_instance(pool_name, instance_name):
    """Take an instance from the pool, returning the new sandbox name."""
    new_name = f"cleanup-{uuid.uuid4().hex[:8]}"
    success, output = run_cs_command([
        "sandbox", "pool", "take", pool_name, instance_name,
        "--name", new_name,
        "--any",
        "--wait=false",
    ])
    if not success:
        log.error("Failed to take instance '%s' from pool '%s'", instance_name, pool_name)
        return None
    return new_name


def remove_sandbox(name):
    """Force-remove a sandbox."""
    success, output = run_cs_command(
        ["sandbox", "remove", name, "--force", "--ignore-lifecycle"],
        capture_output=False,
    )
    if not success:
        log.error("Failed to remove sandbox '%s'", name)
    return success


def process_pool(pool_name):
    """Check a single pool and clean up instances that exceed retention."""
    log.info("Processing pool: %s", pool_name)

    pool_data = get_pool_info(pool_name)
    if pool_data is None:
        return

    retention_str = pool_data.get("spec", {}).get("retention")
    retention_seconds = parse_retention_seconds(retention_str)
    if retention_seconds is None or retention_seconds <= 0:
        log.info("Pool '%s' has no retention set, skipping", pool_name)
        return

    instances = pool_data.get("instances", [])
    if not instances:
        log.info("Pool '%s' has no instances", pool_name)
        return

    now = datetime.now(timezone.utc)
    retention_delta = timedelta(seconds=retention_seconds)
    removed = 0

    log.info(
        "Pool '%s': retention=%s, instances=%d",
        pool_name, retention_str, len(instances),
    )

    for inst in instances:
        inst_name = inst.get("name", "unknown")
        created_at_str = inst.get("created_at")
        if not created_at_str:
            log.warning("Instance '%s' has no created_at, skipping", inst_name)
            continue

        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        age = now - created_at

        if age <= retention_delta:
            log.info(
                "  %s: age=%s, within retention, skipping",
                inst_name, age,
            )
            continue

        log.info(
            "  %s: age=%s, EXCEEDS retention of %s",
            inst_name, age, retention_str,
        )

        if DRY_RUN:
            log.info("  [DRY RUN] Would take and remove '%s'", inst_name)
            removed += 1
            continue

        new_name = take_instance(pool_name, inst_name)
        if new_name is None:
            continue

        if remove_sandbox(new_name):
            removed += 1
            log.info("  Removed '%s' (was '%s')", new_name, inst_name)

    log.info("Pool '%s': cleaned up %d instance(s)", pool_name, removed)


def main():
    if DRY_RUN:
        log.info("=== DRY RUN MODE — no changes will be made ===")

    log.info("Starting pool cleanup for %d pool(s)", len(POOL_NAMES))

    for pool_name in POOL_NAMES:
        try:
            process_pool(pool_name)
        except Exception:
            log.exception("Unexpected error processing pool '%s'", pool_name)

    log.info("Pool cleanup complete")


if __name__ == "__main__":
    main()
