import argparse
import json
import signal
import time

from workloads.cybermarket.config import Settings
from workloads.cybermarket.ingestion.writer import LandingWriter


def run(follow=True, idle_exit_seconds=None, ready_event=None):
    settings = Settings.from_env()
    writer = LandingWriter(settings)
    stopping = False
    total = 0
    idle_since = time.monotonic()

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        writer.ensure_slot()
        if ready_event:
            ready_event.set()
        while not stopping:
            rows = writer.peek()
            if rows:
                total += writer.land_batch(rows)
                idle_since = time.monotonic()
                continue
            if not follow:
                break
            if (
                idle_exit_seconds is not None
                and time.monotonic() - idle_since >= idle_exit_seconds
            ):
                break
            time.sleep(settings.cdc_poll_interval)
    finally:
        writer.close()
    print(
        json.dumps({"landed_changes": total, "slot": settings.cdc_slot}, sort_keys=True)
    )
    return total


def main():
    parser = argparse.ArgumentParser(
        description="Land PostgreSQL WAL changes as immutable raw JSONL"
    )
    parser.add_argument(
        "--drain", action="store_true", help="Land available changes and exit"
    )
    parser.add_argument(
        "--idle-exit-seconds", type=float, help="Exit after the slot stays idle"
    )
    args = parser.parse_args()
    run(follow=not args.drain, idle_exit_seconds=args.idle_exit_seconds)


if __name__ == "__main__":
    main()
