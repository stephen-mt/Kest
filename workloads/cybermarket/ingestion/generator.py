import argparse
import json
import signal
import time
from collections import Counter

from workloads.cybermarket.config import Settings
from workloads.cybermarket.source.live import LiveEventWriter

EVENT_FRAME = (
    "purchase",
    "buyer_session",
    "payment_update",
    "buyer_session",
    "risk_prediction",
    "purchase",
    "buyer_session",
    "transaction_status_update",
    "buyer_session",
    "purchase",
    "payment_update",
    "buyer_session",
    "purchase",
    "buyer_session",
    "risk_prediction",
    "buyer_session",
    "purchase",
    "payment_update",
    "buyer_session",
    "purchase",
)


def run(duration=None, ready_event=None):
    settings = Settings.from_env()
    writer = LiveEventWriter(settings)
    counts = Counter()
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    deadline = started + duration if duration else None
    sequence = 0
    if ready_event:
        ready_event.set()
    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            event = EVENT_FRAME[sequence % len(EVENT_FRAME)]
            writer.emit(event)
            counts[event] += 1
            sequence += 1
            target = started + sequence / settings.event_rate
            time.sleep(max(0, target - time.monotonic()))
    finally:
        writer.close()
    print(json.dumps(dict(sorted(counts.items())), sort_keys=True))
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Run the fixed 20 events/sec CyberMarket workload"
    )
    parser.add_argument(
        "--duration", type=float, help="Seconds to run; default runs until stopped"
    )
    args = parser.parse_args()
    run(args.duration)


if __name__ == "__main__":
    main()
