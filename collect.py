"""Collect the local DemoMart dataset into a persistent SQLite ledger."""

import argparse
import json
from pathlib import Path

from collector import Ledger, Runner
from demomart import DemoMart, FixtureTransport, fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--stores", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--per-store-interval", type=float, default=0.075)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.stores < 1 or args.concurrency < 1 or args.per_store_interval < 0:
        parser.error("stores and concurrency must be positive and the interval nonnegative")

    adapter = DemoMart(FixtureTransport(fixture(args.stores)))
    stores = [f"store-{number:02}" for number in range(1, args.stores + 1)]
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with Ledger(args.db) as ledger:
        report = Runner(
            adapter, ledger, concurrency=args.concurrency,
            per_store_interval=args.per_store_interval,
        ).collect(stores, ("run-1", "run-2"), 40)
    serialized = json.dumps(report, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(serialized, encoding="utf-8")
    else:
        print(serialized, end="")


if __name__ == "__main__":
    main()
