"""Verify the track and produce reports using only the Python standard library."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import tempfile

from collector import Ledger, Runner
from demomart import DemoMart, FixtureTransport, fixture


TARGET = ("store-01", "run-1", "page-2")
RECOVERY = {TARGET: [("429", 0.12), ("503", 0.08)]}
RUNS = ("run-1", "run-2")


class VirtualClock:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.delays.append(round(seconds, 8))
        self.now += seconds


def runner(db, pages, *, faults=None, concurrency=1, clock=None, max_pages=20):
    adapter = DemoMart(FixtureTransport(pages, faults=faults, clock=clock))
    return Runner(adapter, db, clock=clock, concurrency=concurrency, max_pages=max_pages)


def stores(count):
    return [f"store-{number:02}" for number in range(1, count + 1)]


def check_counts(report, accepted, duplicates, rejected, attempts, pages):
    fields = ("accepted", "duplicates", "rejected", "request_attempts", "pages")
    assert tuple(report[field] for field in fields) == (
        accepted, duplicates, rejected, attempts, pages
    ), report


def summary(report):
    return {key: value for key, value in report.items() if key != "jobs"}


class SimulatedCrash(Exception):
    pass


def crash_after_page(key, page):
    if key == ("demomart", "store-01", "run-1") and page == 2:
        raise SimulatedCrash


def verify(directory):
    pages = fixture()
    results = {}
    for name, faults, attempts in (("healthy", None, 16), ("recovery", RECOVERY, 18)):
        clock = VirtualClock()
        with Ledger(directory / f"{name}.sqlite") as db:
            collector = runner(db, pages, faults=faults, clock=clock)
            report = collector.collect(stores(2), RUNS, 40)
            check_counts(report, 160, 12, 2, attempts, 16)
            assert report["completeness"] == 1
            assert {j["status"] for j in report["jobs"]} == {"complete", "complete_with_rejections"}
            results[name] = summary(report)
            replay = collector.collect(stores(2), RUNS, 40)
            assert replay["jobs"] == report["jobs"]
            results[f"{name}_replay"] = summary(replay)
            key = ("demomart", "store-01", "run-1")
            get = lambda product: db.lookup(key, product)
            first = get("sku-001")["observation"]
            second = get("sku-002")["observation"]
            other = db.lookup(("demomart", "store-02", "run-1"), "sku-001")["observation"]
            assert first["name"] == second["name"] and first["product_id"] != second["product_id"]
            assert first["price_cents"] != other["price_cents"]
            repeated = db.lookup(("demomart", "store-01", "run-2"), "sku-006")["observation"]
            assert repeated["price_cents"] == get("sku-006")["observation"]["price_cents"]
            assert repeated["observed_at"] != get("sku-006")["observation"]["observed_at"]
            assert get("sku-003")["outcome"] == "unavailable"
            assert get("sku-004")["outcome"] == "unknown_availability"
            assert get("sku-005")["observation"]["pricing_basis"] == "lb"
            assert first["pricing_basis"] == "package"
            assert get("absent")["outcome"] == "missing"
            assert get("bad-price")["outcome"] == "failed_to_collect"
            if faults:
                audit = [dict(row) for row in db.db.execute(
                    """SELECT attempt,result,retry_cause,delay_before FROM request_attempt
                       WHERE store_id='store-01' AND run_id='run-1' AND cursor='page-2'
                       ORDER BY id"""
                )]
                assert [row["result"] for row in audit] == ["429", "503", "ok"]
                assert [row["delay_before"] for row in audit] == [0, 0.12, 0.08]
                assert 0.12 in clock.delays and 0.08 in clock.delays
                assert report["retries_by_cause"] == {"429": 1, "503": 1}
                results["recovery_attempts"] = audit

    with Ledger(directory / "timeout.sqlite") as db:
        report = runner(db, pages, faults={TARGET: [("timeout", None)] * 3},
                        clock=VirtualClock()).collect(stores(1), RUNS[:1], 40)
        check_counts(report, 10, 0, 0, 4, 1)
        assert report["jobs"][0]["status"] == "incomplete"
        assert report["jobs"][0]["error"] == "timeout"
        assert report["retries_by_cause"] == {"timeout": 2}
        assert db.lookup(("demomart", *TARGET[:2]), "sku-011")["outcome"] == "failed_to_collect"
        results["timeout"] = report

    changed = deepcopy(pages)
    changed["store-02", "run-1", None]["served_store_id"] = "store-01"
    with Ledger(directory / "wrong-store.sqlite") as db:
        coverage = runner(db, changed, clock=VirtualClock()).collect(stores(2), RUNS, 40)
        check_counts(coverage, 120, 9, 2, 13, 12)
        failed = next(job for job in coverage["jobs"] if job["status"] == "failed")
        assert failed["accepted"] == failed["pages"] == 0
        assert failed["error"] == "location error: requested store-02, served store-01"

    crash_path = directory / "crash.sqlite"
    try:
        with Ledger(crash_path) as db:
            runner(db, pages, clock=VirtualClock()).collect_one("store-01", "run-1", 40, crash_after_page)
    except SimulatedCrash:
        pass
    else:
        raise AssertionError("Crash hook was not reached")
    with Ledger(crash_path) as db:
        before = db.report(0)
        check_counts(before, 20, 1, 2, 2, 2)
        after = runner(db, pages, clock=VirtualClock()).collect(stores(2), RUNS, 40)
        check_counts(after, 160, 12, 2, 16, 16)
        results["restart"] = {"before": summary(before), "after": summary(after)}

    for name in ("repeated_cursor", "missing_items", "conflicting_duplicate",
                 "empty_continuing", "empty_terminal", "pagination_limit"):
        changed = deepcopy(pages)
        page = changed[TARGET]
        if name == "repeated_cursor":
            page["next_cursor"] = "page-2"
        elif name == "missing_items":
            del page["items"]
        elif name == "conflicting_duplicate":
            page["items"][10]["price_cents"] += 1
        elif name in {"empty_continuing", "empty_terminal"}:
            page["items"] = []
            if name == "empty_terminal":
                page["next_cursor"] = None
            else:
                changed["store-01", "run-1", "page-3"]["items"][:0] = deepcopy(
                    pages[TARGET]["items"][:10]
                )
        limit = 2 if name == "pagination_limit" else 20
        with Ledger(directory / f"{name}.sqlite") as db:
            report = runner(db, changed, clock=VirtualClock(), max_pages=limit).collect(
                stores(1), RUNS[:1], 40
            )
            expected = {
                "repeated_cursor": (10, 0, 0, 2, 1, "repeated pagination cursor"),
                "missing_items": (10, 0, 0, 2, 1, "missing or invalid items"),
                "conflicting_duplicate": (40, 2, 3, 4, 4, None),
                "empty_continuing": (40, 2, 0, 4, 4, None),
                "empty_terminal": (10, 0, 0, 2, 2, "terminal page with unexpected observation count"),
                "pagination_limit": (20, 1, 2, 2, 2, "pagination limit reached"),
            }[name]
            check_counts(report, *expected[:5])
            assert report["jobs"][0]["error"] == expected[5]
            expected_status = {
                "conflicting_duplicate": "complete_with_rejections", "empty_continuing": "complete"
            }.get(name, "incomplete")
            assert report["jobs"][0]["status"] == expected_status
            if name == "conflicting_duplicate":
                reasons = [row[0] for row in db.db.execute(
                    "SELECT reason FROM row_outcome WHERE outcome='rejected'"
                )]
                assert reasons.count("conflicting duplicate") == 1
                saved = db.lookup(("demomart", "store-01", "run-1"), "sku-010")
                assert saved["observation"]["price_cents"] == pages["store-01", "run-1", None]["items"][9]["price_cents"]
            results[name] = report
    return results, coverage


def benchmark(directory):
    pages = fixture(50)
    results = {}
    for name, concurrency, faults in (
        ("sequential_healthy", 1, None),
        ("parallel_healthy", 8, None),
        ("parallel_recovery", 8, RECOVERY),
    ):
        print(f"Starting {name}: 50 stores, {concurrency} worker(s).", flush=True)
        with Ledger(directory / f"{name}.sqlite") as db:
            report = runner(db, pages, faults=faults, concurrency=concurrency).collect(
                stores(50), RUNS, 40
            )
        check_counts(report, 4000, 300, 2, 402 if faults else 400, 400)
        assert report["completeness"] == 1
        assert report["retries_by_cause"] == ({"429": 1, "503": 1} if faults else {})
        results[name] = dict(concurrency=concurrency, **summary(report))
        print(name, results[name]["elapsed_seconds"], "s", flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--benchmark", action="store_true",
        help="also time three 50-store runs; typically takes about 45 seconds",
    )
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=args.out) as staging:
        directory = Path(staging)
        results, coverage = verify(directory)
        for name, data in (("verification", results), ("coverage", coverage)):
            (args.out / f"{name}.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        print(f"Correctness and fault scenarios passed. Reports saved to {args.out.resolve()}.", flush=True)
        if args.benchmark:
            print("Running the benchmark. The sequential baseline takes about 30 seconds.", flush=True)
            data = benchmark(directory)
            (args.out / "benchmark.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
            print(f"Benchmark complete. Results saved to {(args.out / 'benchmark.json').resolve()}.", flush=True)


if __name__ == "__main__":
    main()
