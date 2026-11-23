"""Retailer-independent collection, persistence, and coverage reporting."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import sqlite3
import threading
import time
from typing import Protocol


def encode(value):
    return json.dumps(value, sort_keys=True, allow_nan=False)


class SourceError(Exception):
    pass


class FetchError(Exception):
    def __init__(self, cause, retry_after=None):
        super().__init__(cause)
        self.cause = cause
        self.retry_after = retry_after


@dataclass(frozen=True)
class Page:
    served_store_id: str
    next_cursor: str | None
    records: list[tuple[int, dict, dict]]
    errors: list[tuple[int, object, str]]


class Adapter(Protocol):
    retailer: str

    def fetch_page(self, store_id, run_id, cursor): ...

    def parse_page(self, raw_page) -> Page: ...


class SystemClock:
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)


SCHEMA = """
CREATE TABLE IF NOT EXISTS job (
    retailer TEXT, store_id TEXT, run_id TEXT, expected INTEGER,
    status TEXT DEFAULT 'incomplete', cursor TEXT, pages INTEGER DEFAULT 0,
    error TEXT, PRIMARY KEY (retailer, store_id, run_id)
);
CREATE TABLE IF NOT EXISTS page (
    retailer TEXT, store_id TEXT, run_id TEXT, page_no INTEGER,
    cursor TEXT NOT NULL, next_cursor TEXT,
    PRIMARY KEY (retailer, store_id, run_id, page_no),
    UNIQUE (retailer, store_id, run_id, cursor)
);
CREATE TABLE IF NOT EXISTS observation (
    retailer TEXT, store_id TEXT, run_id TEXT, product_id TEXT,
    page_no INTEGER, row_no INTEGER, payload TEXT,
    PRIMARY KEY (retailer, store_id, run_id, product_id)
);
CREATE TABLE IF NOT EXISTS row_outcome (
    retailer TEXT, store_id TEXT, run_id TEXT, page_no INTEGER, row_no INTEGER,
    outcome TEXT, reason TEXT, raw TEXT,
    PRIMARY KEY (retailer, store_id, run_id, page_no, row_no)
);
CREATE TABLE IF NOT EXISTS request_attempt (
    id INTEGER PRIMARY KEY, retailer TEXT, store_id TEXT, run_id TEXT,
    cursor TEXT, attempt INTEGER, result TEXT, retry_cause TEXT,
    delay_before REAL
);
"""


class Ledger:
    def __init__(self, path):
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.db.close()

    def job(self, key, expected):
        with self.lock, self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO job (retailer, store_id, run_id, expected) VALUES (?,?,?,?)",
                (*key, expected),
            )
            job = self.db.execute(
                "SELECT * FROM job WHERE (retailer, store_id, run_id) = (?,?,?)", key
            ).fetchone()
            if job["expected"] != expected:
                raise ValueError(f"Expected count changed for {key}")
            return job

    def attempt(self, key, cursor, number, result, retry_cause, delay):
        with self.lock, self.db:
            self.db.execute(
                """INSERT INTO request_attempt
                   (retailer,store_id,run_id,cursor,attempt,result,retry_cause,delay_before)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (*key, cursor, number, result, retry_cause, delay),
            )

    def stop(self, key, job, reason, retryable=False):
        status = "incomplete" if job["pages"] or retryable else "failed"
        with self.lock, self.db:
            self.db.execute(
                """UPDATE job SET status=?, error=?
                   WHERE (retailer,store_id,run_id)=(?,?,?) AND pages=?""",
                (status, reason, *key, job["pages"]),
            )

    def commit_page(self, key, job, page, max_pages):
        with self.lock, self.db:
            # Acquire the SQLite writer lock before reading the checkpoint.
            self.db.execute("BEGIN IMMEDIATE")
            current = self.db.execute(
                "SELECT pages,status FROM job WHERE (retailer,store_id,run_id)=(?,?,?)", key
            ).fetchone()
            if current["pages"] != job["pages"] or current["status"].startswith("complete"):
                return False
            cursor = job["cursor"]
            page_no = job["pages"] + 1
            if page.next_cursor is not None:
                prior = self.db.execute(
                    """SELECT 1 FROM page
                       WHERE (retailer,store_id,run_id,cursor)=(?,?,?,?)""",
                    (*key, encode(page.next_cursor)),
                ).fetchone()
                if page.next_cursor == cursor or prior:
                    raise SourceError("repeated pagination cursor")
            self.db.execute(
                "INSERT INTO page VALUES (?,?,?,?,?,?)",
                (*key, page_no, encode(cursor), page.next_cursor),
            )
            outcomes = [(row, "rejected", reason, raw) for row, raw, reason in page.errors]
            for row, record, raw in page.records:
                payload = encode(record)
                product = record["product_id"]
                previous = self.db.execute(
                    """SELECT payload FROM observation
                       WHERE (retailer,store_id,run_id,product_id)=(?,?,?,?)""",
                    (*key, product),
                ).fetchone()
                if previous is None:
                    self.db.execute(
                        "INSERT INTO observation VALUES (?,?,?,?,?,?,?)",
                        (*key, product, page_no, row, payload),
                    )
                    outcomes.append((row, "accepted", None, raw))
                elif previous["payload"] == payload:
                    outcomes.append((row, "duplicate", None, raw))
                else:
                    outcomes.append((row, "rejected", "conflicting duplicate", raw))
            self.db.executemany(
                "INSERT INTO row_outcome VALUES (?,?,?,?,?,?,?,?)",
                ((*key, page_no, row, outcome, reason, encode(raw))
                 for row, outcome, reason, raw in outcomes),
            )
            status, error = "incomplete", None
            if page.next_cursor is None:
                counts = self.counts(key)
                if counts["accepted"] != job["expected"]:
                    error = "terminal page with unexpected observation count"
                else:
                    status = "complete_with_rejections" if counts["rejected"] else "complete"
            elif page_no >= max_pages:
                error = "pagination limit reached"
            self.db.execute(
                """UPDATE job SET status=?, cursor=?, pages=?, error=?
                   WHERE (retailer,store_id,run_id)=(?,?,?)""",
                (status, page.next_cursor, page_no, error, *key),
            )
            return True

    def counts(self, key):
        return Counter(dict(self.db.execute(
            """SELECT outcome,COUNT(*) FROM row_outcome
               WHERE (retailer,store_id,run_id)=(?,?,?) GROUP BY outcome""", key
        )))

    def report(self, elapsed):
        report = {"elapsed_seconds": round(elapsed, 6), "jobs": []}
        totals = Counter()
        retries = Counter()
        with self.lock:
            for job in self.db.execute("SELECT * FROM job ORDER BY retailer, store_id, run_id"):
                key = tuple(job[name] for name in ("retailer", "store_id", "run_id"))
                counts = self.counts(key)
                attempts = list(self.db.execute(
                    """SELECT result,retry_cause FROM request_attempt
                       WHERE (retailer,store_id,run_id)=(?,?,?)""", key
                ))
                causes = Counter(a["retry_cause"] for a in attempts if a["retry_cause"])
                stats = dict(
                    accepted=counts["accepted"], duplicates=counts["duplicate"],
                    rejected=counts["rejected"], expected=job["expected"],
                    request_attempts=len(attempts), pages=job["pages"],
                )
                report["jobs"].append(dict(
                    retailer=key[0], store_id=key[1], run_id=key[2],
                    status=job["status"], error=job["error"], **stats,
                    retries_by_cause=dict(causes), **ratios(stats),
                ))
                totals.update(stats)
                retries.update(causes)
        report.update(totals, retries_by_cause=dict(retries), **ratios(totals))
        return report

    def lookup(self, key, product_id):
        with self.lock:
            record = self.db.execute(
                """SELECT payload FROM observation
                   WHERE (retailer,store_id,run_id,product_id)=(?,?,?,?)""",
                (*key, product_id),
            ).fetchone()
            if record:
                payload = json.loads(record["payload"])
                availability = {True: "available", False: "unavailable", None: "unknown_availability"}
                return dict(outcome=availability[payload["available"]], observation=payload)
            job = self.db.execute(
                "SELECT status FROM job WHERE (retailer,store_id,run_id)=(?,?,?)", key
            ).fetchone()
            # A rejected listing with a known ID was seen and failed validation.
            rejected = self.db.execute(
                """SELECT 1 FROM row_outcome WHERE (retailer,store_id,run_id)=(?,?,?)
                   AND outcome='rejected' AND json_extract(raw,'$.product_id')=?""",
                (*key, product_id),
            ).fetchone()
            missing = job and job["status"].startswith("complete") and not rejected
            return {"outcome": "missing" if missing else "failed_to_collect"}


def ratios(stats):
    return dict(
        completeness=stats["accepted"] / stats["expected"] if stats["expected"] else None,
        requests_per_accepted_observation=(
            stats["request_attempts"] / stats["accepted"] if stats["accepted"] else None
        ),
    )


class Runner:
    def __init__(self, adapter: Adapter, ledger: Ledger, *, clock=None,
                 concurrency=5, per_store_interval=0.075, max_pages=20):
        if concurrency < 1 or per_store_interval < 0 or max_pages < 1:
            raise ValueError("Invalid request or pagination limits")
        self.adapter = adapter
        self.ledger = ledger
        self.clock = clock or SystemClock()
        self.concurrency = concurrency
        self.interval = per_store_interval
        self.max_pages = max_pages
        self.slots = threading.BoundedSemaphore(concurrency)
        self.schedule_lock = threading.Lock()
        self.store_locks = {}
        self.next_start = {}

    def fetch(self, key, cursor):
        _, store_id, run_id = key
        with self.schedule_lock:
            store_lock = self.store_locks.setdefault(store_id, threading.Lock())
        delay, retry_cause = 0.0, None
        for attempt in range(1, 4):
            if delay:
                self.clock.sleep(delay)
            with store_lock:
                wait = self.next_start.get(store_id, 0) - self.clock.monotonic()
                if wait > 0:
                    self.clock.sleep(wait)
                with self.slots:
                    self.next_start[store_id] = self.clock.monotonic() + self.interval
                    try:
                        raw = self.adapter.fetch_page(store_id, run_id, cursor)
                    except FetchError as failure:
                        error = failure
                    else:
                        error = None
            self.ledger.attempt(key, cursor, attempt, error.cause if error else "ok",
                                retry_cause, delay)
            if error is None:
                return raw
            if attempt == 3 or error.cause not in {"429", "503", "timeout"}:
                raise error
            retry_cause = error.cause
            delay = error.retry_after if error.retry_after is not None else 0.05 * attempt

    def collect_one(self, store_id, run_id, expected, after_commit=None):
        key = (self.adapter.retailer, store_id, run_id)
        while True:
            job = self.ledger.job(key, expected)
            if job["status"].startswith("complete"):
                return
            if job["pages"] and job["cursor"] is None:
                return
            if job["pages"] >= self.max_pages:
                self.ledger.stop(key, job, "pagination limit reached")
                return
            try:
                raw = self.fetch(key, job["cursor"])
                page = self.adapter.parse_page(raw)
                if page.served_store_id != store_id:
                    raise SourceError(
                        f"location error: requested {store_id}, served {page.served_store_id}"
                    )
                committed = self.ledger.commit_page(key, job, page, self.max_pages)
            except FetchError as error:
                self.ledger.stop(key, job, error.cause,
                                 retryable=error.cause in {"429", "503", "timeout"})
                return
            except SourceError as error:
                self.ledger.stop(key, job, str(error))
                return
            if committed and after_commit:
                after_commit(key, job["pages"] + 1)

    def collect(self, stores, runs, expected, after_commit=None):
        runs = tuple(dict.fromkeys(runs))

        def collect_store(store_id):
            for run_id in runs:
                self.collect_one(store_id, run_id, expected, after_commit)

        start = self.clock.monotonic()
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            list(pool.map(collect_store, dict.fromkeys(stores)))
        return self.ledger.report(self.clock.monotonic() - start)
