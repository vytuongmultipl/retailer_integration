# Track 3 · DemoMart retailer integration

DemoMart collects a generated local fixture into SQLite. The default dataset has two stores, two runs per store, four pages per store/run, and 40 products shared across stores. It uses the Python standard library and makes no network calls.

## Run locally

From this directory, using Python 3.11 or later:

```powershell
python -B collect.py --db .\data\demomart.sqlite --report .\data\coverage.json
python -B collect.py --db .\data\demomart.sqlite --report .\data\coverage.json
python -B evidence.py --out .\reports --benchmark
```

The second collection reuses the committed checkpoints and adds zero requests or observations for completed jobs. Reports show cumulative ledger counts and request costs, and elapsed time for the current invocation. Use a fresh database for an independent measurement. Omit `--benchmark` to rerun only the correctness and fault scenarios with a virtual clock.

## Collection and reporting

- `demomart.py` generates the fixture, injects local faults, checks the response shape, and normalizes listing fields. Every fixture request waits 50 ms on its supplied clock, including failed attempts.
- `collector.py` owns scheduling, retries, checkpoints, status, and coverage. One runner bounds concurrent requests globally and spaces request starts for each store across both runs and any retries. The default spacing is 75 ms. Each fetch gets up to three attempts for HTTP 429, HTTP 503, or a timeout. A supplied delay takes precedence over the default 50 ms and 100 ms retry backoffs. The attempt log attributes each retry to the error that triggered it.
- Each committed page stores its cursor lineage, normalized observations, row outcomes with raw payloads, and the next checkpoint in one SQLite transaction. Observations retain source IDs and timestamp, raw size, both prices, currency, pricing basis, availability, page number, and row number. The page table resolves the page number to the original request cursor. Exact normalized repeats count as duplicate rows. A conflicting repeat is quarantined with its reason and the original observation stays authoritative. A restarted process resumes at the last committed cursor. The three-attempt limit applies to each fetch cycle, so an explicit restart can retry a page that exhausted its previous cycle.
- A terminal page is accepted only after validating the response and matching the served store. Coverage must match the expected count for a completed status. A job that reaches that count at a valid terminal page is `complete` or `complete_with_rejections`. Retry exhaustion, partial progress, a page cap, or a terminal count mismatch yields `incomplete`. A structural or location error before any page commits yields `failed`. A valid empty page with a next cursor continues collection. The default 20-page cap bounds unique-cursor chains and a previously seen cursor stops collection immediately.
- Product lookup distinguishes `missing` on completed coverage, `unavailable` for a collected listing with false availability, `unknown_availability` when the source omits availability, and `failed_to_collect` when coverage or validation prevents a product-level conclusion.

## Evidence

The two-store verification checks 160 accepted observations, 12 duplicate row outcomes, and two rejections from 174 row appearances. Healthy collection uses 16 requests. Recovery uses 18, with 429 and 503 on the designated page before success. The checks cover replay of both completed collections, timeout exhaustion, wrong-store isolation, product and store identity, post-commit interruption and restart, malformed responses, cursor repetition, conflicting prices, empty pages, and the page cap.

- `reports/verification.json` contains correctness and fault results. Elapsed values in this file are virtual-clock times.
- `reports/coverage.json` shows one failed store/run alongside three completed jobs. The wrong-store response contributes zero observations and total coverage is 120 of 160.
- `reports/benchmark.json` records wall-clock times on the same 50-store dataset for sequential healthy, bounded parallel healthy, and bounded parallel recovery runs. All three use 75 ms per-store spacing and 50 ms per mock attempt. Each collects 4,000 observations with 300 duplicate outcomes and two rejected rows. Healthy runs use 400 requests and the recovery run uses 402. Completeness is accepted unique observations divided by expected observations. Request cost is all attempts divided by accepted unique observations.

## Cloud deployment design

Package the collector in a versioned container. A scheduler creates a durable job per retailer, store, and run, and workers lease jobs from a queue. Heartbeats renew leases during long runs. Expired leases return to the queue and the next worker resumes at the durable checkpoint. Use a shared transactional database such as PostgreSQL for checkpoint, outcome, and observation commits across containers. The delivered SQLite runner is intended for a single local process.

Enforce a retailer concurrency cap and retailer request-rate budget across all workers using a shared limiter. Add a store-level spacing key and charge a permit for every attempt, including failures and retries. For preflight, enumerate jobs, estimate nominal pages, and compare the planned requests plus retry headroom and preflight probes against remaining quota and the run deadline. For this 400-page shape, the worst case at three attempts per page is 1,200 requests before probes. If the remaining quota supports a smaller scope, schedule only the jobs that fit and keep the rest pending. Enforce the budget again at request time so estimates cannot authorize an overrun.

Before releasing the full job fan-out, run a small canary against representative stores. Check served store ID, required response fields, cursor progression, availability and price types, and accepted versus expected coverage. Compare results against a known-good contract and recent collection metrics. Hold the remaining jobs when a canary reports a location mismatch, response schema change, cursor loop, or unexpected coverage loss. Save the raw canary response securely for adapter investigation and rerun the canary before resuming.
