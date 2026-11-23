"""DemoMart's deterministic local source and response adapter."""

from copy import deepcopy
from datetime import datetime

from collector import FetchError, Page, SourceError, SystemClock


def fixture(store_count=2):
    pages = {}
    for store_number in range(1, store_count + 1):
        store = f"store-{store_number:02}"
        for run_number in (1, 2):
            run = f"run-{run_number}"
            listings = []
            for sku in range(1, 41):
                price = 350 + sku * 3 + store_number * 11 + run_number * (sku != 6)
                listings.append(dict(
                    product_id=f"sku-{sku:03}", name="Demo Milk" if sku <= 2 else f"Demo Item {sku}",
                    brand="Demo", size_text="1 gal" if sku <= 2 else "1 lb",
                    price_cents=price, regular_price_cents=price + 100, currency="USD",
                    pricing_basis="lb" if sku == 5 else "package",
                    available=sku != 3, observed_at=f"2026-01-07T{9 + run_number}:00:00Z",
                ))
            del listings[3]["available"]
            for page_number in range(1, 5):
                offset = (page_number - 1) * 10
                items = deepcopy(listings[offset:offset + 10])
                if offset:
                    items.append(deepcopy(listings[offset - 1]))
                if (store_number, run_number, page_number) == (1, 1, 2):
                    missing_id = items[0].copy()
                    del missing_id["product_id"]
                    items.extend([missing_id, dict(items[1], product_id="bad-price", price_cents=-1)])
                cursor = None if page_number == 1 else f"page-{page_number}"
                pages[store, run, cursor] = {
                    "served_store_id": store,
                    "next_cursor": f"page-{page_number + 1}" if page_number < 4 else None,
                    "items": items,
                }
    return pages


class FixtureTransport:
    def __init__(self, pages, faults=None, clock=None):
        self.pages = pages
        self.faults = {key: list(sequence) for key, sequence in (faults or {}).items()}
        self.clock = clock or SystemClock()

    def request(self, store, run, cursor):
        self.clock.sleep(0.05)
        key = store, run, cursor
        if self.faults.get(key):
            fault = self.faults[key].pop(0)
            raise FetchError(*fault)
        if key not in self.pages:
            raise FetchError("fixture page not found")
        return deepcopy(self.pages[key])


class DemoMart:
    retailer = "demomart"

    def __init__(self, transport):
        self.transport = transport

    def fetch_page(self, store_id, run_id, cursor):
        return self.transport.request(store_id, run_id, cursor)

    def parse_page(self, raw_page):
        if not isinstance(raw_page, dict):
            raise SourceError("response must be an object")
        store = raw_page.get("served_store_id")
        if not isinstance(store, str) or not store.strip():
            raise SourceError("missing or invalid served_store_id")
        if "next_cursor" not in raw_page:
            raise SourceError("missing next_cursor")
        cursor = raw_page["next_cursor"]
        if cursor is not None and (not isinstance(cursor, str) or not cursor.strip()):
            raise SourceError("invalid next_cursor")
        if not isinstance(raw_page.get("items"), list):
            raise SourceError("missing or invalid items")
        records, errors = [], []
        for index, raw in enumerate(raw_page["items"], start=1):
            try:
                records.append((index, self.normalize(raw), raw))
            except ValueError as error:
                errors.append((index, raw, str(error)))
        return Page(store, cursor, records, errors)

    @staticmethod
    def normalize(raw):
        if not isinstance(raw, dict):
            raise ValueError("listing must be an object")
        record = {}
        for field in ("product_id", "name", "brand", "size_text", "currency",
                      "pricing_basis", "observed_at"):
            value = raw.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"missing or invalid {field}")
            record[field] = value
        for field in ("price_cents", "regular_price_cents"):
            value = raw.get(field)
            if type(value) is not int or value < 0:
                raise ValueError(f"missing or invalid {field}: expected nonnegative integer")
            record[field] = value
        timestamp = datetime.fromisoformat(record["observed_at"])
        if timestamp.utcoffset() is None:
            raise ValueError("observed_at must include an offset")
        availability = raw.get("available")
        if availability is not None and type(availability) is not bool:
            raise ValueError("invalid available")
        record["available"] = availability
        return record
