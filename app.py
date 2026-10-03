"""
Economic Calendar - official published figures, Investing.com-style
=====================================================================

Run with:   streamlit run app.py

One page: a weekly calendar (date, country, event + period, actual, previous,
change, % change, status) and a "latest figure" table per country.

Sources (all official statistics agencies or central banks, all free):
  US  FRED (St. Louis Fed)          - needs a free FRED API key for dates
  EU  Eurostat (euro area)          - no key
  UK  Office for National Statistics- no key
  JP  Statistics Bureau of Japan, via the DBnomics open-data mirror - no key
  AU  Australian Bureau of Statistics Data API - no key
  SG  Singapore Department of Statistics (SingStat) - no key
  KR  Bank of Korea ECOS - works with the public 'sample' key; a free personal
      key (ECOS_API_KEY) is recommended
  CA  Statistics Canada - no key
  CN, NZ - not covered: see NOT_COVERED below.

No consensus forecasts are shown anywhere: only figures the agencies published.

Every publication date carries a "Date basis":
  official    the agency's own release timestamp for that figure
  approx.     the agency's dataset-update timestamp, accepted only on weekdays
              and within the indicator's normal publication lag
  first seen  the source gives no date; the date this app first saw the figure
              (blank on the very first run)
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import socket
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import streamlit as st

# fredapi calls urlopen() with no timeout; bound it process-wide.
socket.setdefaulttimeout(30)
try:
    from fredapi import Fred
except ImportError:  # pragma: no cover
    Fred = None

log = logging.getLogger("econ_calendar")

APP_DIR = Path(__file__).resolve().parent
STORE_DIR = Path(os.getenv("MACRO_STORE_DIR", APP_DIR / "data_store"))
MARKET_TZ = ZoneInfo("America/New_York")
FRED_META_TTL = 600          # how often to ask FRED "has anything changed?"
INTL_TTL = 900               # non-US sources refresh every 15 minutes
CALENDAR_TTL = 1800
RELEASE_MAP_MAX_AGE = 7 * 86400
FRED_CALLS_PER_MIN = 100     # under FRED's ~120 requests/min per key
UA = {"User-Agent": "economic-calendar-dashboard/1.0 (personal use)"}

@dataclass(frozen=True)
class Event:
    """One calendar line.
    source/ref: fred -> series id | eurostat -> "dataset|dim=val,dim=val"
                ons -> ONS timeseries URI path | statcan -> vector id
    calc:   level | mom | yoy | diff | on_change (policy rates: print = last change)
    change: pp  -> figure is a rate; show change in percentage points, no % change
            pct -> figure is a level; show absolute change AND % change
            abs -> show absolute change only (changes-of-changes, diffusion indexes,
                   balances that can flip sign: a % change would be meaningless)
    importance: 1-3, mirrors the low/medium/high flags on Investing.com
    """
    country: str
    name: str
    source: str
    ref: str
    calc: str = "level"
    scale: float = 1.0
    unit: str = "%"
    change: str = "pp"
    decimals: int = 1
    importance: int = 2
    note: str = ""
    max_lag_days: int | None = None  # Eurostat only: plausibility window for release dating

    @property
    def key(self) -> str:
        return f"{self.country}:{self.name}"


E = Event
EUROSTAT_HICP = "prc_hicp_minr|geo=EA,unit={u},coicop18={c}"
EVENTS: list[Event] = [
    # ---------------- United States (FRED) ----------------
    E("US", "CPI (MoM)", "fred", "CPIAUCSL", "mom", importance=3),
    E("US", "CPI (YoY)", "fred", "CPIAUCNS", "yoy", importance=3,
      note="From the not seasonally adjusted index, as BLS reports the 12-month change."),
    E("US", "Core CPI (MoM)", "fred", "CPILFESL", "mom", importance=3),
    E("US", "Core CPI (YoY)", "fred", "CPILFENS", "yoy", importance=3,
      note="From the not seasonally adjusted index, as BLS reports the 12-month change."),
    E("US", "PPI (MoM)", "fred", "PPIFIS", "mom", importance=2),
    E("US", "PPI (YoY)", "fred", "PPIFID", "yoy", importance=2,
      note="From the unadjusted final-demand index, as BLS reports the 12-month change."),
    E("US", "Core PPI (MoM)", "fred", "PPIFES", "mom", importance=2,
      note="Final demand less foods and energy."),
    E("US", "PCE Price Index (MoM)", "fred", "PCEPI", "mom", importance=2),
    E("US", "PCE Price Index (YoY)", "fred", "PCEPI", "yoy", importance=2),
    E("US", "Core PCE Price Index (MoM)", "fred", "PCEPILFE", "mom", importance=3),
    E("US", "Core PCE Price Index (YoY)", "fred", "PCEPILFE", "yoy", importance=3),
    E("US", "Import Price Index (MoM)", "fred", "IR", "mom", importance=1),
    E("US", "Nonfarm Payrolls", "fred", "PAYEMS", "diff", unit="k", change="abs", decimals=0, importance=3),
    E("US", "Private Nonfarm Payrolls", "fred", "USPRIV", "diff", unit="k", change="abs", decimals=0, importance=2),
    E("US", "Manufacturing Payrolls", "fred", "MANEMP", "diff", unit="k", change="abs", decimals=0, importance=1),
    E("US", "Unemployment Rate", "fred", "UNRATE", importance=3),
    E("US", "U6 Unemployment Rate", "fred", "U6RATE", importance=1),
    E("US", "Participation Rate", "fred", "CIVPART", importance=1),
    E("US", "Average Hourly Earnings (MoM)", "fred", "CES0500000003", "mom", importance=2),
    E("US", "Average Hourly Earnings (YoY)", "fred", "CES0500000003", "yoy", importance=2),
    E("US", "Average Weekly Hours", "fred", "AWHAETP", unit="hrs", change="abs", importance=1),
    E("US", "ADP Nonfarm Employment Change", "fred", "ADPMNUSNERSA", "diff", scale=1 / 1000,
      unit="k", change="abs", decimals=0, importance=2),
    E("US", "JOLTS Job Openings", "fred", "JTSJOL", scale=1 / 1000, unit="m", change="pct",
      decimals=3, importance=2),
    E("US", "Initial Jobless Claims", "fred", "ICSA", scale=1 / 1000, unit="k", change="pct",
      decimals=0, importance=2),
    E("US", "Continuing Jobless Claims", "fred", "CCSA", scale=1 / 1000, unit="k", change="pct",
      decimals=0, importance=1),
    E("US", "Jobless Claims 4-Week Avg.", "fred", "IC4WSA", scale=1 / 1000, unit="k", change="pct",
      decimals=0, importance=1),
    E("US", "GDP (QoQ, annualised)", "fred", "A191RL1Q225SBEA", importance=3),
    E("US", "GDP Price Index (QoQ)", "fred", "A191RI1Q225SBEA", importance=1),
    E("US", "Retail Sales (MoM)", "fred", "RSAFS", "mom", importance=3),
    E("US", "Core Retail Sales (MoM)", "fred", "RSFSXMV", "mom", importance=2,
      note="Excluding motor vehicle and parts dealers."),
    E("US", "Industrial Production (MoM)", "fred", "INDPRO", "mom", importance=2),
    E("US", "Capacity Utilization Rate", "fred", "TCU", importance=1),
    E("US", "Durable Goods Orders (MoM)", "fred", "DGORDER", "mom", importance=2),
    E("US", "Core Durable Goods Orders (MoM)", "fred", "ADXTNO", "mom", importance=2,
      note="Excluding transportation."),
    E("US", "Factory Orders (MoM)", "fred", "AMTMNO", "mom", importance=1),
    E("US", "Personal Income (MoM)", "fred", "PI", "mom", importance=1),
    E("US", "Personal Spending (MoM)", "fred", "PCE", "mom", importance=2),
    E("US", "Trade Balance", "fred", "BOPGSTB", scale=1 / 1000, unit="$bn", change="abs", importance=2),
    E("US", "Housing Starts", "fred", "HOUST", unit="k", change="pct", decimals=0, importance=2),
    E("US", "Building Permits", "fred", "PERMIT", unit="k", change="pct", decimals=0, importance=2),
    E("US", "New Home Sales", "fred", "HSN1F", unit="k", change="pct", decimals=0, importance=2),
    E("US", "Existing Home Sales", "fred", "EXHOSLUSM495S", scale=1 / 1e6, unit="m", change="pct",
      decimals=2, importance=2),
    E("US", "Construction Spending (MoM)", "fred", "TTLCONS", "mom", importance=1),
    E("US", "S&P/Case-Shiller National HPI (YoY)", "fred", "CSUSHPISA", "yoy", importance=1),
    E("US", "Michigan Consumer Sentiment", "fred", "UMCSENT", unit="index", change="abs", importance=2,
      note="FRED carries UMich data with roughly a one-month delay."),
    E("US", "Michigan 1-Year Inflation Expectations", "fred", "MICH", importance=1,
      note="FRED carries UMich data with roughly a one-month delay."),
    E("US", "Philadelphia Fed Manufacturing Index", "fred", "GACDFSA066MSFRBPHI", unit="index",
      change="abs", importance=2),
    E("US", "NY Empire State Manufacturing Index", "fred", "GACDISA066MSFRBNY", unit="index",
      change="abs", importance=2),
    E("US", "Fed Interest Rate Decision (upper bound)", "fred", "DFEDTARU", "on_change",
      decimals=2, importance=3, note="Shows the most recent change in the target range."),
    # ---------------- Euro area (Eurostat) ----------------
    E("EA", "ECB Deposit Facility Rate Decision", "fred", "ECBDFR", "on_change", decimals=2, importance=3,
      note="Shows the most recent change; from the ECB via FRED."),
    E("EA", "CPI (YoY)", "eurostat", EUROSTAT_HICP.format(u="RCH_A", c="TOTAL"), importance=3, max_lag_days=20),
    E("EA", "CPI (MoM)", "eurostat", EUROSTAT_HICP.format(u="RCH_M", c="TOTAL"), importance=2, max_lag_days=20),
    E("EA", "Core CPI (YoY)", "eurostat", EUROSTAT_HICP.format(u="RCH_A", c="TOT_X_NRG_FOOD"),
      importance=3, note="HICP excluding energy and food (food incl. alcohol & tobacco).", max_lag_days=20),
    E("EA", "Services Inflation (YoY)", "eurostat", EUROSTAT_HICP.format(u="RCH_A", c="SERV"), importance=2,
      max_lag_days=20),
    E("EA", "Unemployment Rate", "eurostat",
      "une_rt_m|geo=EA21,s_adj=SA,age=TOTAL,sex=T,unit=PC_ACT", importance=2, max_lag_days=35),
    E("EA", "GDP (QoQ)", "eurostat", "namq_10_gdp|geo=EA21,unit=CLV_PCH_PRE,s_adj=SCA,na_item=B1GQ",
      importance=3, max_lag_days=70, note="Flash (~t+30), then revised estimates up to ~t+65 days."),
    E("EA", "GDP (YoY)", "eurostat", "namq_10_gdp|geo=EA21,unit=CLV_PCH_SM,s_adj=SCA,na_item=B1GQ",
      importance=2, max_lag_days=70),
    E("EA", "Retail Sales (MoM)", "eurostat",
      "sts_trtu_m|geo=EA21,indic_bt=VOL_SLS,nace_r2=G47,s_adj=SCA,unit=PCH_PRE", importance=2,
      max_lag_days=40),
    E("EA", "Industrial Production (MoM)", "eurostat",
      "sts_inpr_m|geo=EA21,indic_bt=PRD,nace_r2=B-D,s_adj=SCA,unit=PCH_PRE", importance=2,
      max_lag_days=50),
    E("EA", "PPI (MoM)", "eurostat",
      "sts_inppd_m|geo=EA21,indic_bt=PRC_PRR_DOM,nace_r2=B-E36,s_adj=NSA,unit=PCH_PRE", importance=1,
      max_lag_days=40),
    # ---------------- United Kingdom (ONS) ----------------
    E("UK", "CPI (YoY)", "ons", "/economy/inflationandpriceindices/timeseries/d7g7/mm23", importance=3),
    E("UK", "CPI (MoM)", "ons", "/economy/inflationandpriceindices/timeseries/d7oe/mm23", importance=2),
    E("UK", "Core CPI (YoY)", "ons", "/economy/inflationandpriceindices/timeseries/dko8/mm23", importance=3,
      note="Excluding energy, food, alcohol and tobacco."),
    E("UK", "Unemployment Rate", "ons",
      "/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms", importance=2,
      note="ILO rate, 3-month average ending in the month shown (ONS convention)."),
    E("UK", "Average Earnings incl. Bonus (3M YoY)", "ons",
      "/employmentandlabourmarket/peopleinwork/earningsandworkinghours/timeseries/kac3/lms", importance=2),
    E("UK", "GDP (MoM)", "ons", "/economy/grossdomesticproductgdp/timeseries/ecy2/mgdp", "mom",
      importance=2, note="Monthly GVA index, chained volume measure."),
    E("UK", "GDP (QoQ)", "ons", "/economy/grossdomesticproductgdp/timeseries/ihyq/qna", importance=3),
    E("UK", "GDP (YoY)", "ons", "/economy/grossdomesticproductgdp/timeseries/ihyr/qna", importance=2),
    E("UK", "Retail Sales (MoM)", "ons", "/businessindustryandtrade/retailindustry/timeseries/j5ek/drsi",
      "mom", importance=2, note="Volume, all retailers including fuel."),
    # ---------------- Canada (Statistics Canada) ----------------
    E("CA", "CPI (MoM)", "statcan", "41690973", "mom", importance=3),
    E("CA", "CPI (YoY)", "statcan", "41690973", "yoy", importance=3),
    E("CA", "Unemployment Rate", "statcan", "2062815", importance=3),
    E("CA", "Employment Change", "statcan", "2062811", "diff", unit="k", change="abs", decimals=1,
      importance=3),
    E("CA", "GDP (MoM)", "statcan", "65201210", "mom", importance=2),
    # ---------------- Japan (Statistics Bureau via DBnomics mirror) ----------------
    E("JP", "National CPI (YoY)", "dbnomics", "STATJP/CPIm/001", "yoy", importance=3),
    E("JP", "National Core CPI (YoY)", "dbnomics", "STATJP/CPIm/733", "yoy", importance=3,
      note="Excluding fresh food (Japan's headline 'core' measure)."),
    E("JP", "Core-Core CPI (YoY)", "dbnomics", "STATJP/CPIm/740", "yoy", importance=2,
      note="Excluding fresh food and energy."),
    E("JP", "Unemployment Rate", "dbnomics", "STATJP/MIm/M.UR.B.PCT.SA", importance=2),
    # ---------------- Australia (ABS Data API) ----------------
    E("AU", "CPI (YoY)", "abs", "CPI|3.10001.10.50.M", importance=3, note="Complete monthly CPI."),
    E("AU", "CPI (MoM)", "abs", "CPI|2.10001.10.50.M", importance=2),
    E("AU", "Unemployment Rate", "abs", "LF|M13.3.1599.20.AUS.M", importance=3, note="Seasonally adjusted."),
    # ---------------- Singapore (SingStat) ----------------
    E("SG", "CPI (YoY)", "singstat", "M213751|All Items", "yoy", importance=2, max_lag_days=30),
    E("SG", "CPI (MoM)", "singstat", "M213751|All Items", "mom", importance=1, max_lag_days=30),
    # ---------------- Korea (Bank of Korea ECOS) ----------------
    E("KR", "CPI (YoY)", "ecos", "901Y009|0", "yoy", importance=3),
    E("KR", "CPI (MoM)", "ecos", "901Y009|0", "mom", importance=2),
    E("KR", "Unemployment Rate", "ecos", "901Y027|I61BC|I28B", importance=2, note="Seasonally adjusted."),
]

EVENT = {e.key: e for e in EVENTS}
FRED_SIDS = list(dict.fromkeys(e.ref for e in EVENTS if e.source == "fred"))

# Display order and codes (Investing.com style)
COUNTRIES = {"US": "United States", "EA": "Euro area", "UK": "United Kingdom", "JP": "Japan",
             "CN": "China", "AU": "Australia", "NZ": "New Zealand", "SG": "Singapore",
             "KR": "South Korea", "CA": "Canada"}
CODE = {"US": "US", "EA": "EU", "UK": "UK", "JP": "JP", "CN": "CN", "AU": "AU", "NZ": "NZ",
        "SG": "SG", "KR": "KR", "CA": "CA"}
NOT_COVERED = {
    "CN": "China's National Bureau of Statistics has no reliable public API; the DBnomics "
          "mirror of NBS data was last refreshed in June 2026, too stale to use.",
    "NZ": "Stats NZ's data API requires a registered subscription key, and I could not test "
          "it without one. RBNZ publishes Excel files only.",
}
SOURCE_NAME = {"fred": "FRED", "eurostat": "Eurostat", "ons": "ONS", "statcan": "Statistics Canada",
               "abs": "ABS", "dbnomics": "Statistics Japan (DBnomics)", "singstat": "SingStat",
               "ecos": "Bank of Korea ECOS"}

# =============================================================================
# INFRASTRUCTURE
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def scrub(err: object, secret: str | None = None) -> str:
    """Strip API keys from error text before it reaches the UI or logs.
    `requests` errors embed the full URL, including ?api_key=..."""
    msg = str(err)
    if secret:
        msg = msg.replace(secret, "***")
    msg = re.sub(r"api_key=[A-Za-z0-9]+", "api_key=***", msg)
    return msg[:300]


class PartialResult(Exception):
    """Raised from inside an @st.cache_data function when some fetches failed.

    Streamlit never caches a call that raises, so a degraded result (stale
    fallback data) is returned to the caller but NOT pinned in the cache for the
    full TTL; the next rerun retries. The circuit breaker stops that retrying
    from hammering an API that is down.
    """

    def __init__(self, payload):
        super().__init__("partial result")
        self.payload = payload


class DiskStore:
    """Parquet + JSON sidecar store. Gives persistence across restarts and a
    last-known-good copy when an upstream API is unavailable.
    Writes go to a unique temp file and are atomically renamed, so concurrent
    sessions never read a half-written file."""

    def __init__(self, root: Path):
        self.root = root

    def _paths(self, ns: str, key: str) -> tuple[Path, Path]:
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)[:80]
        digest = hashlib.sha1(key.encode()).hexdigest()[:8]
        folder = self.root / ns
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{safe}-{digest}.parquet", folder / f"{safe}-{digest}.json"

    @staticmethod
    def _atomic_write(path: Path, writer) -> None:
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        writer(tmp)
        os.replace(tmp, path)

    def save(self, ns: str, key: str, df: pd.DataFrame, meta: dict) -> None:
        pq, js = self._paths(ns, key)
        self._atomic_write(pq, lambda p: df.to_parquet(p))
        meta = {**meta, "saved_at": utc_now().isoformat(), "saved_epoch": time.time()}
        self._atomic_write(js, lambda p: p.write_text(json.dumps(meta, default=str)))

    def load(self, ns: str, key: str) -> tuple[pd.DataFrame | None, dict]:
        pq, js = self._paths(ns, key)
        if not pq.exists():
            return None, {}
        try:
            df = pd.read_parquet(pq)
            meta = json.loads(js.read_text()) if js.exists() else {}
            return df, meta
        except Exception as exc:  # corrupted file: treat as missing
            log.warning("store read failed %s/%s: %s", ns, key, exc)
            return None, {}

    def meta(self, ns: str, key: str) -> dict:
        _, js = self._paths(ns, key)
        try:
            return json.loads(js.read_text()) if js.exists() else {}
        except Exception:
            return {}

    def save_json(self, ns: str, key: str, obj: dict) -> None:
        _, js = self._paths(ns, key)
        self._atomic_write(js, lambda p: p.write_text(json.dumps(obj, default=str)))

    def load_json(self, ns: str, key: str) -> dict:
        return self.meta(ns, key)


class RateLimiter:
    """Thread-safe sliding-window limiter shared by all sessions (it lives in
    st.cache_resource), so several open browser tabs cannot jointly exceed FRED's
    per-key limit."""

    def __init__(self, max_calls: int, period: float):
        self.max_calls, self.period = max_calls, period
        self.calls: deque[float] = deque()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                while self.calls and now - self.calls[0] > self.period:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                wait = self.period - (now - self.calls[0])
            time.sleep(max(wait, 0.05))


class CircuitBreaker:
    """After `threshold` failures within `window` seconds, a source is skipped
    for `cooldown` seconds and the disk copy is served instead. Prevents every
    rerun from waiting on retries against an API that is down."""

    def __init__(self, threshold: int = 4, window: float = 120, cooldown: float = 180):
        self.threshold, self.window, self.cooldown = threshold, window, cooldown
        self.failures: dict[str, deque[float]] = {}
        self.lock = threading.Lock()

    def record(self, name: str, ok: bool) -> None:
        with self.lock:
            q = self.failures.setdefault(name, deque())
            if ok:
                q.clear()
            else:
                q.append(time.monotonic())

    def is_open(self, name: str) -> bool:
        with self.lock:
            q = self.failures.get(name, deque())
            now = time.monotonic()
            while q and now - q[0] > self.window:
                q.popleft()
            return len(q) >= self.threshold and now - q[-1] < self.cooldown


@st.cache_resource
def get_resources() -> dict:
    """Process-wide singletons (survive reruns, shared across sessions)."""
    return {
        "store": DiskStore(STORE_DIR),
        "fred_limiter": RateLimiter(FRED_CALLS_PER_MIN, 60.0),
        "breaker": CircuitBreaker(),
        "degraded": {},  # source -> {"at": epoch, "payload": last partial payload}
    }


DEGRADED_RETRY_S = 300


def guarded(res: dict, name: str, fn):
    """Run a cached loader. If it raised PartialResult recently, serve that
    payload instead of retrying on every rerun; retry at most every 5 minutes.
    Combined with 'exceptions are never cached', this gives: healthy results are
    cached for their full TTL, degraded results are retried on a slow cadence."""
    memo = res["degraded"].get(name)
    if memo and time.time() - memo["at"] < DEGRADED_RETRY_S:
        return memo["payload"]
    try:
        out = fn()
        res["degraded"].pop(name, None)
        return out
    except PartialResult as p:
        res["degraded"][name] = {"at": time.time(), "payload": p.payload}
        return p.payload


NON_RETRYABLE = ("api_key", "not registered", "does not exist", "bad request")


def is_transient(exc: BaseException) -> bool:
    """Permanent errors (bad key, unknown series) must not trip the circuit
    breaker, or one dead series ID would block every other FRED request."""
    return not any(tok in str(exc).lower() for tok in NON_RETRYABLE)


def call_with_retry(fn, *args, tries: int = 3, base_delay: float = 1.0, **kwargs):
    """Exponential backoff. Rate-limit responses wait longer; auth/invalid-ID
    errors fail fast because retrying cannot fix them."""
    last_exc: Exception | None = None
    for attempt in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - we re-raise after retries
            last_exc = exc
            msg = str(exc).lower()
            if any(tok in msg for tok in NON_RETRYABLE):
                break
            if attempt < tries - 1:
                rate_limited = "429" in msg or "too many" in msg
                time.sleep(20 if rate_limited else base_delay * 2 ** attempt)
    raise last_exc  # type: ignore[misc]


# =============================================================================
# FRED (United States)
# =============================================================================

class FredSource:
    """Thin wrapper around fredapi plus the release-calendar endpoints that
    fredapi does not cover. Works in a degraded, keyless mode via the public
    fredgraph CSV download (no metadata, no release calendar)."""

    API = "https://api.stlouisfed.org/fred"
    CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"

    def __init__(self, api_key: str | None, limiter: RateLimiter):
        self.key = api_key or None
        self.limiter = limiter
        self.fred = Fred(api_key=self.key) if (self.key and Fred is not None) else None
        self.key_fp = hashlib.sha256(self.key.encode()).hexdigest()[:10] if self.key else "keyless"

    @property
    def keyed(self) -> bool:
        return self.fred is not None

    # --- metadata -------------------------------------------------------------
    def info(self, sid: str) -> dict:
        self.limiter.acquire()
        info = call_with_retry(self.fred.get_series_info, sid, tries=2)
        return {k: info.get(k) for k in ("title", "frequency_short", "units_short",
                                         "seasonal_adjustment_short", "last_updated",
                                         "observation_end")}

    # --- observations -------------------------------------------------------------
    def observations(self, sid: str) -> pd.Series:
        if self.keyed:
            self.limiter.acquire()
            s = call_with_retry(self.fred.get_series, sid, tries=2)
        else:
            s = call_with_retry(self._fredgraph_csv, sid, tries=2)
        s = pd.to_numeric(pd.Series(s), errors="coerce")
        s.index = pd.to_datetime(s.index)
        # Keep NaNs out of storage, but transforms below are date-aligned so a
        # missing month (e.g. Oct-2025 CPI, not collected during the shutdown)
        # does not shift every later YoY calculation.
        return s.dropna().sort_index().rename(sid)

    def _fredgraph_csv(self, sid: str) -> pd.Series:
        r = requests.get(self.CSV, params={"id": sid}, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), na_values=".")
        if df.shape[1] < 2:
            raise ValueError(f"unexpected CSV layout for {sid}")
        return pd.Series(df.iloc[:, 1].to_numpy(), index=pd.to_datetime(df.iloc[:, 0]))

    # --- release endpoints (requests; fredapi has no wrappers for these) ----------
    def _get_json(self, endpoint: str, **params) -> dict:
        def _do():
            self.limiter.acquire()
            r = requests.get(f"{self.API}/{endpoint}",
                             params={**params, "api_key": self.key, "file_type": "json"},
                             timeout=30)
            if r.status_code == 429:
                raise RuntimeError("429 Too Many Requests")
            r.raise_for_status()
            js = r.json()
            if "error_code" in js:
                raise RuntimeError(js.get("error_message", "FRED error"))
            return js
        return call_with_retry(_do, tries=3)

    def series_release(self, sid: str) -> dict:
        js = self._get_json("series/release", series_id=sid)
        rel = (js.get("releases") or [{}])[0]
        return {"release_id": rel.get("id"), "release_name": rel.get("name"),
                "link": rel.get("link")}

    def first_release_dates(self, sid: str, since: str) -> pd.Series:
        """output_type=4 ('Initial Release Only'): each observation comes back
        once, with realtime_start = the date it was FIRST published. That is
        the release date of a print. FRED's series-level last_updated also
        moves on revisions, so it is only a fallback."""
        js = self._get_json("series/observations", series_id=sid, observation_start=since,
                            realtime_start="1776-07-04", realtime_end="9999-12-31", output_type=4)
        df = pd.DataFrame(js.get("observations", []))
        if df.empty:
            return pd.Series(dtype="datetime64[ns]", name="released")
        return pd.Series(pd.to_datetime(df["realtime_start"]).to_numpy(),
                         index=pd.to_datetime(df["date"]), name="released")

    def release_calendar(self, start: date, end: date) -> pd.DataFrame:
        """All FRED release dates in [start, end], including future scheduled
        dates (include_release_dates_with_no_data=true). Paginates at 1000."""
        rows, offset = [], 0
        while True:
            js = self._get_json("releases/dates", realtime_start=start.isoformat(),
                                realtime_end=end.isoformat(),
                                include_release_dates_with_no_data="true",
                                order_by="release_date", sort_order="asc",
                                limit=1000, offset=offset)
            batch = js.get("release_dates", [])
            rows.extend(batch)
            offset += len(batch)
            if not batch or offset >= int(js.get("count", 0)):
                break
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["date", "release_id", "release_name"])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df[(df["date"] >= start) & (df["date"] <= end)].reset_index(drop=True)


@st.cache_data(ttl=FRED_META_TTL, show_spinner=False, max_entries=4)
def fred_metadata_snapshot(series_ids: tuple, key_fp: str, force_after: float,
                           _src: FredSource, _res: dict) -> dict:
    """Ask FRED for each series' `last_updated` stamp. This is the cheap
    'did anything change?' probe that drives release-aware refreshes.
    `key_fp` and `force_after` are cache-key inputs only."""
    breaker = _res["breaker"]
    out: dict[str, dict] = {}
    if breaker.is_open("fred"):
        return {s: {"ok": False, "error": "circuit open"} for s in series_ids}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_src.info, sid): sid for sid in series_ids}
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                out[sid] = {"ok": True, **fut.result()}
                breaker.record("fred", True)
            except Exception as exc:
                out[sid] = {"ok": False, "error": scrub(exc, _src.key)}
                if is_transient(exc):
                    breaker.record("fred", False)
    # A few bad series are cached normally (10-min TTL); only a total failure
    # (network/API down) is treated as degraded and retried on the slow cadence.
    if out and all(not v["ok"] for v in out.values()):
        raise PartialResult(out)
    return out



def get_fred_metadata(src: FredSource, res: dict, force_after: float) -> dict:
    if not src.keyed:
        return {}
    return guarded(res, "fred_meta", lambda: fred_metadata_snapshot(
        tuple(FRED_SIDS), src.key_fp, force_after, _src=src, _res=res))


@st.cache_data(show_spinner="Updating FRED series\u2026", max_entries=6)
def _fred_bundle(token_items: tuple, key_fp: str, force_after: float,
                 _src: FredSource, _res: dict) -> dict:
    """Return {sid: {...}} for every series. The cache key is the tuple of
    (series_id, last_updated) pairs, so this recomputes exactly when FRED
    publishes or revises something. Only changed series hit the network;
    the rest load from the Parquet store."""
    store, breaker = _res["store"], _res["breaker"]
    tokens = dict(token_items)
    to_fetch = []
    for sid, tok in tokens.items():
        dm = store.meta("fred", sid)
        needs = (not dm
                 or (tok and dm.get("token") != tok)
                 or dm.get("saved_epoch", 0) < force_after)
        if needs:
            to_fetch.append(sid)

    errors: dict[str, str] = {}
    if to_fetch and breaker.is_open("fred"):
        errors = {sid: "FRED failing repeatedly; serving stored copy" for sid in to_fetch}
    elif to_fetch:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(_src.observations, sid): sid for sid in to_fetch}
            for fut in as_completed(futures):
                sid = futures[fut]
                try:
                    s = fut.result()
                    if s.empty:
                        raise ValueError("empty series returned")
                    store.save("fred", sid, s.to_frame("value"),
                               {"token": tokens[sid],
                                "via": "FRED API" if _src.keyed else "fredgraph CSV"})
                    breaker.record("fred", True)
                except Exception as exc:
                    errors[sid] = scrub(exc, _src.key)
                    if is_transient(exc):
                        breaker.record("fred", False)

    results = {}
    for sid in tokens:
        df, dm = store.load("fred", sid)
        if df is None or df.empty:
            results[sid] = {"data": None, "status": "failed",
                            "message": errors.get(sid, "no data"), "saved_at": None}
            continue
        status = "stale" if sid in errors else ("updated" if sid in to_fetch else "current")
        results[sid] = {"data": df["value"], "status": status,
                        "message": errors.get(sid, ""), "saved_at": dm.get("saved_at"),
                        "via": dm.get("via", "")}
    if errors:
        raise PartialResult(results)
    return results



def load_fred(src: FredSource, meta: dict, res: dict, force_after: float) -> dict:
    """Release-aware: a series is re-downloaded only when FRED's last_updated
    stamp for it changes (keyless mode falls back to an hourly refresh)."""
    if src.keyed:
        tokens = {sid: (meta.get(sid, {}).get("last_updated") if meta.get(sid, {}).get("ok") else None)
                  for sid in FRED_SIDS}
    else:
        bucket = f"keyless-{int(time.time() // 3600)}"
        tokens = {sid: bucket for sid in FRED_SIDS}
    items = tuple(sorted((k, v or "") for k, v in tokens.items()))
    return guarded(res, "fred", lambda: _fred_bundle(items, src.key_fp, force_after, _src=src, _res=res))


def load_release_map(src: FredSource, res: dict) -> dict:
    """series -> FRED release id/name; stored on disk, refreshed weekly."""
    store = res["store"]
    cached = store.load_json("fred_meta", "release_map")
    now = time.time()
    missing = [s for s in FRED_SIDS if s not in cached or now - cached[s].get("fetched", 0) > RELEASE_MAP_MAX_AGE]
    if missing and src.keyed and not res["breaker"].is_open("fred"):
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(src.series_release, s): s for s in missing}
            for fut in as_completed(futures):
                try:
                    cached[futures[fut]] = {**fut.result(), "fetched": now}
                except Exception as exc:
                    log.warning("release lookup failed: %s", scrub(exc, src.key))
        store.save_json("fred_meta", "release_map", cached)
    return {k: v for k, v in cached.items() if k in FRED_SIDS}


@st.cache_data(ttl=CALENDAR_TTL, show_spinner=False, max_entries=8)
def _calendar(start_iso: str, end_iso: str, key_fp: str, _src: FredSource) -> pd.DataFrame:
    return _src.release_calendar(date.fromisoformat(start_iso), date.fromisoformat(end_iso))


@st.cache_data(show_spinner="Checking US release dates\u2026", max_entries=4)
def _fred_first_release(token_items: tuple, key_fp: str, force_after: float,
                        _src: FredSource, _res: dict) -> dict:
    """First-publication dates for every FRED event series, re-queried only
    when the series' last_updated stamp changes."""
    store = _res["store"]
    tokens = dict(token_items)
    since = (date.today() - timedelta(days=3 * 366)).isoformat()
    todo = [sid for sid, tok in tokens.items()
            if not store.meta("fred_first", sid)
            or (tok and store.meta("fred_first", sid).get("token") != tok)
            or store.meta("fred_first", sid).get("saved_epoch", 0) < force_after]
    errors = {}
    if todo and not _res["breaker"].is_open("fred"):
        with ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(_src.first_release_dates, sid, since): sid for sid in todo}
            for fut in as_completed(futs):
                sid = futs[fut]
                try:
                    store.save("fred_first", sid, fut.result().to_frame("released"), {"token": tokens[sid]})
                except Exception as exc:
                    errors[sid] = scrub(exc, _src.key)
    out = {}
    for sid in tokens:
        df, _ = store.load("fred_first", sid)
        if df is not None and not df.empty:
            out[sid] = df["released"]
    if errors:
        raise PartialResult(out)
    return out



def load_fred_first_release(src: FredSource, meta: dict, res: dict, force_after: float) -> dict:
    if not src.keyed:
        return {}
    items = tuple((s, str(meta.get(s, {}).get("last_updated") or "")) for s in FRED_SIDS)
    return guarded(res, "fred_first", lambda: _fred_first_release(items, src.key_fp, force_after,
                                                                   _src=src, _res=res))


# =============================================================================
# OTHER COUNTRIES
# =============================================================================

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
ONS_SITE = "https://www.ons.gov.uk"
STATCAN_API = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods"
ABS_API = "https://data.api.abs.gov.au/rest/data/ABS,"
DBNOMICS_API = "https://api.db.nomics.world/v22/series/"
SINGSTAT_API = "https://tablebuilder.singstat.gov.sg/api/table/tabledata/"
ECOS_API = "https://ecos.bok.or.kr/api/StatisticSearch/"


def parse_period(p) -> pd.Timestamp:
    """'2026-09', '2026-Q2', '2026 AUG', '2026 Aug', '202608', '2026-09-01' -> period start."""
    p = str(p).strip()
    if re.fullmatch(r"\d{4}[- ]?Q[1-4]", p):
        return pd.Period(p.replace("-", "").replace(" ", ""), freq="Q").start_time
    if re.fullmatch(r"\d{4} [A-Za-z]{3}", p):
        return pd.to_datetime(p.title(), format="%Y %b")
    if re.fullmatch(r"\d{6}", p):
        return pd.Timestamp(f"{p[:4]}-{p[4:]}-01")
    if re.fullmatch(r"\d{4}-\d{2}", p):
        return pd.Timestamp(p + "-01")
    return pd.Timestamp(p)


def _frame(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["period", "value", "flag", "released"]).dropna(subset=["value"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"]).drop_duplicates("period", keep="last")
    return df.set_index("period").sort_index()


def fetch_eurostat(ref: str) -> dict:
    """JSON-stat 2.0. Every non-time dimension is pinned to one value, so the
    flat value index equals the time position. Eurostat gives no per-
    observation release date, only the dataset's dissemination timestamp."""
    ds, query = ref.split("|")
    params = dict(kv.split("=", 1) for kv in query.split(","))
    params["lastTimePeriod"] = "40"
    r = requests.get(EUROSTAT_API + ds, params=params, timeout=45)
    r.raise_for_status()
    js = r.json()
    for dim, size in zip(js["id"], js["size"]):
        if dim != "time" and size != 1:
            raise ValueError(f"Eurostat filter for {ds} not unique on '{dim}'")
    tindex = js["dimension"]["time"]["category"]["index"]
    values, status = js.get("value", {}), js.get("status", {})
    rows = [(parse_period(per), values.get(str(pos)), status.get(str(pos), ""))
            for per, pos in tindex.items()]
    df = pd.DataFrame(rows, columns=["period", "value", "flag"]).dropna(subset=["value"])
    ann = {a.get("type"): a.get("date") for a in js.get("extension", {}).get("annotation", [])}
    stamp = ann.get("DISSEMINATION_TIMESTAMP_DATA") or js.get("updated")
    df["released"] = pd.NaT
    return {"data": df.set_index("period").sort_index(),
            "dataset_released": pd.Timestamp(stamp).tz_convert("UTC").tz_localize(None) if stamp else None,
            "next": None}


def fetch_ons(path: str) -> dict:
    """ONS timeseries JSON: per-observation updateDate (= release date of the
    latest print) and the series' next scheduled release."""
    r = requests.get(ONS_SITE + path + "/data", timeout=45)
    r.raise_for_status()
    if "json" not in r.headers.get("content-type", ""):
        raise RuntimeError("ONS returned non-JSON")
    js = r.json()
    obs = js.get("months") or js.get("quarters") or []
    rows = []
    for o in obs:
        try:
            val = float(o["value"])
        except (TypeError, ValueError):
            continue
        upd = pd.to_datetime(o.get("updateDate"), utc=True, errors="coerce")
        rel = upd.tz_convert("Europe/London").tz_localize(None).normalize() if pd.notna(upd) else pd.NaT
        rows.append((parse_period(o["date"]), val, "", rel))
    df = pd.DataFrame(rows, columns=["period", "value", "flag", "released"]).set_index("period").sort_index()
    nxt = pd.to_datetime(js.get("description", {}).get("nextRelease"), format="%d %B %Y", errors="coerce")
    return {"data": df, "dataset_released": None, "next": None if pd.isna(nxt) else nxt.date()}


def fetch_statcan(vectors: list[str]) -> dict[str, dict]:
    """One POST for all Canadian vectors; each datapoint carries releaseTime (ET)."""
    body = [{"vectorId": int(v), "latestN": 30} for v in vectors]
    r = requests.post(STATCAN_API, json=body, timeout=45)
    r.raise_for_status()
    out = {}
    for item in r.json():
        obj = item.get("object") or {}
        vid = str(obj.get("vectorId"))
        if item.get("status") != "SUCCESS":
            out[vid] = {"error": f"StatCan status {item.get('status')}"}
            continue
        rows = [(parse_period(pt["refPer"]), pt["value"], "",
                 pd.to_datetime(pt.get("releaseTime"), errors="coerce"))
                for pt in obj.get("vectorDataPoint", []) if pt.get("value") is not None]
        df = pd.DataFrame(rows, columns=["period", "value", "flag", "released"]).set_index("period").sort_index()
        out[vid] = {"data": df, "dataset_released": None, "next": None}
    return out



def fetch_abs(ref: str) -> dict:
    """ABS Data API (SDMX, CSV). No release timestamps -> dated 'first seen'."""
    flow, key = ref.split("|")
    start = (date.today() - timedelta(days=3 * 366)).strftime("%Y-%m")
    r = requests.get(f"{ABS_API}{flow}/{key}", params={"startPeriod": start, "format": "csvfilewithlabels"},
                     timeout=90, headers=UA)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    rows = [(parse_period(p), v, "", pd.NaT) for p, v in zip(df["TIME_PERIOD"], df["OBS_VALUE"])]
    return {"data": _frame(rows), "dataset_released": None, "next": None}


def fetch_dbnomics(ref: str) -> dict:
    """DBnomics mirror (CEPREMAP/Banque de France). No official release date."""
    r = requests.get(DBNOMICS_API + ref, params={"observations": 1}, timeout=45, headers=UA)
    r.raise_for_status()
    doc = r.json()["series"]["docs"][0]
    rows = [(parse_period(p), v if v != "NA" else None, "", pd.NaT)
            for p, v in list(zip(doc["period"], doc["value"]))[-60:]]
    return {"data": _frame(rows), "dataset_released": None, "next": None}


def fetch_singstat(ref: str) -> dict:
    """SingStat TableBuilder. Table-level 'dataLastUpdated' -> plausibility-checked date.
    Note: the API returns 403 to Python's default User-Agent; a descriptive one works."""
    table, row_text = ref.split("|")
    r = requests.get(SINGSTAT_API + table, timeout=45, headers=UA)
    r.raise_for_status()
    data = r.json()["Data"]
    row = next(x for x in data["row"] if x["rowText"] == row_text)
    rows = [(parse_period(c["key"]), c["value"], "", pd.NaT) for c in row["columns"][-60:]]
    stamp = pd.to_datetime(data.get("dataLastUpdated"), format="%d/%m/%Y", errors="coerce")
    return {"data": _frame(rows), "dataset_released": None if pd.isna(stamp) else stamp, "next": None}


def fetch_ecos(ref: str, key: str) -> dict:
    """Bank of Korea ECOS. The public 'sample' key returns at most 10 rows per
    request, so the last 26 months are fetched in 9-month windows (works the
    same with a personal key)."""
    parts = ref.split("|")
    stat, items = parts[0], "/".join(parts[1:])
    end = pd.Timestamp(date.today()).to_period("M")
    rows = []
    for i in range(3):
        hi = end - 9 * i
        lo = hi - 8
        url = f"{ECOS_API}{key}/json/en/1/10/{stat}/M/{lo.strftime('%Y%m')}/{hi.strftime('%Y%m')}/{items}"
        r = requests.get(url, timeout=45, headers=UA)
        r.raise_for_status()
        js = r.json()
        if "StatisticSearch" not in js:
            res_ = js.get("RESULT", {})
            if res_.get("CODE") == "INFO-200":   # no data in this window
                continue
            raise RuntimeError(f"ECOS: {res_.get('CODE')} {res_.get('MESSAGE', '')}".strip())
        rows += [(parse_period(x["TIME"]), x["DATA_VALUE"], "", pd.NaT) for x in js["StatisticSearch"]["row"]]
    if not rows:
        raise RuntimeError("ECOS returned no data")
    return {"data": _frame(rows), "dataset_released": None, "next": None}


DATING = {"ons": "official", "statcan": "official", "eurostat": "approx.", "singstat": "approx.",
          "abs": "first seen", "dbnomics": "first seen", "ecos": "first seen"}


@st.cache_data(ttl=INTL_TTL, show_spinner="Updating international figures\u2026", max_entries=4)
def _intl_events(keys: tuple, force_after: float, ecos_fp: str, _ecos_key: str, _res: dict) -> dict:
    store = _res["store"]
    evs = [EVENT[k] for k in keys]
    fetched, errors = {}, {}
    fetchers = {"eurostat": fetch_eurostat, "ons": fetch_ons, "abs": fetch_abs,
                "dbnomics": fetch_dbnomics, "singstat": fetch_singstat,
                "ecos": lambda ref: fetch_ecos(ref, _ecos_key)}
    refs = {(e.source, e.ref) for e in evs if e.source in fetchers}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(call_with_retry, fetchers[s], ref, tries=2): (s, ref) for s, ref in refs}
        for fut in as_completed(futs):
            _, ref = futs[fut]
            try:
                fetched[ref] = fut.result()
            except Exception as exc:
                errors[ref] = scrub(exc, _ecos_key if _ecos_key != "sample" else None)
    can = sorted({e.ref for e in evs if e.source == "statcan"})
    if can:
        try:
            for vid, r_ in call_with_retry(fetch_statcan, can, tries=2).items():
                (errors.__setitem__(vid, r_["error"]) if "error" in r_ else fetched.__setitem__(vid, r_))
        except Exception as exc:
            errors.update({v: scrub(exc) for v in can})

    today = pd.Timestamp(date.today())
    out = {}
    for ref in {e.ref for e in evs}:
        srcname = next(e.source for e in evs if e.ref == ref)
        prev, prev_meta = store.load("events", ref)
        prev_rel = prev["released"] if (prev is not None and "released" in prev) else None
        if ref in fetched:
            f = fetched[ref]
            df = f["data"].copy()
            mode = DATING[srcname]
            if mode != "official":
                # Keep dates already assigned; date only NEW periods.
                max_lag = next((e.max_lag_days for e in evs if e.ref == ref), None)
                freq = infer_freq(df["value"])
                stamp = f["dataset_released"].normalize() if f["dataset_released"] is not None else None
                dates = []
                for per in df.index:
                    if prev_rel is not None and per in prev_rel.index:
                        dates.append(prev_rel.get(per)); continue
                    if mode == "approx." and stamp is not None:
                        end = per + (pd.offsets.QuarterEnd(0) if freq == "Q" else pd.offsets.MonthEnd(0))
                        lag = (stamp - end).days
                        ok = stamp.weekday() < 5 and max_lag is not None and -3 <= lag <= max_lag
                        dates.append(stamp if ok else pd.NaT)
                    elif mode == "first seen" and prev is not None:
                        dates.append(today)      # new period appeared since our last fetch
                    else:
                        dates.append(pd.NaT)     # first ever fetch: no basis for a date
                df["released"] = pd.to_datetime(pd.Series(dates, index=df.index))
            store.save("events", ref, df, {"next": str(f["next"]) if f["next"] else ""})
            out[ref] = {"data": df, "next": f["next"], "status": "updated", "message": ""}
        elif prev is not None:
            nxt = pd.to_datetime(prev_meta.get("next") or None, errors="coerce")
            out[ref] = {"data": prev, "next": None if pd.isna(nxt) else nxt.date(),
                        "status": "stale", "message": errors.get(ref, "")}
        else:
            out[ref] = {"data": None, "next": None, "status": "failed", "message": errors.get(ref, "no data")}
    payload = {"by_ref": out, "errors": errors}
    if errors:
        raise PartialResult(payload)
    return payload


def load_intl_events(res: dict, force_after: float, ecos_key: str) -> dict:
    keys = tuple(e.key for e in EVENTS if e.source != "fred")
    fp = hashlib.sha256(ecos_key.encode()).hexdigest()[:10]
    return guarded(res, "intl_events", lambda: _intl_events(keys, force_after, fp, ecos_key, _res=res))


# =============================================================================
# CALCULATIONS
# =============================================================================

PERIODS_PER_YEAR = {"D": 252, "W": 52, "M": 12, "Q": 4, "A": 1}


def infer_freq(s: pd.Series) -> str:
    if len(s) < 3:
        return "?"
    gap = s.index.to_series().diff().dt.days.median()
    if gap <= 4:
        return "D"
    if gap <= 10:
        return "W"
    if gap <= 40:
        return "M"
    if gap <= 120:
        return "Q"
    return "A"


def lagged(s: pd.Series, freq: str, periods: int) -> pd.Series:
    """Value `periods` periods earlier, aligned BY DATE, not by row position.
    Row-based shift() silently breaks when an observation is missing - e.g.
    FRED has no October-2025 CPIAUCSL/UNRATE (BLS did not collect them during
    the federal shutdown), so shift(12) would compare against a 13-month-old
    value for the following year."""
    if freq in ("M", "Q", "A"):
        months = {"M": 1, "Q": 3, "A": 12}[freq] * periods
        prior_idx = s.index - pd.DateOffset(months=months)
        return pd.Series(s.reindex(prior_idx).to_numpy(), index=s.index)
    return s.shift(periods)


def week_bounds(d: date) -> tuple[date, date]:
    start = d - timedelta(days=d.weekday())
    return start, start + timedelta(days=6)


def fmt_period(ts: pd.Timestamp, freq: str) -> str:
    if freq == "M":
        return ts.strftime("%b %Y")
    if freq == "Q":
        return f"Q{(ts.month - 1) // 3 + 1} {ts.year}"
    if freq == "W":
        return f"w/e {ts.strftime('%d %b %Y')}"
    if freq == "A":
        return str(ts.year)
    return ts.strftime("%d %b %Y")


def compute_event(ev: Event, base: pd.Series, released: pd.Series | None,
                  fallback_released: pd.Timestamp | None) -> dict:
    """Latest print, previous print, and when the latest print was published.
    Uses date-aligned lags (see `lagged`), so a missing month yields 'n/a'
    instead of a silently wrong comparison."""
    out = {"actual": np.nan, "previous": np.nan, "period": None, "freq": "?", "released": pd.NaT}
    s = base.dropna().astype(float) * ev.scale
    if len(s) < 2:
        return out
    f = infer_freq(s)
    out["freq"] = f
    if ev.calc == "on_change":
        changed = s[s.diff().fillna(0) != 0]
        when = changed.index[-1] if len(changed) else s.index[0]
        before = s[s.index < when]
        out.update(actual=s.loc[when], previous=before.iloc[-1] if len(before) else np.nan,
                   period=when, freq="D", released=when)
        return out
    if ev.calc == "level":
        t = s
    elif ev.calc == "mom":
        t = (s / lagged(s, f, 1) - 1) * 100
    elif ev.calc == "yoy":
        t = (s / lagged(s, f, PERIODS_PER_YEAR.get(f, 12)) - 1) * 100
    elif ev.calc == "diff":
        t = s - lagged(s, f, 1)
    else:
        raise ValueError(ev.calc)
    t = t.replace([np.inf, -np.inf], np.nan)
    latest = s.index[-1]
    out["period"] = latest
    out["actual"] = t.get(latest, np.nan)
    prior = t[t.index < latest].dropna()
    out["previous"] = prior.iloc[-1] if len(prior) else np.nan
    rel = released.get(latest, pd.NaT) if released is not None else pd.NaT
    out["released"] = rel if pd.notna(rel) else (fallback_released if fallback_released is not None else pd.NaT)
    return out


def fmt_event_value(v: float, ev: Event) -> str:
    if v is None or pd.isna(v):
        return "n/a"
    d = ev.decimals
    if round(v, d) == 0:
        v = 0.0  # avoid "-0.0%"
    return {"%": f"{v:.{d}f}%", "k": f"{v:,.{d}f}K", "m": f"{v:,.{d}f}M",
            "$bn": f"{v:,.1f}B"}.get(ev.unit, f"{v:,.{d}f}")


def fmt_event_change(a: float, p: float, ev: Event) -> tuple[str, str]:
    """(change in the figure's own unit, % change). % change is only shown for
    levels; for rates the meaningful change is in percentage points."""
    if pd.isna(a) or pd.isna(p):
        return "", ""
    # Work from the figures as displayed/published (rounded), so the change
    # always agrees with Actual and Previous (3.4% vs 3.3% -> +0.1 pp).
    dec = 1 if ev.unit == "$bn" else ev.decimals
    a, p = round(a, dec), round(p, dec)
    d = a - p
    if round(d, max(ev.decimals, 1)) == 0:
        d = 0.0
    if ev.unit == "%":
        chg = f"{d:+.{max(ev.decimals, 1)}f} pp"
    else:
        suffix = {"k": "K", "m": "M", "$bn": "B"}.get(ev.unit, "")
        chg = f"{d:+,.{ev.decimals if ev.unit != '$bn' else 1}f}{suffix}"
    pct = f"{(a / p - 1) * 100:+.1f}%" if (ev.change == "pct" and p not in (0, 0.0)) else "\u2014"
    return chg, pct


def stars(n: int) -> str:
    return "\u2605" * n + "\u2606" * (3 - n)



def build_event_table(fred_raw: dict, first_rel: dict, intl: dict, fred_meta: dict) -> pd.DataFrame:
    """One row per indicator: its latest official print and when it was published."""
    rows = []
    by_ref = intl.get("by_ref", {})
    for ev in EVENTS:
        df, nxt, flag = None, None, ""
        if ev.source == "fred":
            base = fred_raw.get(ev.ref)
            rel = first_rel.get(ev.ref)
            lu = pd.to_datetime(fred_meta.get(ev.ref, {}).get("last_updated"), utc=True, errors="coerce")
            fb = lu.tz_convert(MARKET_TZ).tz_localize(None).normalize() if pd.notna(lu) else None
            basis = "official" if rel is not None else ("FRED update" if fb is not None else "")
            status, msg = ("ok", "") if base is not None else ("failed", "FRED series unavailable")
        else:
            r = by_ref.get(ev.ref, {})
            df = r.get("data")
            base = df["value"] if df is not None else None
            rel = df["released"] if df is not None else None
            fb, nxt = None, r.get("next")
            basis = DATING[ev.source]
            status, msg = r.get("status", "failed"), r.get("message", "")
        res_ = (compute_event(ev, base, rel, fb) if base is not None else
                {"actual": np.nan, "previous": np.nan, "period": None, "freq": "?", "released": pd.NaT})
        if ev.source == "eurostat" and df is not None and res_["period"] is not None:
            fl = df["flag"].get(res_["period"], "")
            flag = {"e": "flash/estimate", "p": "provisional"}.get(fl, fl or "")
        chg, pct = fmt_event_change(res_["actual"], res_["previous"], ev)
        period_txt = fmt_period(res_["period"], res_["freq"]) if res_["period"] is not None else ""
        released = pd.Timestamp(res_["released"]).normalize() if pd.notna(res_["released"]) else pd.NaT
        b = base.dropna() if base is not None else pd.Series(dtype=float)
        rows.append({
            "key": ev.key, "country": ev.country, "Code": CODE[ev.country],
            "Event": f"{ev.name} ({period_txt})" if period_txt else ev.name, "name": ev.name,
            "importance": ev.importance, "Imp.": stars(ev.importance), "Period": period_txt,
            "Actual": fmt_event_value(res_["actual"], ev), "Previous": fmt_event_value(res_["previous"], ev),
            "Change": chg, "% change": pct, "released": released,
            "Date basis": basis if pd.notna(released) else "", "Next release": nxt, "Flag": flag,
            "source": ev.source, "status": status, "message": msg, "Notes": ev.note,
            "last_obs": b.index[-1] if len(b) else pd.NaT,
            "current": fmt_event_value(b.iloc[-1] * ev.scale, ev) if len(b) else "n/a",
        })
    return pd.DataFrame(rows)


def build_week_calendar(events: pd.DataFrame, w_start: date, w_end: date, today: date,
                        us_cal: pd.DataFrame | None, release_map: dict) -> pd.DataFrame:
    """Released figures dated inside the window + scheduled items
    (US: FRED release calendar; UK: ONS next-release dates)."""
    lo, hi = pd.Timestamp(w_start), pd.Timestamp(w_end)
    rel = events[events["released"].between(lo, hi)].copy()
    rel["Date"] = rel["released"]
    rel["Status"] = np.where(rel["Flag"] != "", "Released (" + rel["Flag"] + ")", "Released")
    extra = []
    blank = {"Actual": "", "Change": "", "% change": "", "Date basis": ""}
    if us_cal is not None and not us_cal.empty:
        sid_to_rel = {s: int(v["release_id"]) for s, v in release_map.items() if v.get("release_id")}
        for _, c in us_cal.iterrows():
            rid, day = int(c["release_id"]), c["date"]
            for ev in EVENTS:
                if ev.source != "fred" or sid_to_rel.get(ev.ref) != rid:
                    continue
                row = events[events["key"] == ev.key].iloc[0]
                if pd.notna(row["released"]) and row["released"].date() >= day:
                    continue
                if ev.calc == "on_change" and "FOMC" not in str(c.get("release_name", "")):
                    continue  # daily-published policy-rate series: only actual changes are shown
                if ev.calc == "on_change" and pd.notna(row["last_obs"]) and row["last_obs"].date() >= day:
                    extra.append({**row.to_dict(), "Date": pd.Timestamp(day), "Status": "Released (no change)",
                                  "Event": f"{ev.name} ({day:%d %b %Y})", "Actual": row["current"],
                                  "Previous": row["current"], "Change": "+0.00 pp", "% change": "\u2014"})
                    continue
                extra.append({**row.to_dict(), **blank, "Date": pd.Timestamp(day), "Event": ev.name,
                              "Previous": row["Actual"],
                              "Status": "Scheduled" if day > today else "Due / not yet on FRED"})
    for _, row in events[(events["source"] == "ons") & events["Next release"].notna()].iterrows():
        day = row["Next release"]
        if w_start <= day <= w_end and day >= today:
            extra.append({**row.to_dict(), **blank, "Date": pd.Timestamp(day), "Event": row["name"],
                          "Previous": row["Actual"], "Status": "Scheduled"})
    cal = pd.concat([rel, pd.DataFrame(extra)], ignore_index=True) if extra else rel
    if cal.empty:
        return cal
    order = {c: i for i, c in enumerate(COUNTRIES)}
    cal = cal.drop_duplicates(subset=["key", "Date", "Status"])
    cal["_o"] = cal["country"].map(order)
    return cal.sort_values(["Date", "_o", "importance"], ascending=[True, True, False]).drop(columns="_o")


# =============================================================================
# UI
# =============================================================================

st.set_page_config(page_title="Economic Calendar", page_icon="\U0001f5d3\ufe0f", layout="wide")
res = get_resources()
today_et = datetime.now(MARKET_TZ).date()

with st.sidebar:
    st.header("Settings")
    fred_key, key_src = os.getenv("FRED_API_KEY", "").strip(), "environment variable"
    if not fred_key:
        try:
            fred_key, key_src = str(st.secrets.get("FRED_API_KEY", "")).strip(), "secrets.toml"
        except Exception:
            fred_key = ""
    if not fred_key:
        fred_key, key_src = st.text_input("FRED API key (US data)", type="password").strip(), "sidebar"
    if fred_key and not re.fullmatch(r"[a-z0-9]{32}", fred_key):
        st.error("A FRED key is 32 lowercase letters/digits. Ignoring the value entered.")
        fred_key = ""
    st.caption(f"FRED key loaded from {key_src}." if fred_key else
               "No FRED key: US figures show values but no publication dates.")

    ecos_key = os.getenv("ECOS_API_KEY", "").strip() or "sample"
    st.caption("Korea: " + ("personal ECOS key loaded." if ecos_key != "sample" else
                            "using ECOS's public sample key (limited; a free personal key is recommended)."))

    check_every = st.selectbox("Check for new figures", ["Every 15 min", "Every 30 min", "Off"])
    if st.button("Refresh all data now", type="primary", width="stretch"):
        st.session_state["force_after"] = time.time()
        res["degraded"].clear()
        st.cache_data.clear()
        st.rerun()
    force_after = st.session_state.get("force_after", 0.0)

src = FredSource(fred_key or None, res["fred_limiter"])
fred_meta = get_fred_metadata(src, res, force_after)
fred = load_fred(src, fred_meta, res, force_after)
first_rel = load_fred_first_release(src, fred_meta, res, force_after)
intl = load_intl_events(res, force_after, ecos_key)
raw = {sid: r["data"] for sid, r in fred.items() if r.get("data") is not None}
events = build_event_table(raw, first_rel, intl, fred_meta)

interval = {"Every 15 min": "15m", "Every 30 min": "30m"}.get(check_every)


@st.fragment(run_every=interval)
def watcher() -> None:
    """Re-checks sources on a timer while the page is open; reruns the page
    only if FRED reports a change or the 15-minute refresh window rolls over."""
    meta = get_fred_metadata(src, res, st.session_state.get("force_after", 0.0))
    sig = hashlib.md5(json.dumps([sorted((k, str(v.get("last_updated"))) for k, v in meta.items()),
                                  int(time.time() // INTL_TTL)]).encode()).hexdigest()
    prev = st.session_state.get("sig")
    st.session_state["sig"] = sig
    if prev is not None and sig != prev:
        st.rerun()
    st.caption(f"Last checked {datetime.now(MARKET_TZ):%d %b %Y, %H:%M} ET")


st.title("Economic Calendar")
top_l, top_r = st.columns([3, 1])
with top_l:
    st.caption("Official published figures only \u2014 no forecasts. Previous = prior period as "
               "currently published (including revisions).")
with top_r:
    watcher()

available = [c for c in COUNTRIES if c not in NOT_COVERED]
c1, c2, c3 = st.columns([1.3, 3, 1.2])
with c1:
    which = st.segmented_control("Week", ["Last week", "This week", "Next week"], default="This week",
                                 required=True)
with c2:
    picked = st.multiselect("Countries", available, default=available,
                            format_func=lambda c: f"{CODE[c]} \u00b7 {COUNTRIES[c]}")
with c3:
    min_imp = st.select_slider("Importance", options=[1, 2, 3], value=1, format_func=stars)

shift = {"Last week": -7, "This week": 0, "Next week": 7}[which]
w_start, w_end = week_bounds(today_et + timedelta(days=shift))

us_cal, rel_map = None, {}
if src.keyed:
    try:
        rel_map = load_release_map(src, res)
        us_cal = _calendar(w_start.isoformat(), w_end.isoformat(), src.key_fp, _src=src)
    except Exception as exc:
        st.warning(f"US release schedule unavailable ({scrub(exc, src.key)}).")

st.subheader(f"{w_start:%a %d %b} \u2013 {w_end:%a %d %b %Y}")
cal = build_week_calendar(events, w_start, w_end, today_et, us_cal, rel_map)
if not cal.empty:
    cal = cal[cal["country"].isin(picked) & (cal["importance"] >= min_imp)]
cols = ["Date", "Code", "Event", "Imp.", "Actual", "Previous", "Change", "% change", "Status"]
if cal.empty:
    st.info("No published or scheduled figures for these filters in this week.")
else:
    view = cal[cols].copy()
    view["Date"] = pd.to_datetime(view["Date"]).dt.strftime("%a %d %b")
    st.dataframe(view, hide_index=True, width="stretch", height=min(36 * (len(view) + 1) + 4, 1100),
                 column_config={"Code": st.column_config.TextColumn("", width="small"),
                                "Event": st.column_config.TextColumn("Event (period)", width="large"),
                                "Imp.": st.column_config.TextColumn("Imp.", width="small")})
if not src.keyed and "US" in picked:
    st.caption("US figures are missing from the weekly view until a FRED key is set; their latest "
               "values are in the table below.")

st.subheader("Latest figure for each indicator")
tabs = st.tabs([f"{CODE[c]} \u00b7 {COUNTRIES[c]}" for c in COUNTRIES])
for tab, c in zip(tabs, COUNTRIES):
    with tab:
        if c in NOT_COVERED:
            st.info("Not covered. " + NOT_COVERED[c])
            continue
        t = events[(events["country"] == c) & (events["importance"] >= min_imp)].copy()
        t["Published"] = t["released"].dt.strftime("%a %d %b %Y").fillna("\u2014")
        t["Next release"] = t["Next release"].map(lambda d: d.strftime("%d %b %Y") if d else "")
        t["Source"] = t["source"].map(SOURCE_NAME)
        show = ["Event", "Imp.", "Published", "Date basis", "Actual", "Previous", "Change", "% change"]
        if t["Next release"].astype(bool).any():
            show.append("Next release")
        if t["Flag"].astype(bool).any():
            show.append("Flag")
        show += ["Source", "Notes"]
        st.dataframe(t[show], hide_index=True, width="stretch",
                     column_config={"Event": st.column_config.TextColumn("Event (period)", width="large")})
        bad = t[t["status"].isin(["stale", "failed"])]
        if not bad.empty:
            st.caption("Could not refresh: " + ", ".join(bad["name"]) +
                       ". Showing the last stored figures where available.")

with st.expander("How to read this"):
    st.markdown(
        "- **Event (period)**: the figure and the period it measures, e.g. *CPI (YoY) (Aug 2026)*.\n"
        "- **Published**: the date the figure was released. **Date basis** says how reliable that date is: "
        "*official* = the agency's own timestamp; *approx.* = the agency's dataset-update date, accepted only "
        "on weekdays within the normal publication lag; *first seen* = the source gives no date, so this is "
        "when this app first saw the figure (blank on the first run); *FRED update* = FRED's last-updated "
        "date, used if the exact first-release date could not be fetched.\n"
        "- **Change** is in the figure's own unit. For rates it is in percentage points: CPI moving from "
        "3.3% to 3.4% is **+0.1 pp**, not +3%. **% change** is shown only for levels (e.g. jobless claims, "
        "home sales), where it is meaningful.\n"
        "- **Flash/estimate**: an official early release that will be revised (e.g. euro-area flash CPI).\n"
        "- Not available from free official sources: consensus forecasts, ISM and S&P Global PMIs, "
        "Conference Board confidence, China, New Zealand.")

with st.expander("Data source status"):
    rows = []
    for sid, r in fred.items():
        rows.append({"Source": "FRED", "Series": sid, "Status": r["status"], "Message": r.get("message", "")})
    for ref, r in intl.get("by_ref", {}).items():
        e0 = next(e for e in EVENTS if e.ref == ref)
        rows.append({"Source": SOURCE_NAME[e0.source], "Series": f"{CODE[e0.country]}: {ref}",
                     "Status": r["status"], "Message": r.get("message", "")})
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    st.caption("updated = downloaded now; current = unchanged since last download; stale = download "
               "failed, last stored copy shown; failed = nothing available.")
    st.download_button("Download all latest figures (CSV)",
                       lambda: events[["Code", "name", "Period", "Actual", "Previous", "Change", "% change",
                                       "released", "Date basis", "source", "Notes"]]
                       .assign(released=events["released"].dt.date).to_csv(index=False).encode(),
                       file_name=f"latest_figures_{today_et:%Y%m%d}.csv", mime="text/csv", on_click="ignore")
