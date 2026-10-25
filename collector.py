"""Binance spot USDT juftliklari bo'yicha soatlik shamlarni yig'uvchi robot.

Har soatda GitHub Actions ishga tushiradi: barcha faol USDT juftliklarining
oxirgi yopilgan soatlik shamlari data/YYYY-MM/YYYY-MM-DD.csv fayllariga yoziladi
(sana UTC bo'yicha). Qo'lda ishga tushirilganda --backfill N oldingi N kunni to'ldiradi.

Ma'lumot manbai: data-api.binance.vision (Binance'ning ochiq bozor ma'lumotlari,
kalit talab qilinmaydi).
"""
import argparse
import csv
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

BASE = "https://data-api.binance.vision/api/v3"
DATA_DIR = "data"
HOUR_MS = 3_600_000
FIELDS = ["symbol", "open_time", "open", "high", "low", "close", "volume", "quote_volume", "trades"]
LEVERAGED = re.compile(r"(UP|DOWN|BULL|BEAR)USDT$")
STABLE = {"USDCUSDT", "TUSDUSDT", "FDUSDUSDT", "USDPUSDT", "USDEUSDT", "USD1USDT", "DAIUSDT",
          "EURUSDT", "EURIUSDT", "AEURUSDT", "GBPUSDT", "AUDUSDT", "PAXGUSDT", "XAUTUSDT",
          "WBTCUSDT", "WBETHUSDT", "BFUSDUSDT", "RLUSDUSDT", "XUSDUSDT", "USDSUSDT", "FRAXUSDT"}

session = requests.Session()
session.headers["User-Agent"] = "binance-data-collector/1.0"


def get(path, params, tries=4):
    for i in range(tries):
        r = session.get(f"{BASE}/{path}", params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (418, 429):  # juda ko'p so'rov: kutamiz
            time.sleep(int(r.headers.get("Retry-After", "10")))
            continue
        if i == tries - 1:
            r.raise_for_status()
        time.sleep(2 * (i + 1))
    return None


def symbols():
    info = get("exchangeInfo", {"permissions": "SPOT", "symbolStatus": "TRADING"})
    out = []
    for s in info["symbols"]:
        sym = s["symbol"]
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        if LEVERAGED.search(sym) or sym in STABLE:
            continue
        out.append(sym)
    return sorted(out)


def klines(sym, start_ms=None, end_ms=None, limit=3):
    params = {"symbol": sym, "interval": "1h", "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    if end_ms is not None:
        params["endTime"] = end_ms
    rows = get("klines", params) or []
    now = int(time.time() * 1000)
    out = []
    for k in rows:
        if k[6] >= now:  # hali yopilmagan sham
            continue
        out.append({"symbol": sym, "open_time": k[0], "open": k[1], "high": k[2], "low": k[3],
                    "close": k[4], "volume": k[5], "quote_volume": k[7], "trades": k[8]})
    return out


def day_file(open_time):
    d = datetime.fromtimestamp(open_time / 1000, tz=timezone.utc)
    folder = os.path.join(DATA_DIR, d.strftime("%Y-%m"))
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, d.strftime("%Y-%m-%d") + ".csv")


def save(rows):
    by_file = {}
    for r in rows:
        by_file.setdefault(day_file(r["open_time"]), []).append(r)
    added = 0
    for path, new in by_file.items():
        seen = set()
        if os.path.exists(path):
            with open(path, newline="") as f:
                for r in csv.DictReader(f):
                    seen.add((r["symbol"], int(r["open_time"])))
        fresh = [r for r in new if (r["symbol"], int(r["open_time"])) not in seen]
        if not fresh:
            continue
        write_header = not os.path.exists(path)
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            if write_header:
                w.writeheader()
            for r in sorted(fresh, key=lambda x: (x["open_time"], x["symbol"])):
                w.writerow(r)
        added += len(fresh)
    return added


def run_hourly(syms):
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(lambda s: klines(s, limit=3), syms))
    return [r for rows in results for r in rows]


def run_backfill(syms, days):
    end = int(time.time() * 1000)
    start = end - days * 24 * HOUR_MS

    def one(sym):
        out, cur = [], start
        while cur < end:
            rows = klines(sym, start_ms=cur, limit=1000)
            if not rows:
                break
            out.extend(rows)
            cur = rows[-1]["open_time"] + HOUR_MS
            if len(rows) < 1000:
                break
        return out

    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(one, syms))
    return [r for rows in results for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", type=int, default=0, help="oldingi necha kunni to'ldirish")
    a = ap.parse_args()
    syms = symbols()
    print(f"Faol USDT juftliklari: {len(syms)}")
    rows = run_backfill(syms, a.backfill) if a.backfill > 0 else run_hourly(syms)
    added = save(rows)
    print(f"Olingan shamlar: {len(rows)}, yangi yozilgan: {added}")
    with open(os.path.join(DATA_DIR, "symbols.txt"), "w") as f:
        f.write("\n".join(syms) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
