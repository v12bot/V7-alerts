#!/usr/bin/env python3
"""Gold session briefing -> ntfy. Phase 1: levels only. No trade signals.

One short card before the New York session: prior trading day, Asia and
London highs/lows, the 8:00-8:15 ET opening range, session VWAP, sorted by
price with the distance from the current price. Plus a Sunday week-open
card with the last full session's levels.

Modes (MODE env var):
  briefing   (default) build and send the card, inside the time window
  feedcheck            compare every feed's price, so you can set PRICE_OFFSET
  selftest             verify the maths on synthetic candles, send nothing

Feeds, tried in order. The first one that works prices the card; the card
always names the feed, so a fallback never passes silently.
  1. TradeLocker demo - your execution price (only if demo secrets are set)
  2. OANDA practice   - XAU/USD spot, the market reference
  3. Binance XAUUSDT perpetual, 4. Bybit XAUUSDT perpetual,
     5. Binance PAXGUSDT spot - fallbacks only, labelled as such

Secrets: NTFY_TOPIC (required - treat the topic name like a password,
anyone who knows it can read your alerts), OANDA_API_TOKEN,
TRADELOCKER_DEMO_EMAIL / PASSWORD / SERVER (optional, demo only, never
live). Optional repo variables: PRICE_OFFSET (chart price minus feed
price). The card shows R multiples and point values only — no dollar
amounts, because the lot size varies trade to trade.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ============================ SETTINGS ============================
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").strip().rstrip("/")
NTFY_TOKEN = os.environ.get("NTFY_TOKEN", "").strip()

# Added to every feed price so levels match your chart. Run feedcheck mode,
# compare with your chart, then set this as a repo variable.
PRICE_OFFSET = float(os.environ.get("PRICE_OFFSET", "0.0") or 0.0)

# No alert repeats: the card is sent once. Repeating the BEST line did
# not make the phone beep for 3-5 seconds — the notification sound length
# is the phone's sound file, not something the script controls. If a
# longer alert sound is wanted, set a longer notification sound in the
# ntfy app on the phone.
# ORB filter (range of the first 15m candle after 8:00 ET, in points)
ORB_MIN_RANGE = 4.0
ORB_MAX_RANGE = 15.0

# A trading day starts at this hour (ET). Gold reopens at 18:00 ET.
DAY_START_HOUR_ET = 18

# When a scheduled run may send (minutes after midnight, ET). GitHub can
# delay scheduled runs, so the windows are wide - but they must stay clear
# of the wrong-season cron (07:25 / 09:25 ET and 16:37 / 18:37 ET Sunday).
NY_WINDOW = (7 * 60 + 30, 9 * 60 + 20)
SUNDAY_WINDOW = (17 * 60 + 10, 18 * 60 + 5)

MIN_CANDLES = 150
CANDLE_MS = 15 * 60 * 1000
STALE_MINUTES_NY = 60  # newest complete candle must be this fresh (NY only)

OANDA_API_TOKEN = os.environ.get("OANDA_API_TOKEN", "").strip()
OANDA_HOST = "https://api-fxpractice.oanda.com"
TL_EMAIL = os.environ.get("TRADELOCKER_DEMO_EMAIL", "").strip()
TL_PASSWORD = os.environ.get("TRADELOCKER_DEMO_PASSWORD", "").strip()
TL_SERVER = os.environ.get("TRADELOCKER_DEMO_SERVER", "").strip()

MODE = os.environ.get("MODE", "briefing").strip().lower() or "briefing"
FORCE = os.environ.get("FORCE", "0").strip() == "1"

TRADINGVIEW_CHART = "https://www.tradingview.com/chart/?symbol=OANDA%3AXAUUSD"


# ============================ DATA ============================
def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "gold-briefing/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_oanda_time(s):
    """OANDA timestamps carry 9-digit fractions; trim to 6 for fromisoformat."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        head, rest = s.split(".", 1)
        frac, off = "", ""
        for i, ch in enumerate(rest):
            if ch in "+-":
                off, frac = rest[i:], rest[:i]
                break
        else:
            frac = rest
        frac = (frac + "000000")[:6]
        s = head + "." + frac + off
    return datetime.fromisoformat(s)


def fetch_oanda():
    """XAU/USD M15 mid candles from OANDA practice. Returns (candles, live, err)."""
    url = (f"{OANDA_HOST}/v3/instruments/XAU_USD/candles"
           "?granularity=M15&count=500&price=M")
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {OANDA_API_TOKEN}",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.load(resp)
    candles, live = [], None
    for c in data.get("candles", []):
        mid = c.get("mid", {})
        t_ms = int(_parse_oanda_time(c["time"]).timestamp() * 1000)
        if c.get("complete"):
            candles.append({
                "t": t_ms, "o": float(mid["o"]), "h": float(mid["h"]),
                "l": float(mid["l"]), "c": float(mid["c"]),
                "v": float(c.get("volume", 0)),
            })
        else:
            live = float(mid["c"])  # forming candle: nearest thing to live
    return candles, live, None


def fetch_binance(url):
    """Binance klines (perp or spot). Returns (candles, live, err)."""
    data = http_get(url)
    if isinstance(data, dict):
        return None, None, str(data.get("msg") or data.get("code") or "bad reply")
    rows = sorted(data, key=lambda r: int(r[0]))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    candles, live = [], None
    for r in rows:
        row = {"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
               "l": float(r[3]), "c": float(r[4]), "v": float(r[5])}
        if int(r[6]) <= now_ms:
            candles.append(row)
        else:
            live = row["c"]
    return candles, live, None


def fetch_bybit(url):
    """Bybit v5 kline (linear perp). Returns (candles, live, err)."""
    data = http_get(url)
    if data.get("retCode") != 0:
        return None, None, "retCode %s" % data.get("retCode")
    rows = sorted(data["result"]["list"], key=lambda r: int(r[0]))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    candles, live = [], None
    for r in rows:
        t = int(r[0])
        row = {"t": t, "o": float(r[1]), "h": float(r[2]),
               "l": float(r[3]), "c": float(r[4]), "v": float(r[5])}
        if t + CANDLE_MS <= now_ms:
            candles.append(row)
        else:
            live = row["c"]
    return candles, live, None


def _coerce_time(v):
    """TradeLocker time may be epoch seconds/ms or an ISO string."""
    if isinstance(v, (int, float)):
        if v > 1e12:  # milliseconds
            v = v / 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc).astimezone(ET)
    if isinstance(v, str):
        return _parse_oanda_time(v).astimezone(ET)
    raise ValueError(f"unrecognised time value: {v!r}")


def _pick(row, *names, default=None):
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default


def fetch_tradelocker():
    """XAUUSD M15 candles from TradeLocker demo. UNVERIFIED against the live
    API: method names come from the official library's README, but the
    resolution string and returned candle shape are unconfirmed."""
    from tradelocker import TLAPI  # lazy: only needed when this feed is on

    tl = TLAPI(environment="https://demo.tradelocker.com",
                username=TL_EMAIL, password=TL_PASSWORD, server=TL_SERVER)
    instrument_id = tl.get_instrument_id_from_symbol_name("XAUUSD")
    hist = tl.get_price_history(instrument_id, resolution="15m",
                                start_timestamp=0, end_timestamp=0,
                                lookback_period="5D")
    candles = _tl_rows_to_candles(hist)
    live = float(tl.get_latest_asking_price(instrument_id))
    return candles, live, None


def _run_fetcher(name, fallback, fn):
    try:
        candles, live, err = fn()
    except Exception as e:  # noqa: BLE001 - report, don't crash the run
        candles, live, err = None, None, "%s: %s" % (type(e).__name__, e)
    if err is None and candles is not None and len(candles) < MIN_CANDLES:
        err = "only %d candles" % len(candles)
        candles = None
    return {"name": name, "fallback": fallback,
            "candles": candles, "live": live, "error": err}


def fetch_all_feeds():
    """Fetch every configured feed. Order is the preference order."""
    feeds = []
    if TL_EMAIL and TL_PASSWORD and TL_SERVER:
        feeds.append(_run_fetcher("TradeLocker demo", False, fetch_tradelocker))
    else:
        feeds.append({"name": "TradeLocker demo", "fallback": False,
                      "candles": None, "live": None,
                      "error": "demo credentials / server not set"})
    if OANDA_API_TOKEN:
        feeds.append(_run_fetcher("OANDA XAU/USD", False, fetch_oanda))
    else:
        feeds.append({"name": "OANDA XAU/USD", "fallback": False,
                      "candles": None, "live": None,
                      "error": "OANDA_API_TOKEN not set"})
    feeds.append(_run_fetcher(
        "Binance perp XAUUSDT", True,
        lambda: fetch_binance(
            "https://fapi.binance.com/fapi/v1/klines?symbol=XAUUSDT&interval=15m&limit=500")))
    feeds.append(_run_fetcher(
        "Bybit perp XAUUSDT", True,
        lambda: fetch_bybit(
            "https://api.bybit.com/v5/market/kline?category=linear&symbol=XAUUSDT&interval=15&limit=500")))
    feeds.append(_run_fetcher(
        "Binance spot PAXGUSDT", True,
        lambda: fetch_binance(
            "https://data-api.binance.vision/api/v3/klines?symbol=PAXGUSDT&interval=15m&limit=500")))
    return feeds


def price_of(result):
    if result["live"] is not None:
        return result["live"]
    if result["candles"]:
        return result["candles"][-1]["c"]
    return None


def is_stale(candles, now_ms):
    last_close_ms = candles[-1]["t"] + CANDLE_MS
    return (now_ms - last_close_ms) > STALE_MINUTES_NY * 60 * 1000


def pick_active(results, kind, now_ms):
    """First feed with enough fresh candles. Stale NY feeds are skipped."""
    for r in results:
        if r["candles"] is None:
            continue
        if kind == "NY" and is_stale(r["candles"], now_ms):
            last = datetime.fromtimestamp(
                (r["candles"][-1]["t"] + CANDLE_MS) / 1000, tz=timezone.utc)
            r["error"] = "stale: last candle closed %s UTC" % last.strftime("%H:%M")
            continue
        return r
    return None


def gap_line(results):
    """TradeLocker vs OANDA difference, when both answered."""
    tl = next((r for r in results if r["name"].startswith("TradeLocker")), None)
    oa = next((r for r in results if r["name"].startswith("OANDA")), None)
    if tl is None or oa is None:
        return ""
    pt, po = price_of(tl), price_of(oa)
    if pt is None or po is None:
        return ""
    return "TradeLocker vs OANDA: %+.2f" % (pt - po)


# ============================ LEVELS ============================
def to_ms(dt):
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def hi_lo(candles, start, end):
    s, e = to_ms(start), to_ms(end)
    sel = [c for c in candles if s <= c["t"] < e]
    if not sel:
        return None
    return max(c["h"] for c in sel), min(c["l"] for c in sel)


def prior_day_window(now):
    """Previous full trading day, skipping blocks when spot gold is closed.

    A block starts at 18:00 ET. Blocks that start on Friday or Saturday are
    closed, so they are skipped (Monday morning shows the Friday session).
    """
    end = now.replace(hour=DAY_START_HOUR_ET, minute=0, second=0, microsecond=0)
    if now < end:
        end -= timedelta(days=1)
    begin = end - timedelta(days=1)
    for _ in range(7):
        if begin.weekday() not in (4, 5):
            return begin, end
        end = begin
        begin = begin - timedelta(days=1)
    return begin, end


def session_vwap(candles, start):
    s = to_ms(start)
    num = den = 0.0
    for c in candles:
        if c["t"] >= s:
            tp = (c["h"] + c["l"] + c["c"]) / 3.0
            num += tp * c.get("v", 0)
            den += c.get("v", 0)
    return num / den if den > 0 else None


def build_levels(candles, now, kind):
    """Return (items, orb, partial_london). items are (price, label) before offset."""
    items = []
    orb = None
    partial = False

    pd_begin, pd_end = prior_day_window(now)
    r = hi_lo(candles, pd_begin, pd_end)
    if r:
        items += [(r[0], "PDH"), (r[1], "PDL")]

    if kind == "NY":
        d03 = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if now < d03:
            d03 -= timedelta(days=1)
        asia_start = (d03 - timedelta(days=1)).replace(hour=19)
        r = hi_lo(candles, asia_start, d03)
        if r:
            items += [(r[0], "ASIA H"), (r[1], "ASIA L")]

        d08 = d03.replace(hour=8)
        london_end = min(now, d08)
        partial = now < d08
        r = hi_lo(candles, d03, london_end)
        if r:
            items += [(r[0], "LON H"), (r[1], "LON L")]

        orb_end = d08 + timedelta(minutes=15)
        if now >= orb_end:
            orb = hi_lo(candles, d08, orb_end)
            if orb:
                items += [(orb[0], "ORB H"), (orb[1], "ORB L")]
    return items, orb, partial


def _atr_latest(candles, n=14):
    """Wilder ATR(n) on 15m candles; latest value or None."""
    if len(candles) < n + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        pc = candles[i - 1]["c"]
        trs.append(max(candles[i]["h"] - candles[i]["l"],
                       abs(candles[i]["h"] - pc),
                       abs(candles[i]["l"] - pc)))
    a = sum(trs[:n]) / n
    for tr in trs[n:]:
        a = (a * (n - 1) + tr) / n
    return a


def _macd_hist_latest(candles):
    """MACD(12,26,9) histogram on closes; latest value or None.
    Reuses _macd_line_series for the MACD line instead of recomputing
    the two EMAs here."""
    if len(candles) < 35:
        return None
    macd = _macd_line_series(candles)
    first = next(i for i, m in enumerate(macd) if m is not None)
    mvals = macd[first:]
    if len(mvals) < 9:
        return None
    sig = _ema_series(mvals, 9)
    return mvals[-1] - sig[-1] if sig[-1] is not None else None


def _pct_move(candles, bars_back):
    if len(candles) <= bars_back:
        return None
    old = candles[-1 - bars_back]["c"]
    return 100.0 * (candles[-1]["c"] - old) / old


# ============================ HIGHER TIMEFRAMES (checklist) ============================
def _ema_series(vals, n):
    """EMA aligned to vals (None until the first n values seed it)."""
    k = 2.0 / (n + 1)
    out = [None] * len(vals)
    if len(vals) >= n:
        out[n - 1] = sum(vals[:n]) / n
        for i in range(n, len(vals)):
            out[i] = vals[i] * k + out[i - 1] * (1 - k)
    return out


def _macd_line_series(candles, fast=12, slow=26):
    """MACD line (fast EMA minus slow EMA) aligned to candles. This is the
    'MACD above or below zero' read the checklist uses for trend and bias."""
    closes = [c["c"] for c in candles]
    ef = _ema_series(closes, fast)
    es = _ema_series(closes, slow)
    return [(a - b if a is not None and b is not None else None)
            for a, b in zip(ef, es)]


def _resample(candles, minutes):
    """Group bars into N-minute bars, aligned to the clock (UTC ms)."""
    span = minutes * 60 * 1000
    out = []
    cur = None
    for c in candles:
        key = (c["t"] // span) * span
        if cur is None or cur["t"] != key:
            cur = {"t": key, "o": c["o"], "h": c["h"], "l": c["l"],
                   "c": c["c"], "v": c.get("v", 0)}
            out.append(cur)
        else:
            cur["h"] = max(cur["h"], c["h"])
            cur["l"] = min(cur["l"], c["l"])
            cur["c"] = c["c"]
            cur["v"] += c.get("v", 0)
    return out


def _weekly_bars(daily):
    """Group daily bars into ISO calendar weeks (Mon-Sun)."""
    out = []
    cur = None
    for c in daily:
        dt = datetime.fromtimestamp(c["t"] / 1000, tz=timezone.utc)
        key = dt.isocalendar()[:2]
        if cur is None or cur["key"] != key:
            cur = {"key": key, "t": c["t"], "o": c["o"], "h": c["h"],
                   "l": c["l"], "c": c["c"], "v": c.get("v", 0)}
            out.append(cur)
        else:
            cur["h"] = max(cur["h"], c["h"])
            cur["l"] = min(cur["l"], c["l"])
            cur["c"] = c["c"]
            cur["v"] += c.get("v", 0)
    return out


def _swing_points(candles, k=2, bar_ms=15 * 60 * 1000):
    """Swing highs/lows: bar j is a swing high if its high is the max of
    highs[j-k..j+k] and strictly above both neighbours (same for lows).
    Each swing carries its confirmation time in ms - only knowable once
    that bar closes, so nothing here peeks ahead."""
    out = []
    n = len(candles)
    for j in range(k, n - k):
        hj = candles[j]["h"]
        lj = candles[j]["l"]
        if (hj > candles[j - 1]["h"] and hj > candles[j + 1]["h"]
                and hj == max(c["h"] for c in candles[j - k:j + k + 1])):
            out.append({"t": candles[j]["t"], "conf_ms": candles[j + k]["t"] + bar_ms,
                        "price": hj, "kind": "H"})
        if (lj < candles[j - 1]["l"] and lj < candles[j + 1]["l"]
                and lj == min(c["l"] for c in candles[j - k:j + k + 1])):
            out.append({"t": candles[j]["t"], "conf_ms": candles[j + k]["t"] + bar_ms,
                        "price": lj, "kind": "L"})
    return out


def _last_swings(candles, k=2, bar_ms=15 * 60 * 1000, as_of_ms=None):
    """The last two confirmed swing highs and lows (prices, newest first).
    as_of_ms limits to swings confirmed at or before that time."""
    sw = _swing_points(candles, k=k, bar_ms=bar_ms)
    if as_of_ms is not None:
        sw = [s for s in sw if s["conf_ms"] <= as_of_ms]
    highs = [s["price"] for s in sw if s["kind"] == "H"][-2:]
    lows = [s["price"] for s in sw if s["kind"] == "L"][-2:]
    return highs[::-1], lows[::-1]


def _structure(candles, k=2, bar_ms=15 * 60 * 1000, as_of_ms=None, swings=None):
    """Structure from the last two confirmed swing highs/lows: +1 for
    HH/HL (bullish), -1 for LH/LL (bearish), None when mixed or unknown.
    Pass precomputed swings (from _last_swings) to avoid recomputing them."""
    if swings is None:
        swings = _last_swings(candles, k=k, bar_ms=bar_ms, as_of_ms=as_of_ms)
    highs, lows = swings
    if len(highs) < 2 or len(lows) < 2:
        return None
    if highs[0] > highs[1] and lows[0] > lows[1]:
        return 1
    if highs[0] < highs[1] and lows[0] < lows[1]:
        return -1
    return None


def _unbroken(candles, swings):
    """The swings price has never closed beyond since they formed: a swing
    high is broken by a later close above it, a swing low by a later close
    below it. Returns (unbroken highs, unbroken lows) as swing dicts, in
    time order. The checklist deletes broken lines; these are the live ones."""
    out_h, out_l = [], []
    for s in swings:
        broken = False
        for c in candles:
            if c["t"] <= s["t"]:
                continue
            if s["kind"] == "H" and c["c"] > s["price"]:
                broken = True
                break
            if s["kind"] == "L" and c["c"] < s["price"]:
                broken = True
                break
        if not broken:
            (out_h if s["kind"] == "H" else out_l).append(s)
    return out_h, out_l


def fetch_daily_for(name):
    """Daily bars for the active feed (context: weekly/daily trend). Best
    effort: returns a list of daily bars or None. TradeLocker daily is
    untested, like the rest of that feed."""
    try:
        if name.startswith("OANDA"):
            url = (f"{OANDA_HOST}/v3/instruments/XAU_USD/candles"
                   "?granularity=D&count=300&price=M")
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {OANDA_API_TOKEN}",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.load(resp)
            out = []
            for c in data.get("candles", []):
                if not c.get("complete"):
                    continue
                mid = c.get("mid", {})
                out.append({
                    "t": int(_parse_oanda_time(c["time"]).timestamp() * 1000),
                    "o": float(mid["o"]), "h": float(mid["h"]),
                    "l": float(mid["l"]), "c": float(mid["c"]),
                    "v": float(c.get("volume", 0)),
                })
            return out or None
        if name.startswith("Binance perp"):
            candles, _, _ = fetch_binance(
                "https://fapi.binance.com/fapi/v1/klines?symbol=XAUUSDT&interval=1d&limit=300")
            return candles
        if name.startswith("Bybit"):
            candles, _, _ = fetch_bybit(
                "https://api.bybit.com/v5/market/kline?category=linear&symbol=XAUUSDT&interval=D&limit=300")
            return candles
        if name.startswith("Binance spot"):
            candles, _, _ = fetch_binance(
                "https://data-api.binance.vision/api/v3/klines?symbol=PAXGUSDT&interval=1d&limit=300")
            return candles
        if name.startswith("TradeLocker"):
            from tradelocker import TLAPI
            tl = TLAPI(environment="https://demo.tradelocker.com",
                        username=TL_EMAIL, password=TL_PASSWORD, server=TL_SERVER)
            instrument_id = tl.get_instrument_id_from_symbol_name("XAUUSD")
            hist = tl.get_price_history(instrument_id, resolution="1d",
                                        start_timestamp=0, end_timestamp=0,
                                        lookback_period="300D")
            return _tl_rows_to_candles(hist)
    except Exception:
        return None
    return None


def _tl_rows_to_candles(hist):
    """TradeLocker price-history rows (any shape the library returns) into
    our candle dicts, sorted by time."""
    if hasattr(hist, "to_dict"):  # pandas DataFrame
        rows = hist.to_dict("records")
    elif isinstance(hist, dict) and "candles" in hist:
        rows = hist["candles"]
    elif isinstance(hist, list):
        rows = hist
    else:
        raise RuntimeError(
            f"TradeLocker price history came back in an unexpected shape "
            f"({type(hist).__name__}); check the run log.")
    candles = []
    for row in rows:
        t = _coerce_time(_pick(row, "time", "timestamp", "date"))
        candles.append({
            "t": int(t.astimezone(timezone.utc).timestamp() * 1000),
            "o": float(_pick(row, "open", "o")),
            "h": float(_pick(row, "high", "h")),
            "l": float(_pick(row, "low", "l")),
            "c": float(_pick(row, "close", "c")),
            "v": float(_pick(row, "volume", "tick_volume", default=0)),
        })
    candles.sort(key=lambda c: c["t"])
    return candles


def _make_card_base(candles, live, now, kind, feed_name, fallback_note, gap, daily=None):
    last_close = candles[-1]["c"]
    price = (live if live is not None else last_close) + PRICE_OFFSET
    items, orb, partial = build_levels(candles, now, kind)
    rows = [(p + PRICE_OFFSET, label) for (p, label) in items]
    if kind == "NY":
        d08 = now.replace(hour=8, minute=0, second=0, microsecond=0)
        vwap = session_vwap(candles, d08)
        if vwap is not None:
            vwap += PRICE_OFFSET
            rows.append((vwap, "VWAP"))
    rows.append((price, "NOW"))
    rows.sort(key=lambda x: -x[0])

    if kind != "NY":
        # Week-open card keeps the plain levels layout.
        lines = []
        for p, label in rows:
            if label == "NOW":
                tag = " (live)" if live is not None else " (close)"
                lines.append("%s  >>> NOW%s <<<" % (format(p, ",.1f"), tag))
            else:
                lines.append("%s  %-6s %+.0f" % (format(p, ",.1f"), label, p - price))
        lines.append("Levels are from the last full session.")
        lines.append("feed: %s%s" % (feed_name, fallback_note))
        if gap:
            lines.append(gap)
        return "GOLD WEEK OPEN", "\n".join(lines)

    # ---- NY scalp card: ranked setups, then the levels list ----
    # Simple layout with a bit of pop: the best trade up top, one compact
    # block per setup, the checklist context in two short lines.
    levels = {label: p + PRICE_OFFSET for (p, label) in items}
    raw_levels = {label: p for (p, label) in items}
    raw_price = price - PRICE_OFFSET
    atr = _atr_latest(candles)
    macd_hist = _macd_hist_latest(candles)
    vols = [c.get("v", 0) for c in candles]
    vol_ok = (len(vols) >= 21 and sum(vols[-21:-1]) > 0
              and vols[-1] > 1.5 * (sum(vols[-21:-1]) / 20.0))
    tf = [("24h", _pct_move(candles, 96)), ("4H", _pct_move(candles, 16)),
          ("1H", _pct_move(candles, 4)), ("15m", _pct_move(candles, 1))]
    tf_line = " | ".join("%s %s" % (n, ("%+.1f%%" % v) if v is not None else "n/a")
                         for n, v in tf)
    orb_range = (orb[0] - orb[1]) if orb else None
    orb_ok = (orb_range is not None
              and ORB_MIN_RANGE <= orb_range <= ORB_MAX_RANGE)

    # ---- Checklist context: 1H bias, 4H swings, weekly/daily trend ----
    now_ms = to_ms(now)
    h1 = _resample(candles, 60)
    h4 = _resample(candles, 240)
    h1_line = _macd_line_series(h1)
    h1_macd = h1_line[-1] if h1_line else None
    h1_h, h1_l = _last_swings(h1, bar_ms=3600 * 1000)
    h1_struct = _structure(h1, bar_ms=3600 * 1000, swings=(h1_h, h1_l))
    # Full 1H bias: MACD line and structure agree. The backtest says this
    # is the best per-trade filter (+0.17R vs +0.14R baseline) but it cuts
    # 75% of trades, so it is shown, not used as a hard gate.
    bias_1h = None
    if h1_macd is not None and h1_struct is not None:
        if h1_macd > 0 and h1_struct == 1:
            bias_1h = 1
        elif h1_macd < 0 and h1_struct == -1:
            bias_1h = -1
    h4_sw = _swing_points(h4, bar_ms=4 * 3600 * 1000)
    h4_uh, h4_ul = _unbroken(h4, h4_sw)
    d_macd = w_macd = None
    d_h = d_l = w_h = w_l = None
    d_above = None
    if daily:
        d_line = _macd_line_series(daily)
        d_macd = d_line[-1] if d_line else None
        d_sw = _swing_points(daily, bar_ms=24 * 3600 * 1000)
        d_uh, d_ul = _unbroken(daily, d_sw)
        if d_uh:
            d_h = d_uh[-1]["price"] + PRICE_OFFSET
        if d_ul:
            d_l = d_ul[-1]["price"] + PRICE_OFFSET
        above = [s["price"] for s in d_uh if s["price"] > raw_price]
        if above:
            d_above = min(above) + PRICE_OFFSET
        wk = _weekly_bars(daily)
        wk_line = _macd_line_series(wk)
        w_macd = wk_line[-1] if wk_line else None
        wk_sw = _swing_points(wk, bar_ms=7 * 24 * 3600 * 1000)
        w_uh, w_ul = _unbroken(wk, wk_sw)
        if w_uh:
            w_h = w_uh[-1]["price"] + PRICE_OFFSET
        if w_ul:
            w_l = w_ul[-1]["price"] + PRICE_OFFSET

    # ---- Swing levels (checklist 15m + 4H layers) ----
    # (label, price, direction, conf_ms). Only the last two 15m swings and
    # the last 4H swing of each kind, near price (within 3xATR), and only
    # ones that do not duplicate a session level.
    swing_defs = []
    if atr:
        m15_sw = _swing_points(candles, bar_ms=15 * 60 * 1000)
        for kind, direction, tag in (("H", "long", "15m H"), ("L", "short", "15m L")):
            recent = [s for s in m15_sw if s["kind"] == kind
                      and s["conf_ms"] <= now_ms][-2:]
            for n, s in enumerate(reversed(recent), 1):
                if abs(s["price"] - raw_price) > 3.0 * atr:
                    continue
                swing_defs.append(("%s%d" % (tag, n), s["price"], direction,
                                   s["conf_ms"]))
        for sw_list, direction, tag in ((h4_uh, "long", "4H H"), (h4_ul, "short", "4H L")):
            if not sw_list:
                continue
            s = sw_list[-1]
            if abs(s["price"] - raw_price) > 3.0 * atr:
                continue
            swing_defs.append((tag, s["price"], direction, s["conf_ms"]))
        session_prices = [p for p, _ in items]
        swing_defs = [(nm, p, d, cm) for nm, p, d, cm in swing_defs
                      if all(abs(p - sp) > 0.25 * atr for sp in session_prices)]
    for nm, p, _d, _cm in swing_defs:
        levels[nm] = p + PRICE_OFFSET
        raw_levels[nm] = p
        rows.append((p + PRICE_OFFSET, nm))
    rows.sort(key=lambda x: -x[0])

    def build_setup(label, direction, entry, cond, ok=True, check_ran=True,
                    invalid=None):
        """A scalp setup: entry, 1xATR stop, TPs at the successive levels
        ahead, a 0-5 score and a TRADE / NO TRADE verdict.
        check_ran only applies to breakout entries (price may already have
        blown past the level); retest entries are pending pullbacks, so the
        price being past the level is the setup itself, not a miss.
        invalid is (price, "below"/"above"): a 15m close beyond it kills
        the setup - back inside the range for an ORB breakout, through the
        level by 0.25 ATR for a retest."""
        if atr is None or atr <= 0:
            return None
        stop = entry - atr if direction == "long" else entry + atr
        risk = abs(entry - stop)
        ahead = sorted([p for p in levels.values()
                        if (p > entry if direction == "long" else p < entry)],
                       reverse=(direction == "short"))
        tps = []
        for p in ahead[:4]:
            r = ((p - entry) / risk if direction == "long"
                 else (entry - p) / risk)
            tps.append((p, r))
        room = tps[-1][1] if tps else 0.0
        score = 0
        if room >= 2.0:
            score += 2
        elif room >= 1.0:
            score += 1
        if vol_ok:
            score += 1
        vwap_with = (vwap is not None and
                     ((direction == "long" and entry > vwap) or
                      (direction == "short" and entry < vwap)))
        if vwap_with:
            score += 1
        macd_ok = (macd_hist is not None and
                   ((direction == "long" and macd_hist > 0) or
                    (direction == "short" and macd_hist < 0)))
        if macd_ok:
            score += 1
        reasons = []
        if not ok:
            reasons.append("ORB range %.1f pts: SKIP" % orb_range)
        if not tps:
            reasons.append("no levels ahead")
        elif room < 1.0:
            reasons.append("room %.1fR < 1R" % room)
        if score < 3:
            reasons.append("score %d < 3" % score)
        ran = ((price - entry) / risk if direction == "long"
               else (entry - price) / risk)
        if check_ran and ran > 0.5:
            reasons.append("already ran %.1fR past entry" % ran)
        why = ["room %.1fR" % room,
               "vol OK" if vol_ok else "vol weak",
               "VWAP with" if vwap_with else "VWAP against",
               "MACD agrees" if macd_ok else "MACD disagrees"]
        if bias_1h is None:
            why.append("1H bias mixed")
        elif ((direction == "long" and bias_1h == 1)
              or (direction == "short" and bias_1h == -1)):
            why.append("1H bias with")
        else:
            why.append("1H bias against")
        return {"label": label, "direction": direction, "entry": entry,
                "stop": stop, "risk": risk, "tps": tps, "room": room,
                "score": score, "trade": not reasons, "reasons": reasons,
                "why": " · ".join(why), "cond": cond, "invalid": invalid}

    def setup_block(s, header):
        """One setup as a compact block: header, entry/SL/TPs, exit+invalid."""
        out = [header]
        out.append("Entry %s · SL %s · Risk %.1f" % (
            format(s["entry"], ",.1f"), format(s["stop"], ",.1f"), s["risk"]))
        if s["tps"]:
            out.append(" · ".join("TP%d %s (%.1fR)" % (
                j + 1, format(p, ",.1f"), r)
                for j, (p, r) in enumerate(s["tps"])))
        else:
            out.append("No TPs: no levels ahead")
        exit_line = "Exit: trail 40%"
        if s.get("invalid"):
            ip, side = s["invalid"]
            exit_line += " · Invalid 15m close %s %s" % (side, format(ip, ",.1f"))
        out.append(exit_line)
        out.append("Why: %s" % s["why"])
        return out

    setups = []
    if orb:
        s = build_setup("ORB LONG", "long", levels["ORB H"],
                        "on 15m close beyond %s" % format(levels["ORB H"], ",.1f"),
                        ok=orb_ok, invalid=(levels["ORB H"], "below"))
        if s:
            setups.append(s)
        s = build_setup("ORB SHORT", "short", levels["ORB L"],
                        "on 15m close below %s" % format(levels["ORB L"], ",.1f"),
                        ok=orb_ok, invalid=(levels["ORB L"], "above"))
        if s:
            setups.append(s)
    # Retests: levels broken after they formed, with price near them now.
    # A level counts as broken only by a close beyond it AFTER its window
    # closed (PDH/PDL 00:00, ASIA 03:00, LON 08:00, swing levels at their
    # confirmation time) - a close before the level existed does not count.
    orb_end_ms = to_ms(d08 + timedelta(minutes=15))
    day = now.date()
    formed_ms = {}
    for label, t in (("PDH", time(0, 0)), ("PDL", time(0, 0)),
                     ("ASIA H", time(3, 0)), ("ASIA L", time(3, 0)),
                     ("LON H", time(8, 0)), ("LON L", time(8, 0))):
        formed_ms[label] = to_ms(datetime.combine(day, t))
    cand_defs = []
    cand_dir = {}
    for label in ("PDH", "PDL", "ASIA H", "ASIA L", "LON H", "LON L"):
        if label in raw_levels:
            direction = "long" if label.endswith("H") else "short"
            cand_defs.append((label, raw_levels[label], direction, formed_ms[label]))
            cand_dir[label] = direction
    for nm, p, direction, cm in swing_defs:
        cand_defs.append((nm, p, direction, cm))
        cand_dir[nm] = direction
    broken = set()
    cands = []
    for label, lv, direction, conf_ms in cand_defs:
        if atr is None:
            continue
        is_high = direction == "long"
        broke = any((c["c"] > lv if is_high else c["c"] < lv)
                    for c in candles
                    if conf_ms <= c["t"] < orb_end_ms)
        if not broke:
            continue
        broken.add(label)
        if abs(raw_price - lv) > 1.5 * atr:
            continue
        # Invalidated if price closed through the level by more than 0.25 ATR.
        if is_high and candles[-1]["c"] < lv - 0.25 * atr:
            continue
        if not is_high and candles[-1]["c"] > lv + 0.25 * atr:
            continue
        cands.append((abs(raw_price - lv), label))
    cands.sort()
    for _, label in cands[:2]:
        lv = levels[label]
        if cand_dir[label] == "long":
            s = build_setup("RETEST LONG", "long", lv, "pullback to %s" % label,
                            check_ran=False,
                            invalid=(lv - 0.25 * atr, "below"))
        else:
            s = build_setup("RETEST SHORT", "short", lv, "rally to %s" % label,
                            check_ran=False,
                            invalid=(lv + 0.25 * atr, "above"))
        if s:
            setups.append(s)
    setups.sort(key=lambda s: (not s["trade"], -s["score"], -s["room"]))

    # WATCH: levels not broken yet, with the full plan for when they break.
    # Nearest four to price, within 3xATR. The ORB levels are already TOP
    # breakout setups, so they are not listed here.
    watch_defs = []
    for label, lv, direction, _cm in cand_defs:
        if label in broken or label.startswith("ORB"):
            continue
        if atr is None or abs(raw_price - lv) > 3.0 * atr:
            continue
        watch_defs.append((abs(raw_price - lv), label, lv, direction))
    watch_defs.sort()
    watch_setups = []
    for _d, label, lv, direction in watch_defs[:4]:
        if direction == "long":
            s = build_setup("WATCH " + label, "long", levels[label],
                            "if 15m closes above %s, then pullback" % format(lv, ",.1f"),
                            check_ran=False,
                            invalid=(levels[label] - 0.25 * atr, "below"))
        else:
            s = build_setup("WATCH " + label, "short", levels[label],
                            "if 15m closes below %s, then rally" % format(lv, ",.1f"),
                            check_ran=False,
                            invalid=(levels[label] + 0.25 * atr, "above"))
        if s:
            watch_setups.append(s)

    lines = []
    lines.append("NOW %s%s · %s" % (format(price, ",.1f"),
                                    " (live)" if live is not None else " (close)",
                                    tf_line))
    if orb_range is not None:
        lines.append("ORB range %.1f pts: %s" % (
            orb_range, "OK" if orb_ok else "SKIP"))
    else:
        lines.append("ORB: forms at 8:15 (first 15m candle after 8:00)")
    if partial:
        lines.append("London levels are partial (session still open)")
    # Context: weekly/daily MACD arrows, 1H bias, 4H swings, W/D swings.
    def _arrow(v):
        if v is None:
            return "n/a"
        return "↑" if v > 0 else "↓"
    ctx1 = ["W %s" % _arrow(w_macd), "D %s" % _arrow(d_macd)]
    if h1_macd is not None:
        st = "(HH/HL)" if h1_struct == 1 else ("(LH/LL)" if h1_struct == -1 else "(mixed)")
        ctx1.append("1H %s %s" % ("↑" if h1_macd > 0 else "↓", st))
    else:
        ctx1.append("1H n/a")
    if h4_uh or h4_ul:
        ctx1.append("4H H %s L %s" % (
            format(h4_uh[-1]["price"] + PRICE_OFFSET, ",.1f") if h4_uh else "n/a",
            format(h4_ul[-1]["price"] + PRICE_OFFSET, ",.1f") if h4_ul else "n/a"))
    lines.append("Context: " + " · ".join(ctx1))
    if daily is None:
        lines.append("W/D swings: daily bars unavailable")
    else:
        ctx2 = []
        if w_h is not None or w_l is not None:
            ctx2.append("W H %s / L %s" % (
                format(w_h, ",.1f") if w_h is not None else "n/a",
                format(w_l, ",.1f") if w_l is not None else "n/a"))
        if d_h is not None or d_l is not None:
            ctx2.append("D H %s / L %s" % (
                format(d_h, ",.1f") if d_h is not None else "n/a",
                format(d_l, ",.1f") if d_l is not None else "n/a"))
        if d_above is not None:
            ctx2.append("D next H above %s" % format(d_above, ",.1f"))
        if ctx2:
            lines.append(" · ".join(ctx2))
    lines.append("")
    # Colour for a markup level, per the Gold Daily Checklist colour key:
    # weekly purple, daily orange, 4H yellow, 1H blue, 15m green, ORB
    # green. The card is plain text (ntfy), so the colour is written as a
    # word — it tells him which colour to draw the line in TradingView.
    def _level_colour(label):
        if label.startswith("W "):
            return "purple"
        if label.startswith("D "):
            return "orange"
        if label.startswith("4H "):
            return "yellow"
        if label.startswith("1H "):
            return "blue"
        if label.startswith("15m") or label.startswith("ORB"):
            return "green"
        if label.startswith(("ASIA", "LON")):
            return "green"
        return ""

    # Markup levels (the chart markup for the day, per the Gold Daily
    # Checklist line budget): weekly 1, daily 2, 4H 2, 1H 2, 15m up to 4,
    # ORB 2. Each is (price, label). The 4H "box" is the entry zone around
    # the 4H swing level; in text it shows as the level line. All prices
    # here are offset, matching `price`.
    markup = []
    # Weekly: nearest unbroken weekly swing (1 line).
    _w = [(abs(p - price), p, lab) for p, lab in ((w_h, "W H"), (w_l, "W L"))
          if p is not None]
    if _w:
        _d, _p, _lab = min(_w)
        markup.append((_p, _lab))
    # Daily: nearest unbroken daily swing high and low (2 lines).
    if d_h is not None:
        markup.append((d_h, "D H"))
    if d_l is not None:
        markup.append((d_l, "D L"))
    # 4H: last unbroken 4H swing high and low (2 lines).
    if h4_uh:
        markup.append((h4_uh[-1]["price"] + PRICE_OFFSET, "4H H"))
    if h4_ul:
        markup.append((h4_ul[-1]["price"] + PRICE_OFFSET, "4H L"))
    # 1H: last 1H swing high and low (2 lines).
    if h1_h:
        markup.append((h1_h[0] + PRICE_OFFSET, "1H H"))
    if h1_l:
        markup.append((h1_l[0] + PRICE_OFFSET, "1H L"))
    # 15m: nearest 4 of the 15m swings + ASIA/LON levels.
    _f15 = []
    for nm, p, _dir, _cm in swing_defs:
        if nm.startswith("15m"):
            _f15.append((abs(p + PRICE_OFFSET - price), p + PRICE_OFFSET, nm))
    for _p, lab in items:
        if lab in ("ASIA H", "ASIA L", "LON H", "LON L"):
            _p2 = _p + PRICE_OFFSET
            _f15.append((abs(_p2 - price), _p2, lab))
    _f15.sort(key=lambda x: x[0])
    for _d, _p, _lab in _f15[:4]:
        markup.append((_p, _lab))
    # ORB: ORB H/L (2 lines, if the ORB has formed).
    if orb:
        markup.append((orb[0] + PRICE_OFFSET, "ORB H"))
        markup.append((orb[1] + PRICE_OFFSET, "ORB L"))
    # Sort by price (highest first), dedupe identical prices.
    markup.sort(key=lambda x: -x[0])
    _seen = set()
    _dedup = []
    for _p, _lab in markup:
        _k = round(_p, 1)
        if _k in _seen:
            continue
        _seen.add(_k)
        _dedup.append((_p, _lab))
    markup = _dedup
    # Line budget: about 10 objects. Always keep the weekly and daily
    # lines (the context), then fill the rest with the nearest 4H/1H/15m/
    # ORB levels until 10 total.
    _keep_wd = [m for m in markup if m[1].startswith(("W ", "D "))]
    _rest = sorted([m for m in markup if not m[1].startswith(("W ", "D "))],
                   key=lambda x: abs(x[0] - price))
    markup = sorted(_keep_wd + _rest[:max(0, 10 - len(_keep_wd))],
                    key=lambda x: -x[0])
    # The 3 TradingView alerts: the 3 markup levels closest to price.
    # Alerts are levels to watch — the trade setup (entry, SL, TPs) is a
    # separate thing, listed under TOP 3.
    _alert = sorted(((abs(p - price), p, lab) for p, lab in markup),
                    key=lambda x: x[0])
    alert_levels = [(p, lab) for _d, p, lab in _alert[:3]]
    # Best trade up top, so the answer is visible without scrolling.
    best = next((s for s in setups if s["trade"]), None)
    if best is not None:
        lines.append("🔥 BEST: %s %s · SL %s · %d/5" % (
            best["label"], format(best["entry"], ",.1f"),
            format(best["stop"], ",.1f"), best["score"]))
    elif setups:
        lines.append("⚠️ NO TRADE SETUP — top: %s (%s)" % (
            setups[0]["label"], "; ".join(setups[0]["reasons"])))
    else:
        lines.append("⚠️ NO TRADE SETUP")
    if alert_levels:
        lines.append("🔔 ALERTS — set these %d in TradingView" % len(alert_levels))
        for i, (p, lab) in enumerate(alert_levels, 1):
            lines.append("%d. %s  %-6s %-7s %+.0f" % (
                i, format(p, ",.1f"), lab, _level_colour(lab), p - price))
    lines.append("")
    lines.append("TOP %d (ranked: score, room)" % len(setups[:3]))
    for i, s in enumerate(setups[:3], 1):
        verdict = "🟢 TRADE" if s["trade"] else "⛔ NO TRADE: " + "; ".join(s["reasons"])
        lines.append("")
        lines.extend(setup_block(
            s, "%d. %s %s · %d/5" % (i, s["label"], verdict, s["score"])))
    if watch_setups:
        lines.append("")
        lines.append("👀 WATCH (not broken yet — plan for when it breaks)")
        for s in watch_setups:
            lines.append("")
            lines.extend(setup_block(s, "%s (%s)" % (s["label"], s["cond"])))
    lines.append("")
    lines.append("Key levels")
    # The day's chart markup (Gold Daily Checklist line budget): weekly 1,
    # daily 2, 4H 2, 1H 2, 15m up to 4, ORB 2. `markup` was computed above,
    # before BEST, so the ALERTS and this list always agree. NOW is merged
    # in by price so the list reads top to bottom.
    _kl = sorted(markup + [(price, "NOW")], key=lambda x: -x[0])
    for p, label in _kl:
        if label == "NOW":
            tag = " (live)" if live is not None else " (close)"
            lines.append("%s  >>> NOW%s <<<" % (format(p, ",.1f"), tag))
        else:
            lines.append("%s  %-6s %-7s %+.0f" % (
                format(p, ",.1f"), label, _level_colour(label), p - price))
    lines.append("feed: %s%s" % (feed_name, fallback_note))
    if gap:
        lines.append(gap)
    return "GOLD SCALPS NY", "\n".join(lines)


# ============================ NEWS ============================
NEWS_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"


def get_news(now):
    """Today's USD high-impact news lines. None if unavailable."""
    try:
        data = http_get(NEWS_URL)
        out = []
        for e in data:
            if e.get("country") != "USD" or e.get("impact") != "High":
                continue
            t = datetime.fromisoformat(e["date"]).astimezone(ET)
            if t.date() != now.date():
                continue
            m = t.hour * 60 + t.minute
            flag = " - ORB: skip or wait" if 480 <= m <= 585 else ""
            out.append((t, "NEWS %s ET: %s%s" % (t.strftime("%-I:%M"), e.get("title", "?"), flag)))
        out.sort(key=lambda x: x[0])
        return [line for _, line in out]
    except Exception:
        return None


# Wraps make_card (defined above) so the NY card carries the news
# lines. The wrapper's signature must match the base make_card
# exactly, or the briefing call breaks - the pasted version was
# written for a 4-argument make_card, this one takes 7.


def make_card(candles, live, now, kind, feed_name, fallback_note, gap, daily=None):
    title, body = _make_card_base(candles, live, now, kind, feed_name,
                                  fallback_note, gap, daily=daily)
    if kind != "NY":
        return title, body
    news = get_news(now)
    if news is None:
        extra = ["NEWS: unavailable"]
    elif news:
        extra = news
    else:
        extra = ["NEWS: no high-impact USD today"]
    lines = body.split("\n")
    return title, "\n".join(lines[:-1] + extra + lines[-1:])


# ============================ NTFY ============================
def send_ntfy(title, body, priority=3, tags="chart_with_upwards_trend"):
    print("----- %s -----" % title)
    print(body)
    if not NTFY_TOPIC:
        print("NTFY_TOPIC is not set, so nothing was sent.")
        return False
    req = urllib.request.Request(
        NTFY_SERVER + "/" + NTFY_TOPIC,
        data=body.encode("utf-8"),
        method="POST",
        headers={"Title": title, "Priority": str(priority), "Tags": tags,
                 "Click": TRADINGVIEW_CHART},
    )
    if NTFY_TOKEN:
        req.add_header("Authorization", "Bearer " + NTFY_TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            print("ntfy status:", resp.status)
        return True
    except Exception as e:  # noqa: BLE001
        print("ntfy send failed:", type(e).__name__, e)
        return False


# ============================ MODES ============================
def feedcheck():
    results = fetch_all_feeds()
    lines = []
    ok = []
    ref = None
    for r in results:
        p = price_of(r)
        if r["candles"] is not None and p is not None:
            lines.append("%s: %s (%d candles)" % (r["name"], format(p, ",.2f"), len(r["candles"])))
            ok.append(r)
            if r["name"].startswith("OANDA"):
                ref = p
        else:
            lines.append("%s: FAILED (%s)" % (r["name"], r["error"]))
    if ref is not None:
        for r in ok:
            if r["name"].startswith("OANDA"):
                continue
            lines.append("%s vs OANDA: %+.2f" % (r["name"], price_of(r) - ref))
    lines.append("Compare with your chart's last closed 15m candle,")
    lines.append("then set PRICE_OFFSET = chart price minus feed price.")
    send_ntfy("GOLD FEED CHECK", "\n".join(lines), priority=3, tags="mag")
    return 0 if ok else 1


def briefing():
    now = datetime.now(ET)
    mins = now.hour * 60 + now.minute
    kind = None
    if FORCE:
        kind = "SUNDAY" if (now.weekday() == 6 and mins >= SUNDAY_WINDOW[0]) else "NY"
    elif now.weekday() <= 4 and NY_WINDOW[0] <= mins <= NY_WINDOW[1]:
        kind = "NY"
    elif now.weekday() == 6 and SUNDAY_WINDOW[0] <= mins <= SUNDAY_WINDOW[1]:
        kind = "SUNDAY"
    else:
        print("Outside the briefing window. Nothing sent.")
        return 0

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    results = fetch_all_feeds()
    active = pick_active(results, kind, now_ms)
    if active is None:
        errors = ["%s: %s" % (r["name"], r["error"]) for r in results]
        send_ntfy("GOLD BRIEFING FAILED",
                  "No price feed responded.\n" + "\n".join(errors),
                  priority=4, tags="warning")
        return 1

    fallback_note = " (fallback - check offset)" if active["fallback"] else ""
    # Daily bars for the weekly/daily trend in the Context section. Best
    # effort: the card still builds without them.
    daily = fetch_daily_for(active["name"])
    title, body = make_card(active["candles"], active["live"], now, kind,
                            active["name"], fallback_note, gap_line(results),
                            daily=daily)
    send_ntfy(title, body)
    return 0


# ============================ SELF-TEST ============================
def _row(dt_et, o, h, l, c, v=100):
    return {"t": int(dt_et.astimezone(timezone.utc).timestamp() * 1000),
            "o": o, "h": h, "l": l, "c": c, "v": v}


def selftest():
    """Synthetic candles with known answers; verifies the maths. Sends nothing."""
    global PRICE_OFFSET
    ok = True

    def check(name, got, want):
        nonlocal ok
        good = got is not None and abs(got - want) < 0.01
        print(("PASS " if good else "FAIL ") + "%s: got %s, want %s" % (name, got, want))
        if not good:
            ok = False

    # --- Monday 2026-11-09 08:25 ET: prior day must be the Friday session ---
    mon = datetime(2026, 11, 9, 8, 25, tzinfo=ET)
    thu = datetime(2026, 11, 5).date()
    fri = datetime(2026, 11, 6).date()
    sun = datetime(2026, 11, 8).date()
    mon_d = datetime(2026, 11, 9).date()
    # Extra history so ATR / MACD / volume all compute (levels windows are
    # untouched: these bars end Thu 06:45, before the Fri prior-day window).
    pre = []
    t0 = datetime(2026, 11, 4, 6, 0, tzinfo=ET)
    p = 4040.0
    for k in range(100):
        dt = t0 + timedelta(minutes=15 * k)
        p += 0.2
        pre.append(_row(dt, p - 0.2, p + 3, p - 3, p))
    candles = pre + [
        _row(datetime.combine(thu, time(18, 0), ET), 4050, 4090.00, 4045, 4060),
        _row(datetime.combine(fri, time(10, 0), ET), 4060, 4062, 4027.83, 4035),
        _row(datetime.combine(sun, time(19, 0), ET), 4040, 4071.20, 4038.55, 4065),
        _row(datetime.combine(mon_d, time(2, 45), ET), 4065, 4068, 4055, 4060),
        _row(datetime.combine(mon_d, time(3, 0), ET), 4060, 4066.40, 4058, 4062),
        _row(datetime.combine(mon_d, time(7, 45), ET), 4050, 4052, 4041.10, 4045),
        _row(datetime.combine(mon_d, time(8, 0), ET), 4060, 4072.00, 4058.30, 4064.00),
        _row(datetime.combine(mon_d, time(8, 15), ET), 4064, 4065, 4062, 4063),
    ]
    items, orb, partial = build_levels(candles, mon, "NY")
    by_label = {label: p for (p, label) in items}
    check("Mon PDH (Friday session)", by_label.get("PDH"), 4090.00)
    check("Mon PDL (Friday session)", by_label.get("PDL"), 4027.83)
    check("Mon ASIA H", by_label.get("ASIA H"), 4071.20)
    check("Mon ASIA L", by_label.get("ASIA L"), 4038.55)
    check("Mon LON H", by_label.get("LON H"), 4066.40)
    check("Mon LON L", by_label.get("LON L"), 4041.10)
    check("Mon ORB H", by_label.get("ORB H"), 4072.00)
    check("Mon ORB L", by_label.get("ORB L"), 4058.30)
    check("Mon ORB range", (orb[0] - orb[1]) if orb else None, 13.70)
    d08 = mon.replace(hour=8, minute=0, second=0, microsecond=0)
    check("Mon VWAP", session_vwap(candles, d08), 4064.05)

    # --- Tuesday 2026-11-10 08:25 ET: prior day is the Monday session ---
    tue = datetime(2026, 11, 10, 8, 25, tzinfo=ET)
    candles2 = candles + [
        _row(datetime.combine(mon_d, time(12, 0), ET), 4070, 4080.00, 4065, 4075),
        _row(datetime.combine(mon_d, time(14, 0), ET), 4040, 4045, 4030.00, 4035),
    ]
    items2, _, _ = build_levels(candles2, tue, "NY")
    by_label2 = {label: p for (p, label) in items2}
    check("Tue PDH (Monday session)", by_label2.get("PDH"), 4080.00)
    check("Tue PDL (Monday session)", by_label2.get("PDL"), 4030.00)

    # --- ORB filter boundaries, via the card text ---
    _, body_ok = make_card(candles, 4064.0, mon, "NY", "test", "", "")
    if "ORB range 13.7 pts: OK" not in body_ok:
        print("FAIL ORB filter: 13.7 should be OK")
        ok = False
    else:
        print("PASS ORB filter: 13.7 is OK")
    wide = [dict(c) for c in candles]
    idx0800 = next(i for i, c in enumerate(candles)
                   if c["t"] == to_ms(datetime.combine(mon_d, time(8, 0), ET)))
    wide[idx0800] = _row(datetime.combine(mon_d, time(8, 0), ET), 4060, 4080.00, 4060.00, 4070)
    _, body_wide = make_card(wide, 4070.0, mon, "NY", "test", "", "")
    if "ORB range 20.0 pts: SKIP" not in body_wide:
        print("FAIL ORB filter: 20.0 should be SKIP")
        ok = False
    else:
        print("PASS ORB filter: 20.0 is SKIP")

    # --- Scalp card: ranked setups with entry / SL / TPs / exit ---
    for needle in ("ORB LONG", "ORB SHORT", "Entry 4,072.0", "Entry 4,058.3",
                   "TP1 4,090.0", "Exit: trail 40%", "TOP 2",
                   "Risk ", "ORB range", "Invalid 15m close",
                   "Context:", "🔥 BEST:", "👀 WATCH", "1H bias",
                   "🔔 ALERTS", "Key levels"):
        if needle not in body_ok:
            print("FAIL scalp card: missing %r" % needle)
            ok = False
        else:
            print("PASS scalp card: %r present" % needle)

    # --- Staleness: 90 min old is stale, 10 min old is fresh ---
    now_ms = int(mon.astimezone(timezone.utc).timestamp() * 1000)
    old = [{"t": now_ms - 105 * 60 * 1000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]
    fresh = [{"t": now_ms - 25 * 60 * 1000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]
    if not is_stale(old, now_ms):
        print("FAIL staleness: 90 min old candle should be stale")
        ok = False
    else:
        print("PASS staleness: 90 min old is stale")
    if is_stale(fresh, now_ms):
        print("FAIL staleness: 10 min old candle should be fresh")
        ok = False
    else:
        print("PASS staleness: 10 min old is fresh")

    # --- Price offset shifts every row ---
    saved_offset = PRICE_OFFSET
    PRICE_OFFSET = 2.0
    _, body_off = make_card(candles, 4064.0, mon, "NY", "test", "", "")
    PRICE_OFFSET = saved_offset
    if "4,074.0  ORB H" not in body_off:
        print("FAIL offset: ORB H should read 4,074.0 with +2.0 offset")
        ok = False
    else:
        print("PASS offset: ORB H reads 4,074.0 with +2.0 offset")

    # --- Checklist helpers: swings, structure, unbroken, MACD line ---
    # Crafted 15m series: swing H 4020 (bar 2), swing L 3990 (bar 5),
    # swing H 4030 (bar 8), swing L 4000 (bar 12). The 4020 high is broken
    # by the 4025 close at bar 8; the rest are unbroken.
    t0 = datetime(2026, 11, 4, 6, 0, tzinfo=ET)
    hl = [(4000, 3990, 3995), (4010, 3995, 4005), (4020, 4000, 4015),
          (4015, 4005, 4010), (4010, 4000, 4005), (4005, 3990, 3995),
          (4010, 3995, 4005), (4020, 4000, 4015), (4030, 4010, 4025),
          (4025, 4015, 4020), (4020, 4010, 4015), (4020, 4010, 4015),
          (4015, 4000, 4005), (4010, 4005, 4008), (4012, 4008, 4010)]
    sw_candles = []
    for k, (h, l, c) in enumerate(hl):
        sw_candles.append(_row(t0 + timedelta(minutes=15 * k), c, h, l, c))
    sw = _swing_points(sw_candles)
    got_sw = [(s["price"], s["kind"]) for s in sw]
    want_sw = [(4020.0, "H"), (3990.0, "L"), (4030.0, "H"), (4000.0, "L")]
    if got_sw != want_sw:
        print("FAIL swings: got %s, want %s" % (got_sw, want_sw))
        ok = False
    else:
        print("PASS swings: %s" % (got_sw,))
    highs, lows = _last_swings(sw_candles)
    if highs != [4030.0, 4020.0] or lows != [4000.0, 3990.0]:
        print("FAIL last swings: highs %s lows %s" % (highs, lows))
        ok = False
    else:
        print("PASS last swings: highs %s lows %s" % (highs, lows))
    if _structure(sw_candles) != 1:
        print("FAIL structure: want bullish (+1)")
        ok = False
    else:
        print("PASS structure: bullish (HH/HL)")
    uh, ul = _unbroken(sw_candles, sw)
    if [s["price"] for s in uh] != [4030.0] or [s["price"] for s in ul] != [3990.0, 4000.0]:
        print("FAIL unbroken: H %s L %s" % ([s["price"] for s in uh], [s["price"] for s in ul]))
        ok = False
    else:
        print("PASS unbroken: H 4030.0, L 3990.0/4000.0 (4020 broken)")
    ml = _macd_line_series(pre)
    if ml[-1] is None or ml[-1] <= 0:
        print("FAIL MACD line: want positive on a steady uptrend, got %s" % ml[-1])
        ok = False
    else:
        print("PASS MACD line: positive on a steady uptrend (%.2f)" % ml[-1])

    # --- Daily context: 200 daily bars, peak high 4200 (unbroken), trough
    # low 4146. Daily MACD ends positive; nearest unbroken H above the
    # live 15m price (4064) is the 4200 peak.
    daily = []
    d0 = datetime(2026, 4, 1, tzinfo=ET)
    c = 4000.0
    for k in range(200):
        if k < 100:
            c += 2.0 if k else 0.0
        elif k < 150:
            c -= 1.0
        else:
            c += 0.5
        daily.append(_row(d0 + timedelta(days=k), c, c + 2, c - 2, c))
    _, body_daily = make_card(candles, 4064.0, mon, "NY", "test", "", "", daily=daily)
    for needle in ("D ↑", "D H 4,200.0 / L 4,146.0", "D next H above 4,200.0"):
        if needle not in body_daily:
            print("FAIL daily context: missing %r" % needle)
            ok = False
        else:
            print("PASS daily context: %r present" % needle)

    print("SELFTEST PASS" if ok else "SELFTEST FAIL")
    return 0 if ok else 1


def main():
    if MODE == "feedcheck":
        return feedcheck()
    if MODE == "selftest":
        return selftest()
    return briefing()


if __name__ == "__main__":
    sys.exit(main())

