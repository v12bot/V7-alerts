# ================= ALEX7 (V4A+) SCREENER - runs on GitHub, pushes to your phone via ntfy =================
# Baseline: the V4A+ Elite Confluence Engine PDF (70/30 edition). SHORTS ONLY. No other framework is used.
# Alert = notification only. You still confirm on the chart before any entry.
#
# Every 15 minutes, 24/7, on all coins in the list below:
#   1 Day (rolling 24h), 4H, 1H, 15m % change per coin (no Week).
#   2 PRE-TOP WATCH (shorts tracking starts here), any ONE of (approved Oct 22):
#       - up 20%+ in rolling 24h
#       - reversal signs: up 10%+ in 24h, 4H still up, 1H or 15m red
#       - a completed sweep + displacement + FVG (F3), any % move
#       - previous-day high swept, then a 15m candle CLOSES back below it (exchange daily candle, UTC) - close only, no minimum distance
#     Funding (Bitget/Gate/MEXC) is shown and used for ranking only; top 5% of positive funding, coins with 1M+ USD 24h volume.
#     PROPOSED REVISION (Oct 22): the previous-day-high route and funding ranking are not yet part of the canonical framework.
#   3 EXHAUSTION: a PRE-TOP WATCH coin showing 4+ of the 7 parabolic signals in framework section 8 (those the script can measure).
#   4 WATCH ALERT: sweep -> displacement -> MSB -> bearish FVG found; price waiting to retrace to the FVG 50%.
#   5 ENTRY ALERT: price retraced to the FVG 50% and a 5m close below the prior 5m low (stand-in for the 1m/5m CHoCH).
#   Each setup shows entry (50% FVG), stop (beyond sweep wick), TP1 (70%), TP2 (30%), R:R (min 2.1, else no trade),
#   score + size tier (framework section 4, OI +2 / 0 / -2), the confluence check, and the exit rules.
#   Top 6 by priority (stage, then score, then R:R, then 24h move). Coins scanned is shown every time.
#
# UNCONFIRMED - USER DECISION REQUIRED: the framework gives no numbers for sweep, displacement, MSB, S/R flip, supply/demand,
# OI flat/stall, MACD settings, choppy 15m, score arithmetic or ranking. The "UNCONFIRMED SETTINGS" block holds provisional
# script values, NOT your rules.
# Not measured (chart only): HTF resistance, choppy 15m, 1m CHoCH, USD events, open positions, daily loss, correlation.
#
# Data: Binance first (perpetual, then spot), then Bybit, MEXC, OKX, Gate, Bitget, BingX. Open interest: OKX only.
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace"); sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception: pass
import os, json, time, difflib, requests, concurrent.futures as cf
import numpy as np, pandas as pd

SCAN_DAYS = 3          # 5m history per coin: Day needs 24h, the previous UTC day high needs up to ~72h
TZ = "America/Toronto"
MIN_FRAC = 0.97        # a coin's 5m data must be at least this complete
WORKERS = 4
STATE_FILE = "state.json"
TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
TEST = os.environ.get("TEST", "").lower() == "true"
PTW_PCT = 20.0         # PRE-TOP WATCH: up 20%+ (rolling 24h)
BIG_MOVE_PCT = 70.0    # PDF F1: moves of 70%+ need 1H lower high + failed reclaim (24h move)
MAX_PTW = 20           # most PRE-TOP WATCH names in one message
MAX_BYTES = 3800       # ntfy limit is 4096 bytes

# ---------------- data (Binance, Bybit, MEXC, OKX, Gate, Bitget, BingX) ----------------
NOTES = {}                # why a coin had no data (shown in the test message)
SRC_TAG = {"binance": "BN", "binance-gapfill": "BN~", "mexc": "MX", "mexc-gapfill": "MX~", "okx": "OK", "okx-gapfill": "OK~",
           "gate": "GT", "gate-gapfill": "GT~", "binance-spot": "BNs", "binance-spot-gapfill": "BNs~",
           "bybit": "BY", "bybit-gapfill": "BY~", "bybit-spot": "BYs", "bybit-spot-gapfill": "BYs~",
           "bitget": "BG", "bitget-gapfill": "BG~", "bingx": "BX", "bingx-gapfill": "BX~"}   # tag shown on each alert line
BN_KIND, BY_KIND = {}, {}  # "perp" or "spot": which feed a coin was read from
BN_ALIAS = {"BEAM": "BEAMX"}   # Binance lists BEAM as BEAMX
# Coins that were renamed on exchanges. Your list keeps the old name; the scan looks up the new one.
ALIAS = {"TON": "GRAM",    # Toncoin renamed to Gram (June 2026)
         "IP": "DATA",     # Story renamed to Data Network (July 2026); Streamr's old DATA became DATAOLD
         "ICX": "SODA"}    # ICON swapped to SODA (swap deadline Sept 30 2026)
# Coins known to be dead, shown next to them in the test message instead of a guess.
KNOWN = {"STG": "Stargate merged into ZRO", "BLAST": "Blast network shut down, being delisted", "HFT": "Hashflow delisted on Binance"}
def _base(sym):
    b = sym.replace("USDT", "")
    return ALIAS.get(b, b)
PFX = (("", 1.0), ("1000", 1e3), ("10000", 1e4), ("1000000", 1e6), ("1M", 1e6))   # multiplier contract names
INSTR = {}                # exchange -> {coin name: that exchange's own symbol}; None if its list could not be loaded
BY_HOSTS = ["https://api.bybit.com", "https://api.bytick.com"]
FAPI = "https://fapi.binance.com/fapi/v1/klines"
BSPOT = "https://data-api.binance.vision/api/v3/klines"
_BN_FUT_BLOCKED = False   # set True if Binance futures refuses this server (some regions get HTTP 451/403)
_BY_BLOCKED = False       # set True if Bybit refuses this server

class NotListed(Exception):
    pass

def _frame(rows, ms0, ms1):
    df = pd.DataFrame(rows, columns=["t", "o", "h", "l", "c", "v"])
    for c in ("o", "h", "l", "c", "v"): df[c] = pd.to_numeric(df[c], errors="coerce")
    df["v"] = df["v"].fillna(0.0)
    df.index = pd.to_datetime(df.pop("t").astype(float), unit="ms", utc=True)
    df = df.sort_index()
    df = df[~df.index.duplicated()]
    return df[(df.index >= pd.to_datetime(ms0, unit="ms", utc=True)) & (df.index <= pd.to_datetime(ms1, unit="ms", utc=True))][["o", "h", "l", "c", "v"]]

def _scale(df, div):
    if div != 1.0: df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]] / div   # show the real price for 1000x contracts
    return df

def resolve(exch, base):
    """-> (exchange symbol, divisor), "guess" if that exchange's list is unavailable, or None if the coin is not listed there."""
    m = INSTR.get(exch)
    if m is None: return "guess"
    if exch == "binance": base = BN_ALIAS.get(base, base)
    for pfx, div in PFX:
        if pfx + base in m: return m[pfx + base], div
    return None

def load_instruments():
    """One call per exchange: which USDT perpetual coins it lists. Lets the scan use the exact symbol and skip coins that are not there."""
    def get(url, **kw):
        r = requests.get(url, timeout=20, **kw)
        return r.json() if r.status_code == 200 else None
    def bn():
        j = get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        return {x["symbol"][:-4]: x["symbol"] for x in j["symbols"] if x.get("contractType") == "PERPETUAL"
                and x.get("quoteAsset") == "USDT" and x.get("status") == "TRADING"}
    def by():
        for host in BY_HOSTS:
            r = requests.get(host + "/v5/market/instruments-info", params=dict(category="linear", limit=1000), timeout=20)
            if r.status_code == 200:
                return {x["symbol"][:-4]: x["symbol"] for x in r.json()["result"]["list"] if x.get("quoteCoin") == "USDT"
                        and x.get("status") == "Trading" and x.get("contractType") == "LinearPerpetual"}
        return None
    def mx():
        j = get("https://contract.mexc.com/api/v1/contract/detail")
        return {x["symbol"][:-5]: x["symbol"] for x in j["data"] if x.get("quoteCoin") == "USDT" and x.get("state", 0) == 0}
    def ok():
        j = get("https://www.okx.com/api/v5/public/instruments", params=dict(instType="SWAP"))
        return {x["instId"].split("-")[0]: x["instId"] for x in j["data"] if x["instId"].endswith("-USDT-SWAP") and x.get("state") == "live"}
    def gt():
        j = get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        return {x["name"][:-5]: x["name"] for x in j if not x.get("in_delisting") and x["name"].endswith("_USDT")}
    def bg():
        j = get("https://api.bitget.com/api/v2/mix/market/contracts", params=dict(productType="USDT-FUTURES"))
        return {x["symbol"][:-4]: x["symbol"] for x in j["data"] if x["symbol"].endswith("USDT") and x.get("symbolStatus", "normal") == "normal"}
    def bx():
        j = get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
        return {x["symbol"][:-5]: x["symbol"] for x in j["data"] if x["symbol"].endswith("-USDT") and x.get("status", 1) == 1}
    for k, fn in (("binance", bn), ("bybit", by), ("mexc", mx), ("okx", ok), ("gate", gt), ("bitget", bg), ("bingx", bx)):
        try: INSTR[k] = fn() or None
        except Exception as e:
            INSTR[k] = None; print("instrument list failed", k, type(e).__name__, e)
    print("instrument lists:", {k: (len(v) if v else None) for k, v in INSTR.items()})

def _bn_get(url, symbol, ms0, ms1):
    r = requests.get(url, params=dict(symbol=symbol, interval="5m", startTime=ms0, endTime=ms1, limit=1500), timeout=15)
    if r.status_code in (451, 403): return "blocked"
    if r.status_code != 200: return None
    j = r.json()
    return j if isinstance(j, list) and j else None

def _binance(sym, ms0, ms1):
    """Binance perpetual (exact symbol from its list), then Binance spot."""
    global _BN_FUT_BLOCKED
    base = _base(sym)
    res = resolve("binance", base)
    tries = []
    if not _BN_FUT_BLOCKED:
        if res == "guess":
            b = BN_ALIAS.get(base, base)
            tries += [(FAPI, b + "USDT", 1.0), (FAPI, "1000" + b + "USDT", 1e3), (FAPI, "1M" + b + "USDT", 1e6)]
        elif res:
            tries += [(FAPI, res[0], res[1])]
    tries += [(BSPOT, BN_ALIAS.get(base, base) + "USDT", 1.0)]
    for url, s_, div in tries:
        j = _bn_get(url, s_, ms0, ms1)
        if j == "blocked":
            if url == FAPI: _BN_FUT_BLOCKED = True
            continue
        if j:
            BN_KIND[sym] = "perp" if url == FAPI else "spot"
            return _scale(_frame([(x[0], x[1], x[2], x[3], x[4], x[5]) for x in j], ms0, ms1), div)
    if res is None: raise NotListed()
    raise RuntimeError("no binance data")

def _by_try(category, symbol, ms0, ms1):
    for host in BY_HOSTS:
        r = requests.get(host + "/v5/market/kline", timeout=15,
                         params=dict(category=category, symbol=symbol, interval="5", start=ms0, end=ms1, limit=1000))
        if r.status_code in (403, 451): continue
        if r.status_code != 200: return None
        j = r.json()
        if j.get("retCode") != 0: return None
        return (j.get("result") or {}).get("list") or None
    return "blocked"

def _bybit(sym, ms0, ms1):
    """Bybit perpetual (exact symbol from its list), then Bybit spot."""
    global _BY_BLOCKED
    if _BY_BLOCKED: raise RuntimeError("bybit blocked")
    base = _base(sym)
    res = resolve("bybit", base)
    tries = []
    if res == "guess":
        tries += [("linear", p + base + "USDT", d) for p, d in PFX]
    elif res:
        tries += [("linear", res[0], res[1])]
    tries += [("spot", base + "USDT", 1.0)]
    for cat, s_, div in tries:
        j = _by_try(cat, s_, ms0, ms1)
        if j == "blocked":
            _BY_BLOCKED = True; raise RuntimeError("bybit blocked")
        if j:
            BY_KIND[sym] = "spot" if cat == "spot" else "perp"
            return _scale(_frame([(x[0], x[1], x[2], x[3], x[4], x[5]) for x in j], ms0, ms1), div)
    if res is None: raise NotListed()
    raise RuntimeError("no bybit data")

def _mexc(sym, ms0, ms1):
    base = _base(sym)
    res = resolve("mexc", base)
    if res is None: raise NotListed()
    name, div = (res if res != "guess" else (base + "_USDT", 1.0))
    rows, s = [], ms0
    while s < ms1:
        e = min(s + 6 * 86400_000, ms1)
        need = (e - s) / 300_000
        ok = False
        for attempt in range(5):
            try:
                j = requests.get(f"https://contract.mexc.com/api/v1/contract/kline/{name}",
                                 params=dict(interval="Min5", start=s // 1000, end=e // 1000), timeout=20).json()
                k = j.get("data") or {}
                t = k.get("time") or []
                if j.get("code") == 0 and len(t) >= 0.97 * need:
                    vol = k.get("vol") or [0.0] * len(t)
                    rows += list(zip(np.array(t, float) * 1000, k["open"], k["high"], k["low"], k["close"], vol))
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(1 + attempt)
        if not ok:
            raise RuntimeError("incomplete chunk")
        s = e
        time.sleep(0.15)
    return _scale(_frame(rows, ms0, ms1), div)

def _okx(sym, ms0, ms1):
    base = _base(sym)
    res = resolve("okx", base)
    if res is None: raise NotListed()
    inst, div = (res if res != "guess" else (f"{base}-USDT-SWAP", 1.0))
    rows, after = [], ms1
    while True:
        d = requests.get("https://www.okx.com/api/v5/market/history-candles",
                         params=dict(instId=inst, bar="5m", after=after, limit=100),
                         timeout=20).json().get("data", [])
        if not d: break
        rows += [x[:6] for x in d]; after = int(d[-1][0])
        if after <= ms0: break
        time.sleep(0.12)
    return _scale(_frame(rows, ms0, ms1), div)

def _gate(sym, ms0, ms1):
    base = _base(sym)
    res = resolve("gate", base)
    if res is None: raise NotListed()
    name, div = (res if res != "guess" else (base + "_USDT", 1.0))
    rows, s = [], ms0
    while s < ms1:
        e = min(s + 5 * 86400_000, ms1)
        r = requests.get("https://api.gateio.ws/api/v4/futures/usdt/candlesticks",
                         params={"contract": name, "interval": "5m", "from": s // 1000, "to": e // 1000}, timeout=20).json()
        if isinstance(r, list): rows += [(float(x["t"]) * 1000, x["o"], x["h"], x["l"], x["c"], x.get("v", 0)) for x in r]
        s = e
        time.sleep(0.1)
    return _scale(_frame(rows, ms0, ms1), div)

def _bitget(sym, ms0, ms1):
    base = _base(sym)
    res = resolve("bitget", base)
    if res is None: raise NotListed()
    name, div = (res if res != "guess" else (base + "USDT", 1.0))
    r = requests.get("https://api.bitget.com/api/v2/mix/market/candles", timeout=20,
                     params=dict(symbol=name, productType="USDT-FUTURES", granularity="5m", startTime=ms0, endTime=ms1, limit=1000))
    d = r.json().get("data") if r.status_code == 200 else None
    if not d: raise RuntimeError("no bitget data")
    return _scale(_frame([(x[0], x[1], x[2], x[3], x[4], x[5]) for x in d], ms0, ms1), div)

def _bingx(sym, ms0, ms1):
    base = _base(sym)
    res = resolve("bingx", base)
    if res is None: raise NotListed()
    name, div = (res if res != "guess" else (base + "-USDT", 1.0))
    r = requests.get("https://open-api.bingx.com/openApi/swap/v3/quote/klines", timeout=20,
                     params=dict(symbol=name, interval="5m", startTime=ms0, endTime=ms1, limit=1440))
    d = r.json().get("data") if r.status_code == 200 else None
    if not d: raise RuntimeError("no bingx data")
    return _scale(_frame([(x["time"], x["open"], x["high"], x["low"], x["close"], x.get("volume", 0)) for x in d], ms0, ms1), div)

def fetch_window(sym, t0, t1, min_frac=MIN_FRAC):
    """Returns (data, source), or (None, None). A source with all its candles is used as is. A thin coin
    (candles missing because nothing traded) is forward-filled, but only if it traded in the last 30 minutes."""
    ms0, ms1 = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    need = (ms1 - ms0) / 300_000
    grid = pd.date_range(start=pd.to_datetime(ms0, unit="ms", utc=True).ceil("5min"),
                         end=pd.to_datetime(ms1, unit="ms", utc=True), freq="5min")
    notes = []
    for name, f in (("binance", _binance), ("bybit", _bybit), ("mexc", _mexc), ("okx", _okx), ("gate", _gate),
                    ("bitget", _bitget), ("bingx", _bingx)):
        tag = {"binance": "bn", "bybit": "by", "mexc": "m", "okx": "o", "gate": "g", "bitget": "bg", "bingx": "bx"}[name]
        try:
            df = f(sym, ms0, ms1)
        except NotListed:
            notes.append(tag + ":no"); continue
        except Exception:
            notes.append(tag + ":x"); continue
        if df is None or len(df) < 60:
            notes.append(f"{tag}:x" if df is None or len(df) == 0 else f"{tag}:{len(df)}bars"); continue
        if (len(df) - 1) >= min_frac * need:
            NOTES.pop(sym, None)
            return df.iloc[:-1], name
        age_min = (pd.Timestamp(t1) - df.index[-1]).total_seconds() / 60
        if age_min > 30:
            notes.append(f"{tag}:stale"); continue
        df2 = df.reindex(grid).ffill().dropna(subset=["c"])
        if len(df2) >= 290:
            NOTES.pop(sym, None)
            return df2.iloc[:-1], name + "-gapfill"
        notes.append(f"{tag}:{int(100 * len(df) / need)}%")
    NOTES[sym] = " ".join(notes)
    return None, None

# ---------------- measurements ----------------
def pct(x, nd=1):
    if x is None: return "n/a"
    if abs(x) < 0.05: return f"{0:.{nd}f}%"
    return f"{x:+.0f}%" if abs(x) >= 100 else f"{x:+.{nd}f}%"

# ---------------- UNCONFIRMED SETTINGS ----------------
# The V4A+ framework gives NO numbers for these. They are provisional script values, NOT your rules.
# Each one is "UNCONFIRMED - USER DECISION REQUIRED". Change a value only when you decide it.
P = dict(
    MACD=(12, 26, 9),      # MACD settings (PDF does not give them)
    SWEEP_LOOKBACK=20,     # 15m candles whose high must be exceeded for a sweep
    SWEEP_AGE=16,          # sweep must be within the last 16 15m candles (4h)
    DISP_BODY_FRAC=0.6,    # displacement: body at least 60% of the candle range ("body > wicks")
    DISP_BODY_X=1.5,       # displacement: body at least 1.5x the average body of the prior 10 candles
    DISP_WINDOW=6,         # displacement must come within 6 candles of the sweep
    STOP_BUFFER=0.001,     # stop = sweep wick high + 0.1%
    OI_FLAT=1.0,           # OI change under 1% over 1 hour = flat
    OI_SPIKE=3.0,          # OI change of 3%+ over 1 hour = spike/drop (exhaustion signal)
    FIB_TOL=0.03,          # tolerance around the 0.382 - 0.618 retracement zone
    VOL_CLIMAX_X=2.0,      # volume climax = 2x the prior 20-candle average (15m)
    MICRO_CHOCH="5m close below the previous 5m low after the zone touch",   # stand-in for the 1m/5m CHoCH
)
FUND_TOP_PCT = 5.0         # funding "extreme" = top 5% of eligible coins (approved Oct 22, ranking only)
FUND_MIN_VOL = 1_000_000   # funding ranking only counts coins with 1M+ USD 24h volume (approved Oct 22)
REV_MIN_PCT = 10.0         # reversal signs need 10%+ in 24h (approved Oct 22)
REV_MIN_PCT = 10.0         # reversal signs need 10%+ in 24h (approved Oct 22)
MIN_RR = 2.1               # from the framework (confirmed)
LEV_STD, LEV_FVG = 10, 15  # PDF: 10-13x standard, 15x on 7 + FVG. 10x used for token math (UNCONFIRMED)
MARGIN_MAX = 0.20          # PDF: margin maximum 20%
ACCOUNT_USD = float(os.environ.get("ACCOUNT_USD") or 0)   # your account size, for token counts (not supplied yet)
RANK = {"PRE-TOP WATCH": 1, "EXHAUSTION": 2, "WATCH ALERT": 3, "ENTRY ALERT": 4}

def to_tf(d5, rule, mins):
    agg = {"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}
    r = d5.resample(rule, label="left", closed="left").agg(agg).dropna()
    cnt = d5.c.resample(rule, label="left", closed="left").count()
    if len(r) and len(cnt) and r.index[-1] == cnt.index[-1] and cnt.iloc[-1] < mins // 5: r = r.iloc[:-1]
    return r

def macd_hist(c):
    f, s, g = P["MACD"]
    if len(c) < 12: return None
    ln = c.ewm(span=f, adjust=False).mean() - c.ewm(span=s, adjust=False).mean()
    return ln - ln.ewm(span=g, adjust=False).mean()

def hist_now(d):
    h = macd_hist(d.c)
    return None if h is None else float(h.iloc[-1])

def pivots(arr, hi=True, w=2):
    out = []
    for i in range(w, len(arr) - w):
        seg = arr[i - w:i + w + 1]
        if (hi and arr[i] == seg.max()) or ((not hi) and arr[i] == seg.min()): out.append((i, float(arr[i])))
    return out

def btc_state(now):
    """BTC 4H bias. Returns dict or None."""
    d, _ = fetch_window("BTCUSDT", now - pd.Timedelta(days=3), now)
    if d is None: return None
    h4, h1 = to_tf(d, "4h", 240), to_tf(d, "1h", 60)
    a, b = macd_hist(h4.c), macd_hist(h1.c)
    if a is None or b is None or len(a) < 3: return None
    cur, p1, p2 = float(a.iloc[-1]), float(a.iloc[-2]), float(a.iloc[-3])
    return dict(h4=cur, h1=float(b.iloc[-1]), unclear=((cur > 0) != (p1 > 0)) or ((p1 > 0) != (p2 > 0)),
                expanding=abs(cur) > abs(p1))

def fetch_oi(sym):
    """Open interest history from OKX (5m). Returns a Series or None. Only OKX-listed coins have it."""
    try:
        r = requests.get("https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history",
                         params=dict(instId=f"{sym.replace('USDT', '')}-USDT-SWAP", period="5m", limit=100), timeout=15).json()
        rows = []
        for x in (r.get("data") or []):
            val = x[2] if len(x) >= 3 and x[2] not in ("", None) else x[1]
            rows.append((pd.to_datetime(int(x[0]), unit="ms", utc=True), float(val)))
        if len(rows) < 14: return None
        return pd.Series(dict(rows)).sort_index()
    except Exception:
        return None

def oi_change_1h(oi):
    if oi is None or len(oi) < 14: return None
    base = float(oi.iloc[-13])
    return (float(oi.iloc[-1]) / base - 1) * 100 if base > 0 else None

def oi_class(chg, p1h):
    """returns (label, points). Short side. UNCONFIRMED: the framework defines OI up + price up = +2, OI up + price stalls = reversal,
    OI down on the break = -2, OI flat = 0 = no trade. For a short the 'OI up + price down' mirror is assumed."""
    if chg is None: return ("unverified", None)
    if abs(chg) < P["OI_FLAT"]: return ("flat", 0)
    if chg < 0: return ("OI down (fakeout)", -2)
    if p1h < -0.3: return ("OI up + price down", 2)
    if abs(p1h) <= 0.3: return ("OI up + price stalls (reversal/trap)", 0)
    return ("OI up + price up", 0)

def detect_short(m15, d5):
    """Sweep -> displacement -> MSB -> bearish FVG, newest first. Returns a dict or None."""
    n = len(m15)
    LB = P["SWEEP_LOOKBACK"]
    if n < LB + 20: return None
    O, H, L, C, V = [m15[k].values for k in "ohlcv"]
    for s in range(n - 1, max(n - 1 - P["SWEEP_AGE"], LB - 1), -1):
        prior = H[s - LB:s].max()
        if not (H[s] > prior and C[s] < prior): continue
        lvl = L[max(0, s - 12):s].min()                      # external swing low that must be broken by a body close
        for k in range(s, min(s + P["DISP_WINDOW"] + 1, n - 1)):
            rg = H[k] - L[k]
            if rg <= 0 or C[k] >= O[k]: continue
            body = O[k] - C[k]
            avgb = np.abs(C[max(0, k - 10):k] - O[max(0, k - 10):k]).mean() if k > 0 else 0
            vm = V[max(0, k - 5):k].mean() if k > 0 else 0
            if body / rg < P["DISP_BODY_FRAC"] or body < P["DISP_BODY_X"] * avgb: continue
            if vm <= 0 or V[k] < 1.2 * vm or C[k] >= lvl: continue
            if not (L[k - 1] > H[k + 1]): continue          # no bearish FVG
            top, bot = float(L[k - 1]), float(H[k + 1])
            wick = float(H[s:k + 1].max())
            if H[k + 1:].max() > wick: continue              # swept again: old setup
            t_form = m15.index[k + 1] + pd.Timedelta(minutes=15)
            ob = None
            for j in range(k - 1, max(s - 4, 0) - 1, -1):
                if C[j] > O[j]: ob = (float(L[j]), float(H[j])); break
            return dict(s=s, k=k, lvl=float(lvl), wick=wick, top=top, bot=bot, mid=(top + bot) / 2, t_form=t_form,
                        vol_pct=(V[k] / vm - 1) * 100, legL=float(L[s:].min()), legH=float(H[s]),
                        zone=bool(ob and ob[0] <= top and ob[1] >= bot))
    return None

def exhaustion_signals(m15, h1, oi_chg, swept):
    """The 7 signals in framework section 8. Returns (met, computable, names_met). HTF resistance cannot be computed (chart)."""
    met, comp, names = 0, 0, []
    def add(ok, name):
        nonlocal met, comp
        if ok is None: return
        comp += 1
        if ok: met += 1; names.append(name)
    v = m15.v.values
    if len(v) >= 30:
        base = v[-28:-8].mean()
        add(base > 0 and v[-8:].max() >= P["VOL_CLIMAX_X"] * base, "volume climax")
    else: add(None, "")
    hh = macd_hist(m15.c)
    if hh is not None and len(m15) > 40:
        pv = pivots(m15.h.values[-40:], True)
        hv = hh.values[-40:]
        add(len(pv) >= 2 and pv[-1][1] > pv[-2][1] and hv[pv[-1][0]] < hv[pv[-2][0]], "momentum divergence")
    else: add(None, "")
    add(None if oi_chg is None else abs(oi_chg) >= P["OI_SPIKE"], "OI spike/drop")
    last = m15.iloc[-6:]
    rg = (last.h - last.l).replace(0, np.nan)
    add(bool((((last.h - np.maximum(last.o, last.c)) / rg) >= 0.5).sum() >= 2), "wicks")
    c1 = h1.c.values
    if len(c1) >= 7:
        rec, prior = c1[-1] / c1[-4] - 1, c1[-4] / c1[-7] - 1
        add(prior > 0 and rec < 0.3 * prior, "speed shift")
    else: add(None, "")
    add(bool(swept), "sweep + rejection")
    return met, comp, names

def has_sweep(m15):
    LB, n = P["SWEEP_LOOKBACK"], len(m15)
    H, C = m15.h.values, m15.c.values
    for i in range(max(LB, n - P["SWEEP_AGE"]), n):
        prior = H[i - LB:i].max()
        if H[i] > prior and C[i] < prior: return True
    return False

def pdh_route(d5):
    """Route A (approved Oct 22): price traded above the previous day's high, and the last completed 15m candle closed back below it.
    'Day' = the exchange daily candle (UTC midnight), the same 1D candle as on the charts. Close only, no minimum distance."""
    day0 = d5.index[-1].floor("D")
    prev = d5[(d5.index >= day0 - pd.Timedelta(days=1)) & (d5.index < day0)]
    today = d5[d5.index >= day0]
    if len(prev) < 230 or len(today) < 3: return None
    pdh = float(prev.h.max())
    m15 = to_tf(today, "15min", 15)
    if len(m15) < 1 or not (float(m15.h.max()) > pdh) or not (float(m15.c.iloc[-1]) < pdh): return None
    return dict(level=pdh, above=(float(m15.h.max()) / pdh - 1) * 100)

def build_signal(c, d5, btc, now):
    """Adds the V4A+ short analysis to the price metrics. Returns a signal dict or None."""
    m15, h1 = to_tf(d5, "15min", 15), to_tf(d5, "1h", 60)
    if len(m15) < 40 or len(h1) < 8: return None
    det = detect_short(m15, d5)
    pct_ptw = c["ptw"]                       # 20%+ in 24h, or reversal signs (approved Oct 22)
    pdh = pdh_route(d5)                      # previous-day high swept + 15m close back below (approved Oct 22)
    ptw = pct_ptw or det is not None or pdh is not None   # any route qualifies (F3 setup approved Oct 22)
    if not ptw: return None
    routes = ([c["why"]] if c.get("why") else []) + (["F3 setup"] if det is not None else []) + (["prev-day high swept"] if pdh else [])
    sym = c["sym"].split(" ")[0]
    price = c["price"]
    oi = fetch_oi(sym + "USDT") if (det or ptw) else None
    oi_chg = oi_change_1h(oi)
    cls, oipts = oi_class(oi_chg, c["h1"])
    swept = det is not None or has_sweep(m15)
    ex_met, ex_comp, ex_names = exhaustion_signals(m15, h1, oi_chg, swept) if ptw else (0, 0, [])
    sig = dict(c=c, stage="PRE-TOP WATCH", setup="", routes=routes, pdh=pdh, fund=None, ex=(ex_met, ex_comp, ex_names), oi=(cls, oipts, oi_chg),
               blocked=[], det=None)
    rolling = bool(len(h1) >= 2 and h1.h.iloc[-1] < h1.h.iloc[-2] and h1.c.iloc[-1] < h1.c.iloc[-2])   # 1H rolling over (UNCONFIRMED: old text says "by eye")
    sig["rolling"] = rolling
    if ptw and ex_met >= 2: sig["stage"] = "EXHAUSTION"
    if ptw and ex_met >= 3 and rolling: sig["stage"] = "WATCH ALERT"
    base_stage = sig["stage"]
    if det is None:
        if not ptw: return None
        return sig
    # ---- setup found: sweep + displacement + MSB + FVG. Work out status, levels, score ----
    after = d5[d5.index >= det["t_form"]]
    stop = det["wick"] * (1 + P["STOP_BUFFER"])
    if len(after) and after.h.max() >= stop: return sig if ptw else None
    mid = det["mid"]
    touched = bool(len(after) and after.h.max() >= mid)
    if touched and price < mid * 0.99: return sig if ptw else None      # already tapped and gone: new sweep + new FVG needed
    status = "waiting for retrace to 50% FVG"
    stage = "WATCH ALERT"
    if touched:
        status = "price at the FVG, waiting for a 5m close + 1m/5m micro CHoCH"
        l5 = d5.iloc[-2:]
        if len(l5) == 2 and l5.c.iloc[-1] < l5.o.iloc[-1] and l5.c.iloc[-1] < l5.l.iloc[-2]:
            stage, status = "ENTRY ALERT", "retraced to 50% FVG + 5m close below prior 5m low. Confirm the 1m/5m micro CHoCH on the chart (framework)"
    entry = mid
    risk = stop - entry
    if risk <= 0: return sig if ptw else None
    # targets: natural levels below entry = 1H swing lows + 24h low
    lows = [v for _, v in pivots(h1.l.values, False) if v < entry * 0.999] + ([float(d5.l.iloc[-288:].min())] if d5.l.iloc[-288:].min() < entry * 0.999 else [])
    tps = sorted(set(round(x, 12) for x in lows), reverse=True)
    blocked = []
    if not tps: blocked.append("no liquidity target below entry")
    tp1 = tps[0] if tps else None
    tp2 = tps[1] if len(tps) > 1 else None
    rr1 = (entry - tp1) / risk if tp1 else None
    if rr1 is not None and rr1 < MIN_RR: blocked.append(f"natural R:R {rr1:.1f} is below 2.1R")
    rr_blend = ((0.7 * (entry - tp1) + 0.3 * (entry - tp2)) / risk if tp1 and tp2 else None)
    # refetch 3 days for a usable 4H MACD
    d3, _ = fetch_window(sym + "USDT", now - pd.Timedelta(days=3), now)
    h4 = to_tf(d3 if d3 is not None else d5, "4h", 240)
    hs = (hist_now(h4), hist_now(h1), hist_now(m15))
    macd_ok = all(x is not None and x < 0 for x in hs)
    # confluences (short side). Each 1 point, OI per the framework (+2 / 0 / -2).
    rng = det["legH"] - det["legL"]
    ratio = (entry - det["legL"]) / rng if rng > 0 else None
    fib_ok = ratio is not None and (0.382 - P["FIB_TOL"]) <= ratio <= (0.618 + P["FIB_TOL"])
    c1 = bool(btc and not btc["unclear"] and btc["h4"] < 0 and btc["h1"] < 0)
    c3 = det["top"] >= det["lvl"] * 0.985            # FVG reaches back up to the broken swing low (S/R flip stand-in)
    c4 = det["vol_pct"] >= 20
    c6 = det["zone"]
    pts = [("BTC", c1), ("MACD", macd_ok), ("S/R", c3), ("Vol", c4), ("Fib", fib_ok), ("Zone", c6)]
    # weekly bias (scanner only, approved Oct 22): rolling 7-day change, a BONUS outside the point score. Down = aligned for a short = bonus; up = shown, no penalty. UNCONFIRMED.
    wk = None
    try:
        t7 = now - pd.Timedelta(days=7)
        dw, _ = fetch_window(sym + "USDT", t7 - pd.Timedelta(hours=6), t7)
        if dw is not None and len(dw): wk = (price / float(dw.c.iloc[-1]) - 1) * 100
    except Exception:
        wk = None
    wk_pts = None if wk is None else (1 if wk < 0 else -1)
    score = sum(1 for _, ok in pts if ok) + (oipts or 0)      # weekly is NOT in the points: it is a bonus tag only
    tp2_ext = False
    if wk_pts == 1 and score >= 7 and len(tps) > 2:           # weekly bonus changes TP2 only (approved Oct 22): runner targets the next natural level lower
        tp2, tp2_ext = tps[2], True
        rr_blend = (0.7 * (entry - tp1) + 0.3 * (entry - tp2)) / risk
    if score <= 6: blocked.append(f"score {score} = no trade (needs 7+)")
    if btc is None or btc["unclear"]: blocked.append("BTC 4H unclear / mid-flip")
    if cls == "flat": blocked.append("OI flat")
    if cls == "OI up + price stalls (reversal/trap)": blocked.append("OI up + price stalls = trap")
    if c["day"] >= BIG_MOVE_PCT:
        ph = pivots(h1.h.values, True)
        if not (len(ph) >= 2 and ph[-1][1] < ph[-2][1] and price < ph[-1][1]):
            blocked.append("70%+ move needs 1H lower high + failed reclaim (not met)")
    tier = ("7 + FVG = FULL 100%" if score >= 7 else f"{score} = NO TRADE") + (" + WEEKLY BONUS" if (wk_pts == 1 and score >= 7) else "")
    sig.update(wk=(wk, wk_pts), tp2_ext=tp2_ext, det=det, entry=entry, stop=stop, tp1=tp1, tp2=tp2, rr1=rr1, rr_blend=rr_blend, status=status, score=score,
               tier=tier, pts=pts, hs=hs, blocked=blocked, fvg=(det["bot"], det["top"]), lev=LEV_FVG,
               aplus=bool(touched and score >= 7 and oipts == 2 and c1))
    sig["stage"] = stage
    if blocked: sig["stage"] = base_stage if ptw else None   # NO TRADE: kept visible only for coins already on PRE-TOP WATCH
    if sig["stage"] is None: return None
    sig["setup"] = ("F3 [A+]" if sig["aplus"] else "F3 (4 of 4)") if touched else "F3 (3 of 4: retrace pending)"
    return sig

def tokens(sig):
    if not ACCOUNT_USD or not sig.get("entry"): return ""
    frac = 1.0 if sig["score"] >= 7 else 0
    if frac == 0: return ""
    n = ACCOUNT_USD * MARGIN_MAX * frac * sig["lev"] / sig["entry"]
    return f"\n   Tokens {n:.6g} total: TP1 {0.7 * n:.6g} (70%) | TP2 {0.3 * n:.6g} (30%)"

ICON = {"PRE-TOP WATCH": "\U0001F440", "EXHAUSTION": "\U0001F7E1", "WATCH ALERT": "\U0001F7E0", "ENTRY ALERT": "\U0001F534"}
NEWTAG = "  \U0001F195"
STARTAG = " \u2605 top 5%"
APTAG = "  \u2B50 A+ gate"
def yn(b): return "\u2705" if b else "\u274C"

def sig_text(i, g):
    """One compact card per coin. Same rows every time so it can be scanned by eye on a phone."""
    c = g["c"]
    t = f"{i}. {c['sym']}  SHORT  {ICON[g['stage']]} {g['stage']}{NEWTAG if g.get('new') else ''}\n"
    moves = f"24h {pct(c['day'])} | 4H {pct(c['h4'])} | 1H {pct(c['h1'])} | 15m {pct(c['m15'])}"
    why = " + ".join(g["routes"]) or "?"
    extra = ""
    if g.get("fund"):
        fr, fx, fe = g["fund"]
        extra += f"Funding {fr:+.4f}%{STARTAG if fe else ''}\n"
    if g.get("pdh"):
        extra += f"Prev-day high {g['pdh']['level']:.6g} swept +{g['pdh']['above']:.1f}%, 15m closed back below\n"
    if g["det"] is None or g["blocked"]:
        t += moves + "\n"
        t += f"Why: {why}" + (f" | {g['setup']}" if g.get("setup") else "") + "\n"
        if g["det"] is None:
            m, cp, nm = g["ex"]
            if g["stage"] in ("EXHAUSTION", "WATCH ALERT"):
                t += f"Signals {m}/7 ({cp} measurable): {', '.join(nm) or 'none'}\n"
            if g["stage"] == "WATCH ALERT": t += "1H rolling over \u2192 set the TradingView alert on the chart\n"
            if c["big"]: t += "70%+ move: needs 1H lower high + failed reclaim\n"
        else:
            t += "\u26D4 NO TRADE: " + "; ".join(g["blocked"]) + "\n"
        return t + extra + f"Price {c['price']:.6g}\n"
    oicls, oipts, oichg = g["oi"]
    oit = (oicls if oicls.startswith("OI") else f"OI {oicls}") + (f" {oichg:+.1f}%/1h" if oichg is not None else "") + (f" ({oipts:+d})" if oipts is not None else "")
    dec = lambda x: ("\u25BC" if x is not None and x < 0 else "\u25B2" if x is not None else "?")
    wk, wkp = g["wk"]
    wkt = "n/a" if wk is None else (f"{wk:+.0f}% bonus \u2705" if wkp == 1 else f"{wk:+.0f}% against, no penalty")
    t += (f"Grade {g['score']}: {g['tier']}{APTAG if g['aplus'] else ''}\n"
          f"Entry {g['entry']:.6g}  (50% FVG {g['fvg'][0]:.6g}-{g['fvg'][1]:.6g})\n"
          f"Stop  {g['stop']:.6g}\n"
          f"TP1   {g['tp1']:.6g}  (70%)\n"
          + (f"TP2   {g['tp2']:.6g}  (30%{', extended: weekly bonus' if g.get('tp2_ext') else ''})\n" if g["tp2"] else "TP2   n/a (no lower natural level)\n")
          + f"R:R {g['rr1']:.1f} to TP1" + (f" | {g['rr_blend']:.1f} blended" if g["rr_blend"] else "") + " | up to 15x | margin max 20%\n"
          + (tokens(g).strip() + "\n" if tokens(g) else "")
          + f"{g['status']}\n"
          + "Checks: " + " ".join(f"{n} {yn(b)}" for n, b in g["pts"]) + f" | MACD 4H{dec(g['hs'][0])} 1H{dec(g['hs'][1])} 15m{dec(g['hs'][2])}\n"
          + f"{oit} | Weekly {wkt}\n"
          + moves + "\n" + f"Why: {why} | {g['setup']}\n" + extra + f"Price {c['price']:.6g} [{SRC_TAG.get(c.get('src'), '?')}]\n")
    return t

# ---------------- funding (ranking only) ----------------
def load_funding(coins):
    """Bulk funding from Bitget, Gate, MEXC (first that has the coin). Binance/Bybit are blocked from GitHub; OKX/BingX have no bulk
    endpoint with volume. Rate = the exchange's current rate per its own funding interval (usually 8h; NOT normalised - UNCONFIRMED).
    Returns {SYMBOL: (rate_pct, exchange, volume_usd)}."""
    def get(url, **kw):
        try:
            r = requests.get(url, timeout=20, **kw)
            return r.json() if r.status_code == 200 else None
        except Exception: return None
    tables = {}
    j = get("https://api.bitget.com/api/v2/mix/market/tickers", params=dict(productType="USDT-FUTURES"))
    if isinstance(j, dict) and isinstance(j.get("data"), list):
        tables["bitget"] = {x.get("symbol"): (x.get("fundingRate"), x.get("usdtVolume") or x.get("quoteVolume")) for x in j["data"]}
    j = get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
    if isinstance(j, list):
        tables["gate"] = {x.get("contract"): (x.get("funding_rate"), x.get("volume_24h_quote")) for x in j}
    j = get("https://contract.mexc.com/api/v1/contract/ticker")
    if isinstance(j, dict) and isinstance(j.get("data"), list):
        tables["mexc"] = {x.get("symbol"): (x.get("fundingRate"), x.get("amount24")) for x in j["data"]}
    out = {}
    for sym in coins:
        base = _base(sym)
        for ex in ("bitget", "gate", "mexc"):
            t = tables.get(ex)
            if not t: continue
            r = resolve(ex, base)
            if not isinstance(r, tuple) or r[0] not in t: continue
            try: rate, vol = float(t[r[0]][0]) * 100, float(t[r[0]][1])
            except (TypeError, ValueError): continue
            out[sym] = (rate, ex, vol); break
    print("funding loaded:", {k: (len(v) if v else None) for k, v in tables.items()}, "coins with funding:", len(out))
    return out

def funding_extreme_set(fund):
    """Top 5% of positive funding among coins with 1M+ USD 24h volume."""
    el = sorted((v[0] for v in fund.values() if v[2] >= FUND_MIN_VOL and v[0] > 0), reverse=True)
    if not el: return set(), None
    cut = el[max(0, int(len(el) * FUND_TOP_PCT / 100 + 0.999) - 1)]
    return {k for k, v in fund.items() if v[2] >= FUND_MIN_VOL and v[0] >= cut and v[0] > 0}, cut

# ---------------- one coin ----------------
BTC = None
def evaluate(sym, now):
    """returns (metrics dict or None, got_usable_data, signal or None)"""
    d5, src = fetch_window(sym, now - pd.Timedelta(days=SCAN_DAYS), now)
    if d5 is None: return None, False, None
    C = d5.c.values
    if len(C) < 289:
        NOTES[sym] = "under 24h of history"; return None, False, None
    price = float(C[-1])
    back = [float(C[-1 - n]) for n in (288, 48, 12, 3)]
    if any(not np.isfinite(v) or v <= 0 for v in [price] + back): return None, False, None
    day, h4, h1, m15 = [(price / b - 1) * 100 for b in back]
    if src and src.startswith("binance") and BN_KIND.get(sym) == "spot": src = src.replace("binance", "binance-spot", 1)
    if src and src.startswith("bybit") and BY_KIND.get(sym) == "spot": src = src.replace("bybit", "bybit-spot", 1)
    b0 = sym.replace("USDT", "")
    m = dict(sym=b0 + (f" ({ALIAS[b0]})" if b0 in ALIAS else ""), price=price, day=day, h4=h4, h1=h1, m15=m15, src=src,
             big=(day >= BIG_MOVE_PCT), ptw=False, why="")
    if day >= PTW_PCT: m["ptw"], m["why"] = True, "24h 20%+"
    elif day >= REV_MIN_PCT and h4 > 0 and (h1 < 0 or m15 < 0): m["ptw"], m["why"] = True, "reversal: 4H up, 1H/15m red, 24h 10%+"
    sig = None
    try:
        sig = build_signal(m, d5, BTC, now)
    except Exception as e:
        print("engine error", sym, type(e).__name__, e)
    return m, True, sig

# ---------------- alerts ----------------
def push(title, text, priority="default", tags="bar_chart"):
    """Returns True only if ntfy accepted the message."""
    if len(text.encode("utf-8")) > MAX_BYTES:
        t, limit = text, MAX_BYTES - 10
        while len(t.encode("utf-8")) > limit and "\n" in t: t = t.rsplit("\n", 1)[0]
        if len(t.encode("utf-8")) > limit: t = t.encode("utf-8")[:limit].decode("utf-8", "ignore")
        text = t.rstrip() + "\n...(cut)"
    try: print("PUSH:", title, "\n" + text)
    except Exception: pass
    if not TOPIC:
        print("(no NTFY_TOPIC set, not sent)"); return False
    try:
        r = requests.post(f"https://ntfy.sh/{TOPIC}", data=text.encode("utf-8"),
                          headers={"Title": title, "Priority": priority, "Tags": tags}, timeout=20)
        if r.status_code >= 300:
            print("push rejected:", r.status_code, r.text[:200]); return False
        return True
    except Exception as e:
        print("push failed:", e); return False

STAGES = "Stages: PRE-TOP WATCH -> EXHAUSTION -> WATCH ALERT -> ENTRY ALERT."
EXITS = ("EXITS: stop hit | BTC 4H flips | OI drops + stalls | FVG fails | opposite liquidity swept | volume does not confirm. "
         "+15-20% PnL: close 35% + stop to breakeven. TP1: close 70% + stop to breakeven, then manage the 30% runner. Never widen the stop. 2 losses = kill switch.")
UNCONF = ("UNCONFIRMED (provisional script values): sweep, displacement, MSB, S/R flip, supply/demand, OI flat, 1H rolling, MACD, score, weekly. "
          "Chart check: choppy 15m, 1m/5m micro CHoCH, HTF resistance, USD event, open positions, correlation.")

def main():
    global BTC
    now = pd.Timestamp.now(tz="UTC")
    load_instruments()
    try: FUND = load_funding(ALL_COINS)
    except Exception as e: print("funding error", e); FUND = {}
    try: fx_set, fx_cut = funding_extreme_set(FUND)
    except Exception as e: print("funding rank error", e); fx_set, fx_cut = set(), None
    try: BTC = btc_state(now)
    except Exception as e: print("btc error", e); BTC = None
    try:
        with open(STATE_FILE) as f: state = json.load(f)
    except Exception: state = {}
    old = state.get("sig", {})
    if not isinstance(old, dict) or any(not isinstance(v, str) for v in old.values()): old = {}
    state["sig"] = old
    state.pop("ptw", None)
    results, sigs, with_data, missing = [], [], 0, []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(evaluate, sym, now): sym for sym in ALL_COINS}
        for fut in cf.as_completed(futs):
            try:
                out, ok, sig = fut.result()
                with_data += 1 if ok else 0
                if not ok: missing.append(futs[fut])
                if out: results.append(out)
                if sig: sigs.append(sig)
            except Exception as e:
                print("error", futs[fut], type(e).__name__, e)

    seen = state["sig"]
    for g in sigs:
        k_ = g["c"]["sym"].split(" ")[0] + "USDT"
        f = FUND.get(k_)
        g["fund"] = (f[0], f[1], k_ in fx_set) if f else None
    ptws = sorted((c for c in results if c["ptw"]), key=lambda c: (-c["day"], c["sym"]))
    def oi_pts(g): return (g["oi"][1] or 0)
    sigs.sort(key=lambda g: (-RANK[g["stage"]], -(g.get("score") or 0), -(g.get("rr1") or 0), -len(g["routes"]), -oi_pts(g),
                             -(1 if g["fund"] and g["fund"][2] else 0), -g["c"]["day"]))
    top = sigs[:6]
    def key(g): return f"{RANK[g['stage']]}|{g.get('entry', 0):.6g}"
    for g in top:
        prev = seen.get(g["c"]["sym"])
        g["new"] = prev is None or RANK[g["stage"]] > int(prev.split("|")[0]) or (RANK[g["stage"]] >= 3 and prev != key(g))
    fresh = [g for g in top if g["new"]]
    print(f"coins scanned {len(ALL_COINS)} | usable {with_data} | up {PTW_PCT:.0f}%+: {len(ptws)} | signals {len(sigs)} | top {len(top)} | new {len(fresh)}")
    missing.sort()
    tagx = {"bn": "binance", "by": "bybit", "m": "mexc", "o": "okx", "g": "gate", "bg": "bitget", "bx": "bingx"}
    def not_listed(m):
        toks = NOTES.get(m, "").split()
        loaded = [t for t in toks if INSTR.get(tagx.get(t.split(":")[0])) is not None]
        return bool(loaded) and all(t.endswith(":no") for t in loaded)
    names = {n for v in INSTR.values() if v for n in v}
    for n in list(names):
        for p, _ in PFX:
            if p and n.startswith(p): names.add(n[len(p):])
    def similar(m):
        c = difflib.get_close_matches(m.replace("USDT", ""), sorted(names), n=2, cutoff=0.66)
        return f" (similar: {', '.join(c)})" if c else ""
    nl = [m for m in missing if not_listed(m)]
    nodata = [m for m in missing if m not in nl]
    miss_txt = ""
    if nl: miss_txt += f"NOT FOUND on {', '.join(k for k, v in INSTR.items() if v)} ({len(nl)}): " + " | ".join(m.replace("USDT", "") + (f" [{KNOWN[m[:-4]]}]" if m[:-4] in KNOWN else similar(m)) for m in nl)
    if nodata: miss_txt += ("\n" if miss_txt else "") + f"LISTED BUT NO DATA ({len(nodata)}) (bn=Binance by=Bybit m=MEXC o=OKX g=Gate bg=Bitget bx=BingX; no=not listed, x=no data/blocked, %=share of candles, stale=not trading): " + " | ".join(m.replace("USDT", "") + " " + NOTES.get(m, "?") for m in nodata)
    print("NO DATA:", miss_txt)

    if BTC is None: btc_t = "BTC 4H: no data - treat as UNCLEAR (NO TRADE)"
    elif BTC["unclear"]: btc_t = "BTC 4H: UNCLEAR / mid-flip = NO TRADE"
    else: btc_t = f"BTC 4H: {'bearish, aligned for shorts' if BTC['h4'] < 0 and BTC['h1'] < 0 else 'NOT bearish on 4H+1H (short confluence 1 fails)'} (MACD hist 4H {'expanding' if BTC['expanding'] else 'shrinking'})"
    head = f"ALEX7 | {len(ALL_COINS)} scanned ({with_data} with data)\n{btc_t}\n"
    guess_set = {g + "USDT" for g in GUESS_COINS.split()}
    guess_miss = [m for m in missing if m in guess_set]
    guess_txt = (f"\nGUESS_COINS with no data ({len(guess_miss)}): " + " ".join(m.replace("USDT", "") for m in guess_miss)) if guess_miss else ""
    body_top = ("TOP 6 (ranked: stage, score, R:R)\n\n" + "\n".join(sig_text(i + 1, g) for i, g in enumerate(top))
                if top else "No coin qualifies right now.\n")
    if TEST:
        body = head + "\n" + body_top + "\n" + EXITS + "\n\n" + UNCONF
        body += "\n\n-- DATA CHECK (test only) --\n" + (f"No data for {len(missing)} coins.\n" + miss_txt + guess_txt if missing else "No data for 0 coins")
        sample = sorted(results, key=lambda c: (-c["day"], c["sym"]))[:3]
        body += "\n\nBiggest 24h moves (not alerts):\n" + "\n".join(f"{c['sym']} {pct(c['day'])} | {c['price']:.6g} [{SRC_TAG.get(c.get('src'), '?')}]" for c in sample)
        push("ALEX7 screener TEST", body)
    elif fresh:
        best = max(RANK[g["stage"]] for g in fresh)
        texts = [sig_text(i + 1, g) for i, g in enumerate(top)]
        chunks, cur = [], ""
        for t in texts:                      # ntfy limit 4096 bytes: split into several messages instead of cutting anything
            if cur and len((cur + t).encode("utf-8")) > 2600: chunks.append(cur); cur = ""
            cur += t + "\n"
        chunks.append(cur)
        title = f"ALEX7 {top[0]['stage']}: {top[0]['c']['sym']}" + (f" +{len(top) - 1} more" if len(top) > 1 else "")
        okall = True
        for n, ch in enumerate(chunks):
            body = (head + "\nTOP 6 (ranked: stage, score, R:R)\n\n" if n == 0 else "") + ch
            if n == len(chunks) - 1: body += "\n" + EXITS + "\n" + UNCONF
            okall &= push(title + (f" ({n + 1}/{len(chunks)})" if len(chunks) > 1 else ""), body,
                          priority="urgent" if best == 4 else "high" if best >= 2 else "default", tags="rotating_light" if best == 4 else "eyes")
        if okall:
            for g in fresh: state["sig"][g["c"]["sym"]] = key(g)
    live = {g["c"]["sym"] for g in sigs}
    state["sig"] = {k: v for k, v in state["sig"].items() if k in live}      # a coin that stops qualifying is forgotten; if it returns it alerts again

    ts = now.timestamp()
    if ts - state.get("heartbeat_ts", 0) > 24 * 3600 and not TEST:
        if push("ALEX7 scanner alive", f"{with_data} of {len(ALL_COINS)} coins had usable data. {len(ptws)} on PRE-TOP WATCH by 24h move/reversal. {len(sigs)} tracked.",
                priority="low", tags="white_check_mark"):
            state["heartbeat_ts"] = ts
    if ALL_COINS and with_data < len(ALL_COINS) * 0.8 and ts - state.get("datawarn_ts", 0) > 6 * 3600:
        if push("ALEX7 scanner: data problem", f"Only {with_data} of {len(ALL_COINS)} coins had usable data. Do not trust quiet periods.",
                priority="high", tags="warning"):
            state["datawarn_ts"] = ts
    with open(STATE_FILE, "w") as f: json.dump(state, f)


# ---------------- coin list ----------------
CURRENT_COINS = """BTC ETH BNB XRP SOL ZEC HYPE DOGE XMR LINK ADA XLM NEAR BCH UNI LTC CC AVAX SUI HBAR QNT TAO CRO ENA PUMP AAVE ONDO M WLD MNT DOT SKY ASTER ICP MORPHO PEPE WLFI ETC VVV ARB KAS ALGO JUP JST RENDER FIL ATOM ZRO AERO CAKE INJ VET DASH APT ETHFI FLR PYTH PENGU RAY CRV TRUMP VIRTUAL GRASS SEI TIA PENDLE BSV SPX STRK FF LDO UB KITE SUN BONK GNO GRT OP JTO LUNC ENS CFX FLOKI JASMY ZBCN RUNE WIF EIGEN COMP USELESS THETA AKT KMNO MANA SAND NEO CHZ FARTCOIN IMX MET APE SUPER 1INCH ATH SNX ORCA AIOZ EGLD GLM AWE BEAM PLUME DYDX GALA SOON PROM CHEEMS PEAQ FORM HNT Q DOG KSM ORDI GAS GMX SAFE RED NMR ZETA SPK RIF QUBIC BERA KAITO CROSS BABYDOGE IP MUBARAK BANANAS31 SUSHI TURBO ARC ZIL UAI BIO BOME ENJ ROSE BABY DEXE BAN MERL GPS CTC COAI PHA BLUR DEEP HUMA BRETT TRB JELLYJELLY FLOW PNUT POPCAT NIL TOSHI HOLO DUSK NOT ALCH MASK EDU AVNT MOG MEW CATI SSV MOODENG PEOPLE MOVE SXT MOCA ONG PROVE COTI BAND RATS SYN ALEO MEME UMA LAB APR B3 GIGGLE NEIRO SQD POWR APEX MMT ORBS SIGN FLUX SKYAI STEEM ONE ZORA MANTA CARV CORE KGEN VELVET HIVE FLOCK ZEREBRO CLOUD IOST XAN TNSR CTSI AUCTION ZBT GMT METIS CETUS BIGTIME OPEN USUAL WOO EDEN XNY HEMI ICNT HOME MOVR INIT HYPER SOLV RIVER AIXBT CGPT STG BANK FIDA GIGA CTK BICO CHR ENSO DIA JCT WCT CYBER GRIFFAIN SOPH DODO MIRA PIPPIN GOAT AVA SIREN EPIC SAPIEN GTC MYX CAT SPELL ICX BLESS DOLO ORDER DRIFT BMT CLANKER MAV RARE PONKE DOOD DYM STBL CHILLGUY BTR GLMR GUN GODS HMSTR BLUAI NEWT TRUST COOKIE ACT SAGA BEL SONIC RESOLV TAC LIGHT SWARMS BLAST CLO MITO NOM QUICK TREE HFT"""

# Paste new coins here, separated by spaces (e.g. NEW_COINS = "ABC DEF GHI"). Duplicates are removed.
NEW_COINS = """ZKP AIA SANTOS OMG BASED OPN LAYER KAT BREV PRL ACE BILL FOGO BEAT FRAX OGN YGG ZEST VANA KNC WAVES MTL BNT EUL REZ NES SAHARA MAGIC ME CSPR ANKR XVG CELO IRYS FOLKS SNEK ASTR XVS IO YFI LPT KAVA WAL ARKM MINA QTUM ZRX W ZEN SENT H S ZAMA TWT BAT MON KAIA AR TRX TON SHIB BTW XDC NIGHT STX FET XPL LIT"""

# Added from your screenshots but the ticker is a best guess. If a ticker is wrong the coin is skipped
# (no data) or, rarely, maps to a different coin, so check the price in the alert against the chart.
GUESS_COINS = """TKB FLU ROBO CLUSTER FANC TAKE GRVT CNPY BLOCK XPIN OPG KII TRUTH BAS ACU RAVE ARX AZTEC MEGA ESP MARS STONK CAP SKR CCAT GENIUS PONS PIEVERSE DAPP SLX 1000SATS TRADOOR US"""

ALL_COINS = [s + "USDT" for s in dict.fromkeys((CURRENT_COINS + " " + NEW_COINS + " " + GUESS_COINS).split())]

if __name__ == "__main__":
    main()
