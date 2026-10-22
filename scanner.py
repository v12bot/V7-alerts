# ================= ALEX7 (V4A+) SCREENER - runs on GitHub, pushes to your phone via ntfy =================
# Baseline: the V4A+ Elite Confluence Engine PDF (70/30 edition). Shorts.
# This script only measures PRICE CHANGE. It does not measure OI, MACD, volume, sweep, displacement, MSB, FVG
# or R:R, it does not grade or rank setups, and it never places a trade. The confluence check, the grade and the
# top 6 are done in the daily session on the 4H / 1H / 15m charts.
#
# What it does:
#   1 Measures Day (rolling 24h), 4H, 1H and 15m % change for every coin (no Week).
#   2 PRE-TOP WATCH: any coin up 20%+ in the rolling 24 hours is sent to you. Tracking starts there (shorts).
#     One alert per coin per day.
#   3 Flags a 70%+ 24h move: needs 1H lower high + failed reclaim (PDF F1).
#   4 Runs 24/7 (Toronto time shown on each alert). No session or dead-zone gating: the framework's session rules are applied by you at chart time.
#
# Data: Binance first (perpetual, then spot), then Bybit, MEXC, OKX, Gate. The alert does not say which one was used.
#
# Progression stages (the stage names are NOT in the V4A+ PDF):
#   PRE-TOP WATCH (this script, up 20%+)  ->  EXHAUSTION  ->  WATCH ALERT  ->  ENTRY ALERT
#   The last three are chart reads. The script does not compute them.
import os, json, time, difflib, requests, concurrent.futures as cf
import numpy as np, pandas as pd

SCAN_DAYS = 2          # 5m history pulled per coin (Day needs 24h; a little extra for gaps)
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

# ---------------- data (Binance, Bybit, then MEXC, OKX, Gate) ----------------
NOTES = {}                # why a coin had no data (shown in the test message)
SRC_TAG = {"binance": "BN", "binance-gapfill": "BN~", "mexc": "MX", "mexc-gapfill": "MX~", "okx": "OK", "okx-gapfill": "OK~",
           "gate": "GT", "gate-gapfill": "GT~", "binance-spot": "BNs", "binance-spot-gapfill": "BNs~",
           "bybit": "BY", "bybit-gapfill": "BY~", "bybit-spot": "BYs", "bybit-spot-gapfill": "BYs~"}   # tag shown on each alert line
BN_KIND, BY_KIND = {}, {}  # "perp" or "spot": which feed a coin was read from
BN_ALIAS = {"BEAM": "BEAMX"}   # Binance lists BEAM as BEAMX
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
    for k, fn in (("binance", bn), ("bybit", by), ("mexc", mx), ("okx", ok), ("gate", gt)):
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
    base = sym.replace("USDT", "")
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
    base = sym.replace("USDT", "")
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
    base = sym.replace("USDT", "")
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
    base = sym.replace("USDT", "")
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
    base = sym.replace("USDT", "")
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

def fetch_window(sym, t0, t1, min_frac=MIN_FRAC):
    """Returns (data, source), or (None, None). A source with all its candles is used as is. A thin coin
    (candles missing because nothing traded) is forward-filled, but only if it traded in the last 30 minutes."""
    ms0, ms1 = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    need = (ms1 - ms0) / 300_000
    grid = pd.date_range(start=pd.to_datetime(ms0, unit="ms", utc=True).ceil("5min"),
                         end=pd.to_datetime(ms1, unit="ms", utc=True), freq="5min")
    notes = []
    for name, f in (("binance", _binance), ("bybit", _bybit), ("mexc", _mexc), ("okx", _okx), ("gate", _gate)):
        tag = {"binance": "bn", "bybit": "by", "mexc": "m", "okx": "o", "gate": "g"}[name]
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

# ---------------- measurements (price change only) ----------------
def pct(x, nd=1):
    if x is None: return "n/a"
    if abs(x) < 0.05: return f"{0:.{nd}f}%"
    return f"{x:+.0f}%" if abs(x) >= 100 else f"{x:+.{nd}f}%"

# ---------------- one coin ----------------
def evaluate(sym, now):
    """returns (metrics dict or None, got_usable_data)"""
    d5, src = fetch_window(sym, now - pd.Timedelta(days=SCAN_DAYS), now)
    if d5 is None: return None, False
    C = d5.c.values
    if len(C) < 289:                                # less than 24h of candles
        NOTES[sym] = "under 24h of history"; return None, False
    price = float(C[-1])
    back = [float(C[-1 - n]) for n in (288, 48, 12, 3)]
    if any(not np.isfinite(v) or v <= 0 for v in [price] + back): return None, False
    day, h4, h1, m15 = [(price / b - 1) * 100 for b in back]
    if src and src.startswith("binance") and BN_KIND.get(sym) == "spot": src = src.replace("binance", "binance-spot", 1)
    if src and src.startswith("bybit") and BY_KIND.get(sym) == "spot": src = src.replace("bybit", "bybit-spot", 1)
    return dict(sym=sym.replace("USDT", ""), price=price, day=day, h4=h4, h1=h1, m15=m15, src=src,
                big=(day >= BIG_MOVE_PCT), ptw=(day >= PTW_PCT)), True

# ---------------- alerts ----------------
def push(title, text, priority="default", tags="bar_chart"):
    """Returns True only if ntfy accepted the message."""
    if len(text.encode("utf-8")) > MAX_BYTES:
        t, limit = text, MAX_BYTES - 10
        while len(t.encode("utf-8")) > limit and "\n" in t: t = t.rsplit("\n", 1)[0]
        if len(t.encode("utf-8")) > limit: t = t.encode("utf-8")[:limit].decode("utf-8", "ignore")
        text = t.rstrip() + "\n...(cut)"
    print("PUSH:", title, "\n" + text)
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

def line(c):
    return (f"{c['sym']} {pct(c['day'])} 24h | 4H {pct(c['h4'])} | 1H {pct(c['h1'])} | 15m {pct(c['m15'])} | price {c['price']:.6g} [{SRC_TAG.get(c.get('src'), '?')}]"
            + ("\n   70%+ move: needs 1H lower high + failed reclaim (check on chart)" if c["big"] else ""))

STAGES = "Stages: PRE-TOP WATCH -> EXHAUSTION -> WATCH ALERT -> ENTRY ALERT (the last three are chart reads, not scanned)."
CHECK_FOOTER = ("Check on chart (not scanned): peaked 4H, 1H rolling, 15m CHoCH, sweep, displacement, MSB body close, fresh FVG, "
                "retrace to 50% FVG + 1m/5m CHoCH, BTC 4H, OI, MACD, volume, score 7, natural R:R >= 2.1, stop beyond sweep wick, "
                "TP1/TP2, USD event +-30 min. Alert = notification only.")

def main():
    now = pd.Timestamp.now(tz="UTC")
    load_instruments()
    try:
        with open(STATE_FILE) as f: state = json.load(f)
    except Exception: state = {}
    old = state.get("ptw", {})
    if not isinstance(old, dict) or any("|" in str(k) or not isinstance(v, dict) for k, v in old.items()): old = {}   # old state format: reset
    state["ptw"] = old
    results, with_data, missing = [], 0, []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(evaluate, sym, now): sym for sym in ALL_COINS}
        for fut in cf.as_completed(futs):
            try:
                out, ok = fut.result()
                with_data += 1 if ok else 0
                if not ok: missing.append(futs[fut])
                if out: results.append(out)
            except Exception as e:
                print("error", futs[fut], type(e).__name__, e)

    tnow = now.tz_convert(TZ)
    today = tnow.strftime("%Y-%m-%d")
    today_seen = state["ptw"].get(today, {})
    ptws = sorted((c for c in results if c["ptw"]), key=lambda c: (-c["day"], c["sym"]))
    fresh = [c for c in ptws if c["sym"] not in today_seen]
    print(f"coins scanned {len(ALL_COINS)} | with usable data {with_data} | up {PTW_PCT:.0f}%+ in 24h: {len(ptws)}")
    print(f"PTW fresh={len(fresh)}")
    missing.sort()
    tagx = {"bn": "binance", "by": "bybit", "m": "mexc", "o": "okx", "g": "gate"}
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
    if nl: miss_txt += f"NOT FOUND on {', '.join(k for k, v in INSTR.items() if v)} ({len(nl)}): " + " | ".join(m.replace("USDT", "") + similar(m) for m in nl)
    if nodata: miss_txt += ("\n" if miss_txt else "") + f"LISTED BUT NO DATA ({len(nodata)}) (bn=Binance by=Bybit m=MEXC o=OKX g=Gate; no=not listed, x=no data/blocked, %=share of candles, stale=not trading): " + " | ".join(m.replace("USDT", "") + " " + NOTES.get(m, "?") for m in nodata)
    print("NO DATA:", miss_txt)
    head = (f"Coins scanned: {len(ALL_COINS)} ({with_data} with usable data).\n"
            f"Scan {tnow.strftime('%H:%M')} Toronto time. The scanner runs 24/7; no session gating.\n"
            "(24h = rolling 24h to last closed 5m candle)\n")

    guess_set = {g + "USDT" for g in GUESS_COINS.split()}
    guess_miss = [m for m in missing if m in guess_set]
    guess_txt = (f"\nGUESS_COINS with no data ({len(guess_miss)}): " + " ".join(m.replace("USDT", "") for m in guess_miss)) if guess_miss else ""
    if TEST:
        body = (head + ("No data for " + str(len(missing)) + " coins.\n" + miss_txt if missing else "No data for 0 coins")
                + guess_txt + "\n\n" + STAGES + "\n\n")
        body += ("PRE-TOP WATCH (up 20%+ in 24h):\n" + "\n".join(line(c) for c in ptws[:MAX_PTW])
                 if ptws else "No coin is up 20%+ in 24h right now.")
        sample = sorted(results, key=lambda c: (-c["day"], c["sym"]))[:3]
        body += "\n\nTest sample only (biggest 24h moves, not alerts):\n" + "\n".join(line(c) for c in sample)
        push("ALEX7 screener TEST", body)
    elif fresh:
        lst = fresh[:MAX_PTW]
        body = (head + "\nPRE-TOP WATCH: up 20%+ in 24h. Tracking starts here (shorts). Not an entry.\n" + STAGES
                + "\n\n" + "\n".join(line(c) for c in lst) + "\n\n" + CHECK_FOOTER)
        if push(f"ALEX7 PRE-TOP WATCH: {len(lst)} up 20%+ in 24h", body, priority="high", tags="eyes"):
            for c in lst: state["ptw"].setdefault(today, {})[c["sym"]] = "ptw"
    state["ptw"] = dict(sorted(state["ptw"].items())[-7:])   # keep the last 7 days

    # daily heartbeat (after 09:00 Toronto), marked done only if the push went through
    if tnow.hour >= 9 and state.get("heartbeat") != today and not TEST:
        if push("ALEX7 scanner alive", f"{with_data} of {len(ALL_COINS)} coins had usable data. {len(ptws)} up 20%+ in 24h.",
                priority="low", tags="white_check_mark"):
            state["heartbeat"] = today
    if ALL_COINS and with_data < len(ALL_COINS) * 0.8 and state.get("datawarn") != today:
        if push("ALEX7 scanner: data problem", f"Only {with_data} of {len(ALL_COINS)} coins had usable data. Do not trust quiet periods.",
                priority="high", tags="warning"):
            state["datawarn"] = today
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
