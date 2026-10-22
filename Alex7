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
#   3b Also lists Binance coins NOT on your list that are up 20%+ in Binance's 24h window (one alert per coin per day).
#   4 Shows the session window and dead zone (PDF section 0).
#
# Data: Binance first (perpetual, then spot), then MEXC, OKX, Gate. The alert does not say which one was used.
#
# Progression stages (the stage names are NOT in the V4A+ PDF):
#   PRE-TOP WATCH (this script, up 20%+)  ->  EXHAUSTION  ->  WATCH ALERT  ->  ENTRY ALERT
#   The last three are chart reads. The script does not compute them.
import os, json, time, requests, concurrent.futures as cf
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

# ---------------- data (Binance first, then MEXC, OKX, Gate) ----------------
def _frame(rows, ms0, ms1):
    df = pd.DataFrame(rows, columns=["t", "o", "h", "l", "c", "v"])
    for c in ("o", "h", "l", "c", "v"): df[c] = pd.to_numeric(df[c], errors="coerce")
    df["v"] = df["v"].fillna(0.0)
    df.index = pd.to_datetime(df.pop("t").astype(float), unit="ms", utc=True)
    df = df.sort_index()
    df = df[~df.index.duplicated()]
    return df[(df.index >= pd.to_datetime(ms0, unit="ms", utc=True)) & (df.index <= pd.to_datetime(ms1, unit="ms", utc=True))][["o", "h", "l", "c", "v"]]

_BN_FUT_BLOCKED = False   # set True if Binance futures refuses this server (some regions get HTTP 451/403)

def _bn_get(url, sym, ms0, ms1):
    r = requests.get(url, params=dict(symbol=sym, interval="5m", startTime=ms0, endTime=ms1, limit=1500), timeout=15)
    if r.status_code in (451, 403): return "blocked"
    if r.status_code != 200: return None
    j = r.json()
    return j if isinstance(j, list) and j else None

def _binance(sym, ms0, ms1):
    """Binance USDT perpetual first (also tries the 1000x contract name), then Binance spot market data."""
    global _BN_FUT_BLOCKED
    base = sym.replace("USDT", "")
    tries = []
    if not _BN_FUT_BLOCKED:
        tries += [("https://fapi.binance.com/fapi/v1/klines", base + "USDT", 1.0),
                  ("https://fapi.binance.com/fapi/v1/klines", "1000" + base + "USDT", 1000.0)]
    tries += [("https://data-api.binance.vision/api/v3/klines", base + "USDT", 1.0)]
    for url, s, div in tries:
        j = _bn_get(url, s, ms0, ms1)
        if j == "blocked":
            if "fapi" in url: _BN_FUT_BLOCKED = True
            continue
        if j:
            rows = [(x[0], x[1], x[2], x[3], x[4], x[5]) for x in j]
            df = _frame(rows, ms0, ms1)
            if div != 1.0: df[["o", "h", "l", "c"]] = df[["o", "h", "l", "c"]] / div   # 1000x contract: show the real price
            return df
    raise RuntimeError("not on binance")

def _mexc(sym, ms0, ms1):
    rows, s = [], ms0
    while s < ms1:
        e = min(s + 6 * 86400_000, ms1)
        need = (e - s) / 300_000
        ok = False
        for attempt in range(5):
            try:
                j = requests.get(f"https://contract.mexc.com/api/v1/contract/kline/{sym.replace('USDT', '')}_USDT",
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
    return _frame(rows, ms0, ms1)

def _okx(sym, ms0, ms1):
    rows, after = [], ms1
    while True:
        d = requests.get("https://www.okx.com/api/v5/market/history-candles",
                         params=dict(instId=f"{sym.replace('USDT', '')}-USDT-SWAP", bar="5m", after=after, limit=100),
                         timeout=20).json().get("data", [])
        if not d: break
        rows += [x[:6] for x in d]; after = int(d[-1][0])
        if after <= ms0: break
        time.sleep(0.12)
    return _frame(rows, ms0, ms1)

def _gate(sym, ms0, ms1):
    rows, s = [], ms0
    while s < ms1:
        e = min(s + 5 * 86400_000, ms1)
        r = requests.get("https://api.gateio.ws/api/v4/futures/usdt/candlesticks",
                         params={"contract": f"{sym.replace('USDT', '')}_USDT", "interval": "5m",
                                 "from": s // 1000, "to": e // 1000}, timeout=20).json()
        if isinstance(r, list): rows += [(float(x["t"]) * 1000, x["o"], x["h"], x["l"], x["c"], x.get("v", 0)) for x in r]
        s = e
        time.sleep(0.1)
    return _frame(rows, ms0, ms1)

def fetch_window(sym, t0, t1, min_frac=MIN_FRAC):
    """Returns (data, source), or (None, None) if no source gives complete-enough data."""
    ms0, ms1 = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    need = (ms1 - ms0) / 300_000
    for name, f in (("binance", _binance), ("mexc", _mexc), ("okx", _okx), ("gate", _gate)):
        try:
            df = f(sym, ms0, ms1)
            if df is not None and len(df) > 100 and (len(df) - 1) >= min_frac * need:
                return df.iloc[:-1], name
        except Exception:
            pass
    return None, None

# ---------------- measurements (price change only) ----------------
def pct(x, nd=1):
    if x is None: return "n/a"
    s = f"{x:+.0f}%" if abs(x) >= 100 else f"{x:+.{nd}f}%"
    return f"{0:.{nd}f}%" if float(s[:-1]) == 0 else s

def session_label(t):
    """PDF section 0 windows (Toronto time). Outside the listed windows the PDF gives no rule."""
    m = t.hour * 60 + t.minute
    if 8 * 60 <= m < 11 * 60: return "Window 1 (8-11 AM)"
    if 12 * 60 <= m < 14 * 60: return "DEAD ZONE (12-2 PM): no entry per the PDF"
    if 14 * 60 <= m < 16 * 60:
        return "Window 2 (2-4 PM)" + (", after 3:30 PM = swing only" if m >= 15 * 60 + 30 else "")
    if 19 * 60 <= m < 22 * 60: return "7-10 PM: F3 only (Pre-Asia plan)"
    if 16 * 60 <= m < 19 * 60: return "Phase 2 (4-8 PM): research + swing only"
    return "outside the PDF's listed windows"

# ---------------- one coin ----------------
def evaluate(sym, now):
    """returns (metrics dict or None, got_usable_data)"""
    d5, src = fetch_window(sym, now - pd.Timedelta(days=SCAN_DAYS), now)
    if d5 is None: return None, False
    C = d5.c.values
    if len(C) < 289: return None, False            # less than 24h of candles
    price = float(C[-1])
    back = [float(C[-1 - n]) for n in (288, 48, 12, 3)]
    if any(not np.isfinite(v) or v <= 0 for v in [price] + back): return None, False
    day, h4, h1, m15 = [(price / b - 1) * 100 for b in back]
    return dict(sym=sym.replace("USDT", ""), price=price, day=day, h4=h4, h1=h1, m15=m15,
                big=(day >= BIG_MOVE_PCT), ptw=(day >= PTW_PCT)), True

# ---------------- Binance coins that are NOT on your list ----------------
def binance_extras(listed):
    """Binance USDT coins up PTW_PCT+ in Binance's own 24h window that are not in ALL_COINS.
    One call, no history. Returns (list, source) or ([], None) if Binance can't be reached."""
    for url, name in (("https://fapi.binance.com/fapi/v1/ticker/24hr", "binance perp"),
                      ("https://data-api.binance.vision/api/v3/ticker/24hr", "binance spot")):
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200: continue
            out = []
            for x in r.json():
                sym = x.get("symbol", "")
                if not sym.endswith("USDT") or sym in listed: continue
                base = sym[:-4]
                if base.endswith(("UP", "DOWN", "BULL", "BEAR")) and name == "binance spot": continue
                if base.startswith("1000") and (base[4:] + "USDT") in listed: continue   # already on your list
                ch, px = float(x["priceChangePercent"]), float(x["lastPrice"])
                if ch >= PTW_PCT and px > 0:
                    out.append(dict(sym=base, day=ch, price=px, vol=float(x.get("quoteVolume") or 0)))
            return sorted(out, key=lambda c: (-c["day"], c["sym"])), name
        except Exception as e:
            print("extras failed", name, type(e).__name__, e)
    return [], None

def xline(c):
    return f"{c['sym']} {pct(c['day'])} 24h | price {c['price']:.6g} | 24h volume ${c['vol']/1e6:.1f}M"

# ---------------- alerts ----------------
def push(title, text, priority="default", tags="bar_chart"):
    """Returns True only if ntfy accepted the message."""
    data = text.encode("utf-8")
    if len(data) > MAX_BYTES:
        text = data[:MAX_BYTES].decode("utf-8", "ignore").rstrip() + "\n...(cut)"
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
    return (f"{c['sym']} {pct(c['day'])} 24h | 4H {pct(c['h4'])} | 1H {pct(c['h1'])} | 15m {pct(c['m15'])} | price {c['price']:.6g}"
            + ("\n   70%+ move: needs 1H lower high + failed reclaim (check on chart)" if c["big"] else ""))

STAGES = "Stages: PRE-TOP WATCH -> EXHAUSTION -> WATCH ALERT -> ENTRY ALERT (the last three are chart reads, not scanned)."
CHECK_FOOTER = ("Check on chart (not scanned): peaked 4H, 1H rolling, 15m CHoCH, sweep, displacement, MSB body close, fresh FVG, "
                "retrace to 50% FVG + 1m/5m CHoCH, BTC 4H, OI, MACD, volume, score 7, natural R:R >= 2.1, stop beyond sweep wick, "
                "TP1/TP2, USD event +-30 min. Alert = notification only.")

def main():
    now = pd.Timestamp.now(tz="UTC")
    try:
        with open(STATE_FILE) as f: state = json.load(f)
    except Exception: state = {}
    seen = state.get("ptw", {})
    results, with_data = [], 0
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(evaluate, sym, now): sym for sym in ALL_COINS}
        for fut in cf.as_completed(futs):
            try:
                out, ok = fut.result()
                with_data += 1 if ok else 0
                if out: results.append(out)
            except Exception as e:
                print("error", futs[fut], type(e).__name__, e)

    tnow = now.tz_convert(TZ)
    today = tnow.strftime("%Y-%m-%d")
    extras, xsrc = binance_extras(set(ALL_COINS))
    xfresh = [c for c in extras if f"{today}|X|{c['sym']}" not in seen]
    xtext = ("\n\nNOT ON YOUR LIST (Binance, up 20%+ in 24h; Binance 24h figures, no 4H/1H/15m here):\n"
             + "\n".join(xline(c) for c in xfresh[:MAX_PTW])) if xfresh else ""
    ptws = sorted((c for c in results if c["ptw"]), key=lambda c: (-c["day"], c["sym"]))
    fresh = [c for c in ptws if f"{today}|{c['sym']}" not in seen]
    print(f"coins scanned {len(ALL_COINS)} | with usable data {with_data} | up {PTW_PCT:.0f}%+ in 24h: {len(ptws)}")
    head = (f"Coins scanned: {len(ALL_COINS)} ({with_data} with usable data).\n"
            f"Session {tnow.strftime('%H:%M')} Toronto: {session_label(tnow)}\n")

    if TEST:
        body = head + "\n" + STAGES + "\n\n"
        body += ("PRE-TOP WATCH (up 20%+ in 24h):\n" + "\n".join(line(c) for c in ptws[:MAX_PTW])
                 if ptws else "No coin is up 20%+ in 24h right now.")
        sample = sorted(results, key=lambda c: (-c["day"], c["sym"]))[:3]
        body += "\n\nTest sample only (biggest 24h moves, not alerts):\n" + "\n".join(line(c) for c in sample)
        body += ("\n\nNOT ON YOUR LIST (Binance, up 20%+ in 24h):\n" + "\n".join(xline(c) for c in extras[:MAX_PTW])
                 if extras else f"\n\nNot on your list: none up 20%+ on Binance ({'source ' + xsrc if xsrc else 'Binance not reachable'}).")
        push("ALEX7 screener TEST", body)
    elif fresh or xfresh:
        lst = fresh[:MAX_PTW]
        body = (head + "\nPRE-TOP WATCH: up 20%+ in 24h. Tracking starts here (shorts). Not an entry.\n" + STAGES
                + "\n\n" + ("\n".join(line(c) for c in lst) if lst else "None on your list.") + xtext + "\n\n" + CHECK_FOOTER)
        if push(f"ALEX7 PRE-TOP WATCH: {len(lst) + len(xfresh[:MAX_PTW])} up 20%+ in 24h", body, priority="high", tags="eyes"):
            for c in lst: seen[f"{today}|{c['sym']}"] = 1
            for c in xfresh[:MAX_PTW]: seen[f"{today}|X|{c['sym']}"] = 1
    if len(seen) > 400: seen = dict(list(seen.items())[-200:])   # keeps the newest 200
    state["ptw"] = seen

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
