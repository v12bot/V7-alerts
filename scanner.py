# ================= V7 ALERT SCANNER (locked V7 rules, runs on GitHub, pushes to your phone via ntfy) =================
# Shorts only. It finds candidates and sends heads-up alerts. It does NOT place trades.
# Rules taken from the locked V7 PDF. Anything marked PH is a placeholder choice that you have not confirmed.
# NOT BUILT: CVD (check on chart), EXHAUSTION / WATCH ALERT phase labels (chart judgment),
#            position size, kill switch, hand-trailing.
import os, json, time, requests, concurrent.futures as cf
import numpy as np, pandas as pd
from dataclasses import dataclass

SCAN_DAYS = 8                                   # 5m history used (need enough for 4H swing lows)
TZ = "America/Toronto"
MIN_FRAC = 0.97                                 # a coin's 5m data must be at least this complete
WORKERS = 4
STATE_FILE = "state.json"
TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
TEST = os.environ.get("TEST", "").lower() == "true"
MAX_ALERTS = 3                                  # top three alerts, ranked by best setup
MAX_PTW = 6                                     # PRE-TOP WATCH names per message

@dataclass
class Cfg:
    # ---- LOCKED V7 ----
    min_rr: float = 2.1                 # step 7: R:R to TP1
    disp_body: float = 0.60             # step 2: body >= 60% of range
    disp_close_zone: float = 0.25       # step 2: close in bottom 25% of range
    disp_vol_mult: float = 1.5          # step 2: volume >= 1.5x 20-bar average
    vol_avg_bars: int = 20
    fib_lo: float = 0.5                 # step 5: fib zone
    fib_hi: float = 0.618
    ptw_pct: float = 20.0               # PRE-TOP WATCH: up 20%+ (rolling 24h, your choice)
    # ---- PLACEHOLDERS (PH) ----
    pivot_n_1h: int = 3                 # PH: major 1H pivot = 3-bar fractal (sweep level and break level)
    pivot_n_15m: int = 2                # PH: lower high = 15m fractal, 2 bars each side
    pivot_n_4h: int = 2                 # PH: 4H swing low = 2-bar fractal
    zone_tfs: tuple = ("1h", "15min")   # PH: FVG timeframes (kept from the old file)
    min_fvg_pct: float = 0.0            # PH: "meaningful" FVG size (0 = any)
    stop_buffer_pct: float = 0.0        # PH: stop above the lower high (0 = exact)
    lower_high_pick: str = "highest"    # PH: "highest" = highest 15m swing high below the sweep extreme (wider stop); "latest" = most recent one (tighter stop)
    break_report_min: int = 180         # PH: only report a closed break from the last 3 hours (alert noise limit, not a V7 rule)
    oi_flat_pct: float = 0.3            # PH: OI change within +/- this % counts as flat

# ---------------- data (MEXC, then OKX, Gate) ----------------
def _frame(rows, ms0, ms1):
    df = pd.DataFrame(rows, columns=["t", "o", "h", "l", "c", "v"])
    for c in ("o", "h", "l", "c", "v"): df[c] = pd.to_numeric(df[c], errors="coerce")
    df["v"] = df["v"].fillna(0.0)
    df.index = pd.to_datetime(df.pop("t").astype(float), unit="ms", utc=True)
    df = df.sort_index()
    df = df[~df.index.duplicated()]
    return df[(df.index >= pd.to_datetime(ms0, unit="ms", utc=True)) & (df.index <= pd.to_datetime(ms1, unit="ms", utc=True))][["o", "h", "l", "c", "v"]]

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
    for name, f in (("mexc", _mexc), ("okx", _okx), ("gate", _gate)):
        try:
            df = f(sym, ms0, ms1)
            if df is not None and len(df) > 100 and (len(df) - 1) >= min_frac * need:
                return df.iloc[:-1], name
        except Exception:
            pass
    return None, None

def fetch_oi(sym):
    """Open interest history from OKX (5m, latest 100 points, coin units not USD). Returns a Series or None."""
    try:
        r = requests.get("https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history",
                         params=dict(instId=f"{sym.replace('USDT', '')}-USDT-SWAP", period="5m", limit=100),
                         timeout=15).json()
        rows = []
        for x in (r.get("data") or []):
            val = x[2] if len(x) >= 3 and x[2] not in ("", None) else x[1]
            rows.append((pd.to_datetime(int(x[0]), unit="ms", utc=True), float(val)))
        if len(rows) < 3: return None
        return pd.Series(dict(rows)).sort_index()
    except Exception:
        return None

def oi_verdict(oi, t0, cfg):
    """OI change from just before t0 to the latest value. Returns (word, pct)."""
    if oi is None: return "unverified", None
    if len(oi[oi.index > t0]) < 2: return "unverified", None
    base = oi[oi.index <= t0]
    b = float(base.iloc[-1]) if len(base) else float(oi.iloc[0])
    if b <= 0: return "unverified", None
    pct = (float(oi.iloc[-1]) / b - 1) * 100
    if pct > cfg.oi_flat_pct: return "rising", pct
    if pct < -cfg.oi_flat_pct: return "falling", pct
    return "flat", pct

# ---------------- structure ----------------
def rs(d, rule):
    r = d.resample(rule, label="left", closed="left").agg(
        {"o": "first", "h": "max", "l": "min", "c": "last", "v": "sum"}).dropna()
    return r.iloc[:-1]

def pivots(a, n, kind):
    out = []
    for i in range(n, len(a) - n):
        w = a[i - n:i + n + 1]
        ext = w.max() if kind == "hi" else w.min()
        if a[i] == ext and (w == a[i]).sum() == 1: out.append((i, a[i]))
    return out

def find_sweeps(h1, n):
    """1H sweeps: wick above a confirmed 1H pivot high, close back inside. level = last 1H pivot low before the sweep."""
    H, C = h1.h.values, h1.c.values
    phi, plo = pivots(H, n, "hi"), pivots(h1.l.values, n, "lo")
    live, pi, out = [], 0, []
    for j in range(len(H)):
        while pi < len(phi) and phi[pi][0] + n <= j - 1:
            live.append(phi[pi][1]); pi += 1
        hit = [p for p in live if H[j] > p]
        if not hit: continue
        live = [p for p in live if H[j] <= p]
        if not any(C[j] <= p for p in hit): continue
        ref = None
        for i, pr in plo:
            if i + n <= j: ref = pr
            else: break
        if ref is None: continue
        out.append(dict(time=h1.index[j], ext=float(H[j]), level=float(ref)))
    return out

def fvg_zones(d, tf, min_pct):
    """Bearish FVGs (gap between candle i-2 low and candle i high)."""
    H, L, C, z, dt = d.h.values, d.l.values, d.c.values, [], pd.Timedelta(tf)
    for i in range(2, len(H)):
        if L[i - 2] > H[i]:
            lo, hi = H[i], L[i - 2]
            if (hi - lo) / abs(C[i]) >= min_pct / 100:
                z.append(dict(lo=lo, hi=hi, ready=d.index[i] + dt, tf=tf))
    return z

def _touched_before(z, d5, t_end):
    seg = d5.loc[z["ready"]:t_end - pd.Timedelta(minutes=5)]
    return bool(((seg.h >= z["lo"]) & (seg.l <= z["hi"])).any()) if len(seg) else False

def fmt(ts): return ts.tz_convert(TZ).strftime("%m-%d %H:%M")

# ---------------- one coin ----------------
def evaluate(sym, cfg, now):
    """returns (list of candidate dicts, PRE-TOP WATCH dict or None, got_complete_data)"""
    cands, ptw = [], None
    d5, src = fetch_window(sym, now - pd.Timedelta(days=SCAN_DAYS), now)
    if d5 is None: return cands, ptw, False
    name = sym.replace("USDT", "")
    price = float(d5.c.iloc[-1])
    if len(d5) > 289:
        chg = (price / float(d5.c.iloc[-289]) - 1) * 100          # rolling 24h
        if chg >= cfg.ptw_pct: ptw = dict(sym=name, pct=chg, price=price)

    h1 = rs(d5, "1h")
    sweeps = find_sweeps(h1, cfg.pivot_n_1h)
    if not sweeps: return cands, ptw, True
    t5, O, H, L, C, V = d5.index, d5.o.values, d5.h.values, d5.l.values, d5.c.values, d5.v.values

    # most recent sweep whose extreme has not been exceeded since
    s = None
    for sw in reversed(sweeps):
        w = d5.loc[sw["time"]: sw["time"] + pd.Timedelta(minutes=55)]
        if not len(w): continue
        e_idx = t5.get_loc(w.h.idxmax())
        if e_idx + 1 < len(H) and H[e_idx + 1:].max() > sw["ext"]: continue
        s = sw; break
    if s is None: return cands, ptw, True
    level, ext, t_e = s["level"], s["ext"], t5[e_idx]

    # step 3: first 5m close below the level after the sweep extreme
    below = np.where(C[e_idx + 1:] < level)[0]
    b = int(e_idx + 1 + below[0]) if len(below) else None

    # step 6: lower high = 15m swing high after the sweep and before the break, below the extreme (which one: see lower_high_pick)
    d15 = rs(d5, "15min")
    t_cut = t5[b] if b is not None else now + pd.Timedelta(minutes=5)
    lhs = [(d15.index[i], float(v)) for i, v in pivots(d15.h.values, cfg.pivot_n_15m, "hi")
           if d15.index[i] > t_e and v < ext and d15.index[i] + pd.Timedelta(minutes=15) <= t_cut]
    if not lhs: return cands, ptw, True            # no clear lower high = no entry
    lh = max(v for _, v in lhs) if cfg.lower_high_pick == "highest" else lhs[-1][1]
    stop = lh * (1 + cfg.stop_buffer_pct / 100)

    # step 7: TP1 = nearest 4H swing low below entry (swing low must be confirmed by the entry time)
    d4 = rs(d5, "4h"); n4 = cfg.pivot_n_4h
    p4l = pivots(d4.l.values, n4, "lo")
    p4 = np.array([float(v) for _, v in p4l])
    c4 = np.array([(d4.index[i + n4] + pd.Timedelta(hours=4)).value for i, _ in p4l])
    def tp1_at(entry, ct):
        m = (c4 <= ct.value) & (p4 < entry) if len(p4) else np.array([], bool)
        return float(p4[m].max()) if len(p4) and m.any() else None

    if b is not None:
        # ===== BREAK: 5m close below the level has happened =====
        ct = t5[b] + pd.Timedelta(minutes=5)
        if now - ct > pd.Timedelta(minutes=cfg.break_report_min): return cands, ptw, True
        entry = float(C[b]); risk = stop - entry
        if risk <= 0: return cands, ptw, True
        rng = H[b] - L[b]
        if rng <= 0: return cands, ptw, True
        if (O[b] - C[b]) / rng < cfg.disp_body or (C[b] - L[b]) / rng > cfg.disp_close_zone:
            return cands, ptw, True                 # step 2 fails on the break candle
        avg = float(V[b - cfg.vol_avg_bars:b].mean()) if b >= cfg.vol_avg_bars else 0.0
        if avg > 0:
            mult = float(V[b]) / avg
            if mult < cfg.disp_vol_mult: return cands, ptw, True
            disp_txt = f"body/close ok, volume {mult:.1f}x"
        else:
            disp_txt = "body/close ok, volume unverified"
        tp1 = tp1_at(entry, ct)
        if tp1 is None: return cands, ptw, True
        rr = (entry - tp1) / risk
        if rr < cfg.min_rr: return cands, ptw, True
        word, pct = oi_verdict(fetch_oi(sym), t5[b], cfg)   # step 4
        if word in ("flat", "falling"): return cands, ptw, True
        oi_txt = "unverified, check chart" if word == "unverified" else f"rising {pct:+.1f}% since the break"
        cands.append(dict(sym=name, status="BREAK", rr=rr, entry=entry, stop=stop, tp1=tp1, price=price,
                          stop_pct=risk / entry * 100, break_time=fmt(ct), disp_txt=disp_txt, oi_txt=oi_txt,
                          sweep=fmt(s["time"]), key=f"{sym}|{s['time'].isoformat()}|BREAK"))
        return cands, ptw, True

    # ===== SETUP (watch): sweep done, lower high in place, level not yet broken on a 5m close =====
    risk_b = stop - level
    tp1_b = tp1_at(level, now)
    rr_b = ((level - tp1_b) / risk_b) if (risk_b > 0 and tp1_b is not None) else None

    # retrace path: FVG overlapping the 0.5-0.618 fib zone (swing high = sweep extreme, swing low = lowest low since)
    swing_low = float(L[e_idx:].min()); frng = ext - swing_low
    zone, rr_z = None, None
    if frng > 0:
        zlo_f, zhi_f = swing_low + cfg.fib_lo * frng, swing_low + cfg.fib_hi * frng
        ov = []
        for tf in cfg.zone_tfs:
            for z in fvg_zones(rs(d5, tf), tf, cfg.min_fvg_pct):
                if z["ready"] <= t_e: continue
                lo, hi = max(z["lo"], zlo_f), min(z["hi"], zhi_f)
                if lo >= hi or hi < price: continue          # no overlap, or price already above the zone
                if _touched_before(z, d5, now + pd.Timedelta(minutes=5)): continue
                ov.append((lo, hi))
        if ov:
            lo, hi = min(ov, key=lambda t: max(0.0, t[0] - price))
            ez = (lo + hi) / 2
            tpz = tp1_at(ez, now)
            if ez < stop and tpz is not None:
                zone, rr_z = (lo, hi), (ez - tpz) / (stop - ez)
    if rr_b is None or rr_b < cfg.min_rr: return cands, ptw, True   # alert only on break-level R:R; the zone is info only
    best = rr_b

    word, pct = oi_verdict(fetch_oi(sym), s["time"], cfg)
    oi_txt = "unverified, check chart" if word == "unverified" else f"{word} {pct:+.1f}% since the sweep"
    cands.append(dict(sym=name, status="SETUP", rr=best, rr_b=rr_b, rr_z=rr_z, zone=zone, level=level, stop=stop,
                      tp1=tp1_b, price=price, dist=(price - level) / level * 100,
                      stop_pct=(stop - level) / level * 100, oi_txt=oi_txt, sweep=fmt(s["time"]),
                      key=f"{sym}|{s['time'].isoformat()}|SETUP"))
    return cands, ptw, True

# ---------------- alerts ----------------
def push(title, text, priority="default", tags="bar_chart"):
    print("PUSH:", title, "\n" + text)
    if not TOPIC:
        print("(no NTFY_TOPIC set, not sent)"); return
    try:
        requests.post(f"https://ntfy.sh/{TOPIC}", data=text.encode("utf-8"),
                      headers={"Title": title, "Priority": priority, "Tags": tags}, timeout=20)
    except Exception as e:
        print("push failed:", e)

def describe(c):
    if c["status"] == "BREAK":
        return (f"{c['sym']} SHORT | 5m BREAK CLOSED {c['break_time']}, entry {c['entry']:.6g}\n"
                f"  stop {c['stop']:.6g} ({c['stop_pct']:.1f}%) above lower high | TP1 {c['tp1']:.6g} (4H swing low) | R:R {c['rr']:.2f}\n"
                f"  displacement: {c['disp_txt']} | OI: {c['oi_txt']} | CVD: check chart\n"
                f"  price now {c['price']:.6g} | sweep {c['sweep']}")
    lines = [f"{c['sym']} SHORT | WATCH, set TradingView alert at {c['level']:.6g} (price {c['price']:.6g}, {c['dist']:.1f}% above)",
             f"  stop {c['stop']:.6g} ({c['stop_pct']:.1f}%) above lower high"
             + (f" | TP1 {c['tp1']:.6g} (4H swing low)" if c["tp1"] is not None else "")]
    if c["rr_b"] is not None:
        lines.append(f"  R:R if it breaks at the level {c['rr_b']:.2f} (the real 5m close will be a bit worse)")
    if c["zone"] is not None:
        lines.append(f"  retrace zone (info only) {c['zone'][0]:.6g}-{c['zone'][1]:.6g}, FVG in the 0.5-0.618 fib")
    lines.append(f"  OI: {c['oi_txt']} | check displacement, OI and CVD at the break | sweep {c['sweep']}")
    return "\n".join(lines)

def main():
    cfg = Cfg()
    now = pd.Timestamp.now(tz="UTC")
    try: state = json.load(open(STATE_FILE))
    except Exception: state = {}
    seen = state.get("seen", {})
    results, ptws, with_data = [], [], 0
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(evaluate, sym, cfg, now): sym for sym in ALL_COINS}
        for f in cf.as_completed(futs):
            try:
                out, ptw, ok = f.result()
                with_data += 1 if ok else 0
                results += out
                if ptw: ptws.append(ptw)
            except Exception as e:
                print("error", futs[f], type(e).__name__, e)
    results.sort(key=lambda c: (c["status"] != "BREAK", -c["rr"]))
    ptws.sort(key=lambda p: -p["pct"])
    print(f"coins {len(ALL_COINS)} | with COMPLETE data {with_data} | V7 candidates: {len(results)} | up {cfg.ptw_pct:.0f}%+ in 24h: {len(ptws)}")
    fresh = [c for c in results if seen.get(c["key"]) != c["status"]]
    today_utc = now.strftime("%Y-%m-%d")
    fresh_ptw = [p for p in ptws if seen.get(f"ptw|{p['sym']}|{today_utc}") != "PTW"]

    if TEST:
        body = f"Test message. Coins with complete data: {with_data} of {len(ALL_COINS)}.\n"
        body += ("Current top:\n" + "\n\n".join(describe(c) for c in results[:MAX_ALERTS])) if results else "No V7 candidates right now."
        if ptws: body += "\n\nPRE-TOP WATCH: " + ", ".join(f"{p['sym']} {p['pct']:+.0f}%" for p in ptws[:MAX_PTW])
        push("V7 scanner TEST", body)
    else:
        if fresh:
            top = fresh[:MAX_ALERTS]
            has_break = any(c["status"] == "BREAK" for c in top)
            push(f"V7 short: {len(top)} alert{'s' if len(top) > 1 else ''} (best first)",
                 "\n\n".join(describe(c) for c in top), priority="high" if has_break else "default")
            for c in top: seen[c["key"]] = c["status"]
        if fresh_ptw:
            top = fresh_ptw[:MAX_PTW]
            push("V7 PRE-TOP WATCH (up 20%+ in 24h)",
                 "Tag only, not entries:\n" + "\n".join(f"{p['sym']} {p['pct']:+.0f}% (price {p['price']:.6g})" for p in top),
                 priority="low", tags="eyes")
            for p in top: seen[f"ptw|{p['sym']}|{today_utc}"] = "PTW"

    if len(seen) > 400: seen = dict(list(seen.items())[-200:])
    # daily heartbeat (after 09:00 Toronto)
    today = now.tz_convert(TZ).strftime("%Y-%m-%d")
    if now.tz_convert(TZ).hour >= 9 and state.get("heartbeat") != today and not TEST:
        push("V7 scanner alive", f"{with_data} of {len(ALL_COINS)} coins had complete data. {len(results)} V7 candidates right now, {len(ptws)} coins up 20%+ in 24h.", priority="low", tags="white_check_mark")
        state["heartbeat"] = today
    if with_data < len(ALL_COINS) * 0.8 and state.get("datawarn") != today:
        push("V7 scanner: data problem", f"Only {with_data} of {len(ALL_COINS)} coins had complete data. Do not trust quiet periods.", priority="high", tags="warning")
        state["datawarn"] = today
    state["seen"] = seen
    json.dump(state, open(STATE_FILE, "w"))

ALL_COINS = [s + "USDT" for s in """BTC ETH BNB XRP SOL ZEC HYPE DOGE XMR LINK ADA XLM NEAR BCH UNI LTC CC AVAX SUI HBAR QNT TAO CRO ENA PUMP AAVE ONDO M WLD MNT DOT SKY ASTER ICP MORPHO PEPE WLFI ETC VVV ARB KAS ALGO JUP JST RENDER FIL ATOM ZRO AERO CAKE INJ VET DASH APT ETHFI FLR PYTH PENGU RAY CRV TRUMP VIRTUAL GRASS SEI TIA PENDLE BSV SPX STRK FF LDO UB KITE SUN BONK GNO GRT OP JTO LUNC ENS CFX FLOKI JASMY ZBCN RUNE WIF EIGEN COMP USELESS THETA AKT KMNO MANA SAND NEO CHZ FARTCOIN IMX MET APE SUPER 1INCH ATH SNX ORCA AIOZ EGLD GLM AWE BEAM PLUME DYDX GALA SOON PROM CHEEMS PEAQ FORM HNT Q DOG KSM ORDI GAS GMX SAFE RED NMR ZETA SPK RIF QUBIC BERA KAITO CROSS BABYDOGE IP MUBARAK BANANAS31 SUSHI TURBO ARC ZIL UAI BIO BOME ENJ ROSE BABY DEXE BAN MERL GPS CTC COAI PHA BLUR DEEP HUMA BRETT TRB JELLYJELLY FLOW PNUT POPCAT NIL TOSHI HOLO DUSK NOT ALCH MASK EDU AVNT MOG MEW CATI SSV MOODENG PEOPLE MOVE SXT MOCA ONG PROVE COTI BAND RATS SYN ALEO MEME UMA LAB APR B3 GIGGLE NEIRO SQD POWR APEX MMT ORBS SIGN FLUX SKYAI STEEM ONE ZORA MANTA CARV CORE KGEN VELVET HIVE FLOCK ZEREBRO CLOUD IOST XAN TNSR CTSI AUCTION ZBT GMT METIS CETUS BIGTIME OPEN USUAL WOO EDEN XNY HEMI ICNT HOME MOVR INIT HYPER SOLV RIVER AIXBT CGPT STG BANK FIDA GIGA CTK BICO CHR ENSO DIA JCT WCT CYBER GRIFFAIN SOPH DODO MIRA PIPPIN GOAT AVA SIREN EPIC SAPIEN GTC MYX CAT SPELL ICX BLESS DOLO ORDER DRIFT BMT CLANKER MAV RARE PONKE DOOD DYM STBL CHILLGUY BTR GLMR GUN GODS HMSTR BLUAI NEWT TRUST COOKIE ACT SAGA BEL SONIC RESOLV TAC LIGHT SWARMS BLAST CLO MITO NOM QUICK TREE HFT""".split()]

if __name__ == "__main__":
    main()
