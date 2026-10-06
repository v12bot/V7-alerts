# ================= V7 ALERT SCANNER (runs on GitHub, pushes to your phone via ntfy) =================
# Mechanical V7 using ONLY what you answered. Anything you left "by eye" is a
# PLACEHOLDER (marked PH). It gives SETUPS, it does NOT place trades.
import time, requests
import numpy as np, pandas as pd
from collections import Counter
from dataclasses import dataclass

SCAN_DAYS = 6                                   # history used by the live finder
TZ = "America/Toronto"

@dataclass
class Cfg:
    # ---- YOUR RECORDED RULES ----
    min_rr: float = 2.0
    split_tp1: float = 0.70
    breakeven_after_tp1: bool = True
    max_open_coins: int = 2
    daily_loss_limit: int = 2
    zone_used_by: str = "first_touch"   # first touch uses a zone up
    trigger_any_color: bool = True      # first touching candle closes inside the zone, any color
    # ---- PLACEHOLDERS (by eye / pending) ----
    pivot_n: int = 3                    # PH: major 1H pivot = 3-bar fractal
    mss_window_1h: int = 40             # PH: bars allowed from sweep to MSS
    zone_tfs: tuple = ("1h", "15min")   # you allowed 1H/15m/4H
    min_fvg_pct: float = 0.0            # PH: "meaningful" FVG size (0 = any)
    setup_hours: float = 24.0           # PH: you said "hours"
    stop_buffer_pct: float = 0.0        # PH: stop beyond sweep extreme (0 = exact)
    tp_unswept_only: bool = False       # PH
    tp2_mode: str = "pivot"             # PH: "pivot" or "r"
    tp2_r: float = 3.0
    fee_bps_side: float = 6.0           # PH
    slip_bps_side: float = 0.0          # PH
# NOT BUILT: position size/heat, OI/CVD veto, volume cutoff, opposing-MSS kill, hand-trailing.

# ---------------- data (MEXC, then OKX, Gate) ----------------
def _frame(rows, cols, ms0, ms1, scale=1.0):
    df = pd.DataFrame(rows, columns=cols)
    for c in ("o", "h", "l", "c"): df[c] = df[c].astype(float)
    df["v"] = 0.0
    df.index = pd.to_datetime(df.pop("t").astype(float) * scale, unit="ms", utc=True)
    df = df.sort_index()
    df = df[~df.index.duplicated()]
    return df[(df.index >= pd.to_datetime(ms0, unit="ms", utc=True)) & (df.index <= pd.to_datetime(ms1, unit="ms", utc=True))][["o", "h", "l", "c", "v"]]

def _mexc(sym, ms0, ms1):
    rows, s = [], ms0
    while s < ms1:
        e = min(s + 6 * 86400_000, ms1)
        k = requests.get(f"https://contract.mexc.com/api/v1/contract/kline/{sym.replace('USDT', '')}_USDT",
                         params=dict(interval="Min5", start=s // 1000, end=e // 1000), timeout=20).json().get("data") or {}
        if k.get("time"):
            rows += list(zip(np.array(k["time"], float) * 1000, k["open"], k["high"], k["low"], k["close"]))
        s = e
        time.sleep(0.1)
    return _frame(rows, ["t", "o", "h", "l", "c"], ms0, ms1)

def _okx(sym, ms0, ms1):
    rows, after = [], ms1
    while True:
        d = requests.get("https://www.okx.com/api/v5/market/history-candles",
                         params=dict(instId=f"{sym.replace('USDT', '')}-USDT-SWAP", bar="5m", after=after, limit=100),
                         timeout=20).json().get("data", [])
        if not d: break
        rows += [x[:5] for x in d]; after = int(d[-1][0])
        if after <= ms0: break
        time.sleep(0.12)
    return _frame(rows, ["t", "o", "h", "l", "c"], ms0, ms1)

def _gate(sym, ms0, ms1):
    rows, s = [], ms0
    while s < ms1:
        e = min(s + 5 * 86400_000, ms1)
        r = requests.get("https://api.gateio.ws/api/v4/futures/usdt/candlesticks",
                         params={"contract": f"{sym.replace('USDT', '')}_USDT", "interval": "5m",
                                 "from": s // 1000, "to": e // 1000}, timeout=20).json()
        if isinstance(r, list): rows += [(float(x["t"]) * 1000, x["o"], x["h"], x["l"], x["c"]) for x in r]
        s = e
        time.sleep(0.1)
    return _frame(rows, ["t", "o", "h", "l", "c"], ms0, ms1)

def fetch_window(sym, t0, t1):
    ms0, ms1 = int(t0.timestamp() * 1000), int(t1.timestamp() * 1000)
    for name, f in (("mexc", _mexc), ("okx", _okx), ("gate", _gate)):
        try:
            df = f(sym, ms0, ms1)
            if df is not None and len(df) > 100: return df.iloc[:-1], name
        except Exception:
            pass
    return None, None

# ---------------- structure ----------------
def flip(d):   # mirror prices so a long is processed as a short
    return pd.DataFrame({"o": -d.o, "h": -d.l, "l": -d.h, "c": -d.c, "v": d.v}, index=d.index)

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

def find_setups(h1, cfg):
    n = cfg.pivot_n; H, L, C = h1.h.values, h1.l.values, h1.c.values; t = h1.index
    phi, plo = pivots(H, n, "hi"), pivots(L, n, "lo")
    live, pi, setups, st = [], 0, [], Counter()
    for j in range(len(H)):
        while pi < len(phi) and phi[pi][0] + n <= j - 1:
            live.append(phi[pi][1]); pi += 1
        hit = [p for p in live if H[j] > p]
        if not hit: continue
        live = [p for p in live if H[j] <= p]
        if not any(C[j] <= p for p in hit): continue
        st["sweeps"] += 1
        ref = None
        for i, pr in plo:
            if i + n <= j: ref = pr
            else: break
        if ref is None: continue
        ext = H[j]
        for k in range(j + 1, min(j + 1 + cfg.mss_window_1h, len(H))):
            if H[k] > ext: break
            if C[k] < ref:
                st["mss"] += 1
                setups.append(dict(sweep_time=t[j], sweep_high=ext, mss_close=t[k] + pd.Timedelta(hours=1)))
                break
    return setups, st, plo

def fvg_zones(d, tf, min_pct):
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

class PivotLows:
    def __init__(self, plo, h1, n):
        self.p = np.array([p for _, p in plo]); self.i = np.array([i for i, _ in plo])
        self.conf = np.array([(h1.index[i + n] + pd.Timedelta(hours=1)).value for i, _ in plo])
        self.L, self.idx = h1.l.values, h1.index
    def lookup(self, ct, entry, unswept):
        m = (self.conf <= ct.value) & (self.p < entry)
        if unswept and m.any():
            k = self.idx.searchsorted(ct - pd.Timedelta(hours=1), side="right")
            for q in np.where(m)[0]:
                seg = self.L[self.i[q] + 1:k]
                if len(seg) and seg.min() <= self.p[q]: m[q] = False
        c = self.p[m]
        if len(c) == 0: return None, None
        tp1 = c.max(); r = c[c < tp1]
        return tp1, (r.max() if len(r) else None)

# ---------------- one setup, 5m ----------------
def run_setup(s, d5, zones, pl, cfg, ban_until):
    t5, O, H, L, C = d5.index, d5.o.values, d5.h.values, d5.l.values, d5.c.values
    ext = s["sweep_high"]; stop = ext + abs(ext) * cfg.stop_buffer_pct / 100
    zl = sorted([z for z in zones if z["ready"] > s["sweep_time"] + pd.Timedelta(hours=1)
                 and not _touched_before(z, d5, s["mss_close"])], key=lambda z: z["ready"])
    deadline = s["mss_close"] + pd.Timedelta(hours=cfg.setup_hours)
    zi, elig, had = 0, [], False
    for b in range(max(t5.searchsorted(s["mss_close"]), 1), len(t5)):
        tb = t5[b]
        if tb >= deadline: return dict(dead="time_limit", end=tb)
        if H[b] > ext: return dict(dead="extreme_broken", end=tb)
        while zi < len(zl) and zl[zi]["ready"] <= tb:
            elig.append(zl[zi]); zi += 1; had = True
        if not elig: continue
        prev = C[b - 1]
        active = min(elig, key=lambda z: max(0.0, z["lo"] - prev))
        touched = [z for z in elig if H[b] >= z["lo"] and L[b] <= z["hi"]]
        trig = active if (any(active is z for z in touched) and active["lo"] <= C[b] <= active["hi"]
                          and (cfg.trigger_any_color or C[b] < O[b])) else None
        if cfg.zone_used_by == "first_touch":
            elig = [z for z in elig if not any(z is q for q in touched)]
        else:
            elig = [z for z in elig if not (any(z is q for q in touched) and C[b] > z["hi"])]
        if trig is None:
            if had and not elig: return dict(dead="zones_used_up", end=tb)
            continue
        ct = tb + pd.Timedelta(minutes=5); entry = C[b]; risk = stop - entry
        if ct < ban_until: return dict(dead="reentry_ban", end=ct)
        if risk <= 0: return dict(dead="bad_risk", end=ct)
        tp1, tp2 = pl.lookup(ct, entry, cfg.tp_unswept_only)
        if tp1 is None: return dict(dead="no_tp1", end=ct)
        rr = (entry - tp1) / risk
        if rr < cfg.min_rr: return dict(dead="rr_fail", end=ct)
        if cfg.tp2_mode == "r": tp2 = entry - cfg.tp2_r * risk
        r1 = (entry - tp1) / risk; r2 = None if tp2 is None else (entry - tp2) / risk
        sp = cfg.split_tp1; done, cur, real, out, res, xk = False, stop, 0.0, None, None, len(t5) - 1
        for k in range(b + 1, len(t5)):
            if not done:
                if H[k] >= cur: out, res, xk = "stop", -1.0, k; break
                if L[k] <= tp1:
                    done, real = True, sp * r1
                    if cfg.breakeven_after_tp1: cur = entry
            else:
                if H[k] >= cur: out, res, xk = "tp1_then_stop", real + (1 - sp) * (entry - cur) / risk, k; break
                if r2 is not None and L[k] <= tp2: out, res, xk = "tp1_tp2", real + (1 - sp) * r2, k; break
        if out is None:
            out = "open_end"
            res = (real + (1 - sp) * (entry - C[-1]) / risk) if done else (entry - C[-1]) / risk
        cost = 2 * (cfg.fee_bps_side + cfg.slip_bps_side) / 1e4 * abs(entry) / risk
        return dict(trade=True, end=t5[xk] + pd.Timedelta(minutes=5), entry_time=ct, entry=entry, stop=stop,
                    tp1=tp1, tp2=tp2, rr=rr, outcome=out, R=res - cost, still_open=(out == "open_end"))
    return dict(pending=True, end=t5[-1], stop=stop, ext=ext, zones=list(elig), had=had)


import os, json, concurrent.futures as cf

ALL_COINS = [s + "USDT" for s in """BTC ETH BNB XRP SOL ZEC HYPE DOGE XMR LINK ADA XLM NEAR BCH UNI LTC CC AVAX SUI HBAR QNT TAO CRO ENA PUMP AAVE ONDO M WLD MNT DOT SKY ASTER ICP MORPHO PEPE WLFI ETC VVV ARB KAS ALGO JUP JST RENDER FIL ATOM ZRO AERO CAKE INJ VET DASH APT ETHFI FLR PYTH PENGU RAY CRV TRUMP VIRTUAL GRASS SEI TIA PENDLE BSV SPX STRK FF LDO UB KITE SUN BONK GNO GRT OP JTO LUNC ENS CFX FLOKI JASMY ZBCN RUNE WIF EIGEN COMP USELESS THETA AKT KMNO MANA SAND NEO CHZ FARTCOIN IMX MET APE SUPER 1INCH ATH SNX ORCA AIOZ EGLD GLM AWE BEAM PLUME DYDX GALA SOON PROM CHEEMS PEAQ FORM HNT Q DOG KSM ORDI GAS GMX SAFE RED NMR ZETA SPK RIF QUBIC BERA KAITO CROSS BABYDOGE IP MUBARAK BANANAS31 SUSHI TURBO ARC ZIL UAI BIO BOME ENJ ROSE BABY DEXE BAN MERL GPS CTC COAI PHA BLUR DEEP HUMA BRETT TRB JELLYJELLY FLOW PNUT POPCAT NIL TOSHI HOLO DUSK NOT ALCH MASK EDU AVNT MOG MEW CATI SSV MOODENG PEOPLE MOVE SXT MOCA ONG PROVE COTI BAND RATS SYN ALEO MEME UMA LAB APR B3 GIGGLE NEIRO SQD POWR APEX MMT ORBS SIGN FLUX SKYAI STEEM ONE ZORA MANTA CARV CORE KGEN VELVET HIVE FLOCK ZEREBRO CLOUD IOST XAN TNSR CTSI AUCTION ZBT GMT METIS CETUS BIGTIME OPEN USUAL WOO EDEN XNY HEMI ICNT HOME MOVR INIT HYPER SOLV RIVER AIXBT CGPT STG BANK FIDA GIGA CTK BICO CHR ENSO DIA JCT WCT CYBER GRIFFAIN SOPH DODO MIRA PIPPIN GOAT AVA SIREN EPIC SAPIEN GTC MYX CAT SPELL ICX BLESS DOLO ORDER DRIFT BMT CLANKER MAV RARE PONKE DOOD DYM STBL CHILLGUY BTR GLMR GUN GODS HMSTR BLUAI NEWT TRUST COOKIE ACT SAGA BEL SONIC RESOLV TAC LIGHT SWARMS BLAST CLO MITO NOM QUICK TREE HFT""".split()]
STATE_FILE = "state.json"
TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
TEST = os.environ.get("TEST", "").lower() == "true"
MAX_ALERTS = 3

def evaluate(sym, cfg, now):
    """returns (list of candidate dicts, got_data)"""
    out = []
    d5, src = fetch_window(sym, now - pd.Timedelta(days=SCAN_DAYS), now)
    if d5 is None: return out, False
    cur = float(d5.c.iloc[-1]); utc0 = pd.Timestamp("1970-01-01", tz="UTC")
    for direction in ("short", "long"):
        sgn = 1 if direction == "short" else -1
        d = d5 if sgn == 1 else flip(d5)
        h1 = rs(d, "1h"); setups, st, plo = find_setups(h1, cfg)
        setups = [s for s in setups if s["mss_close"] > d.index[-1] - pd.Timedelta(hours=cfg.setup_hours)]
        if not setups: continue
        zones = [z for tf in cfg.zone_tfs for z in fvg_zones(rs(d, tf), tf, cfg.min_fvg_pct)]
        pl = PivotLows(plo, h1, cfg.pivot_n); s = setups[-1]
        r = run_setup(s, d, zones, pl, cfg, utc0)
        status, entry_f, ext, stop_f, zlo, zhi = None, None, None, None, None, None
        if r.get("trade") and r.get("still_open") and now - r["entry_time"] < pd.Timedelta(hours=3):
            status, entry_f, stop_f, ext = "TRIGGERED", r["entry"], r["stop"], r["stop"]
            z = None
        elif r.get("pending") and r["zones"]:
            status, stop_f, ext = "WATCH", r["stop"], r["ext"]
            z = min(r["zones"], key=lambda z: max(0.0, z["lo"] - d.c.iloc[-1]))
            zlo, zhi = z["lo"], z["hi"]; entry_f = (zlo + zhi) / 2
        else:
            continue
        risk = stop_f - entry_f
        if risk <= 0: continue
        # fib TP1 (info/placeholder swing low: lowest low of the 14 days before the swept high)
        try:
            d14o, _s = fetch_window(sym, s["sweep_time"] - pd.Timedelta(days=14), s["sweep_time"] + pd.Timedelta(hours=1))
        except Exception:
            d14o = None
        if d14o is None or len(d14o) < 100: continue
        d14 = d14o if sgn == 1 else flip(d14o)
        lo14 = float(d14.l.min()); fib_f = ext - 0.382 * (ext - lo14)
        if fib_f >= entry_f: continue
        rr = (entry_f - fib_f) / risk
        need = (2 * stop_f + fib_f) / 3          # entry (flipped) must be >= this for 2:1
        if status == "WATCH":
            if need > zhi: continue              # 2:1 is impossible anywhere inside the zone
            frac = (zhi - max(zlo, need)) / (zhi - zlo) if zhi > zlo else 1.0
        else:
            if rr < 2: continue
            frac = 1.0
        out.append(dict(sym=sym.replace("USDT", ""), direction=direction.upper(), status=status, rr=rr, frac=frac,
                        zone=(None if zlo is None else sorted([sgn * zlo, sgn * zhi])),
                        alert=(None if zlo is None else sgn * zlo), price=cur,
                        dist=(None if zlo is None else abs(sgn * zlo - cur) / cur * 100),
                        stop=sgn * stop_f, stop_pct=risk / abs(entry_f) * 100, fib=sgn * fib_f,
                        need=sgn * need, need_op=(">=" if sgn == 1 else "<="), entry=sgn * entry_f,
                        sweep=fmt(s["sweep_time"]), mss=fmt(s["mss_close"]),
                        key=f"{sym}|{direction}|{s['mss_close'].isoformat()}"))
    return out, True

def fmt(ts): return ts.tz_convert(TZ).strftime("%m-%d %H:%M")

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
    if c["status"] == "WATCH":
        z = c["zone"]
        return (f"{c['sym']} {c['direction']} | watch zone {z[0]:.6g}-{z[1]:.6g} (price {c['price']:.6g}, {c['dist']:.1f}% away)\n"
                f"  2:1 needs the FIRST 5m close {c['need_op']} {c['need']:.6g} inside the zone\n"
                f"  stop {c['stop']:.6g} ({c['stop_pct']:.1f}%) | fib TP1 {c['fib']:.6g} | R:R at zone middle {c['rr']:.2f}\n"
                f"  sweep {c['sweep']}, MSS {c['mss']}")
    return (f"{c['sym']} {c['direction']} | TRIGGERED, entry {c['entry']:.6g}\n"
            f"  stop {c['stop']:.6g} ({c['stop_pct']:.1f}%) | fib TP1 {c['fib']:.6g} | R:R {c['rr']:.2f}\n"
            f"  sweep {c['sweep']}, MSS {c['mss']}")

def main():
    cfg = Cfg(); cfg.min_rr = 0.0          # do not let the old pivot rule kill setups
    now = pd.Timestamp.now(tz="UTC")
    try: state = json.load(open(STATE_FILE))
    except Exception: state = {}
    seen = state.get("seen", {})
    results, with_data = [], 0
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(evaluate, sym, cfg, now): sym for sym in ALL_COINS}
        for f in cf.as_completed(futs):
            try:
                out, ok = f.result()
                with_data += 1 if ok else 0; results += out
            except Exception as e:
                print("error", futs[f], type(e).__name__, e)
    results.sort(key=lambda c: (c["status"] != "TRIGGERED", -c["rr"]))
    print(f"coins {len(ALL_COINS)} | with data {with_data} | candidates that can reach 2:1: {len(results)}")
    fresh = [c for c in results if seen.get(c["key"]) != c["status"]]
    if TEST:
        top = (results[:MAX_ALERTS] or [])
        push("V7 scanner TEST", "Test message. " + (("Current top:\n" + "\n".join(describe(c) for c in top)) if top else "No setups can reach 2:1 right now."))
    elif fresh:
        top = fresh[:MAX_ALERTS]
        push(f"V7: {len(top)} setup{'s' if len(top) > 1 else ''} (top by R:R)", "\n\n".join(describe(c) for c in top), priority="high")
        for c in top: seen[c["key"]] = c["status"]
    # keep the seen list small
    if len(seen) > 400: seen = dict(list(seen.items())[-200:])
    # daily heartbeat (after 09:00 Toronto)
    today = now.tz_convert(TZ).strftime("%Y-%m-%d")
    if now.tz_convert(TZ).hour >= 9 and state.get("heartbeat") != today and not TEST:
        push("V7 scanner alive", f"Scanned {with_data} of {len(ALL_COINS)} coins with data. {len(results)} live setups can reach 2:1 inside their zone right now.", priority="low", tags="white_check_mark")
        state["heartbeat"] = today
    if with_data < len(ALL_COINS) * 0.4 and state.get("datawarn") != today:
        push("V7 scanner: data problem", f"Only {with_data} of {len(ALL_COINS)} coins returned data.", priority="high", tags="warning")
        state["datawarn"] = today
    state["seen"] = seen
    json.dump(state, open(STATE_FILE, "w"))

if __name__ == "__main__":
    main()
