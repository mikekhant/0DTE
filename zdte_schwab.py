"""
zdte_schwab.py - live 0DTE dealer-gamma data for QQQ and SPY from the Schwab Trader API
(Market Data Production). Used by the /0dte briefing.

Run:   python zdte_schwab.py                 -> compact JSON on stdout
       python zdte_schwab.py --symbols QQQ   -> one ticker
       python zdte_schwab.py --check         -> login status only

Needs: schwab_config.json (your Client ID / Secret) and tokens.json (created by schwab_auth.py),
both in this folder. Python 3.8+ standard library only - nothing to install.

Inside the feed, run() also starts the order-book and tape workers in zdte_stream.py, and
adds their latest output (tape, depth, stream) to the JSON.
"""
import base64, json, math, os, sys, threading, time, urllib.parse, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "schwab_config.json"
TOKENS = HERE / "tokens.json"
API = "https://api.schwabapi.com"
REFRESH_LIFETIME_S = 7 * 24 * 3600

# one lock for token refreshes, shared by the feed and the stream workers (survives module reloads)
if not hasattr(sys, "_zdte_token_lock"):
    sys._zdte_token_lock = threading.RLock()

# ---------------------------------------------------------------- time (US Eastern, no tzdata needed)
def _nth_sunday(year, month, n):
    d = datetime(year, month, 1)
    d += timedelta(days=(6 - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)

def et_offset_hours(utc_dt):
    """-4 during US daylight time, else -5. DST: 2nd Sun Mar 07:00 UTC to 1st Sun Nov 06:00 UTC."""
    y = utc_dt.year
    start = _nth_sunday(y, 3, 2).replace(hour=7, tzinfo=timezone.utc)
    end = _nth_sunday(y, 11, 1).replace(hour=6, tzinfo=timezone.utc)
    return -4 if start <= utc_dt < end else -5

def to_et(ms):
    u = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return u + timedelta(hours=et_offset_hours(u))  # naive-ish ET wall clock (tz label is UTC; ignore it)

def expiry_ms(date_str):
    """16:00 ET on an ISO date, as epoch ms."""
    y, m, d = map(int, date_str[:10].split("-"))
    guess = datetime(y, m, d, 20, 0, tzinfo=timezone.utc)
    off = et_offset_hours(guess)
    return int(datetime(y, m, d, 16 - off, 0, tzinfo=timezone.utc).timestamp() * 1000)

def hm(minute_of_day):
    return "%d:%02d" % (minute_of_day // 60, minute_of_day % 60)

# ---------------------------------------------------------------- auth
def load_config():
    if not CONFIG.exists():
        raise SystemExit(json.dumps({"error": "missing_config", "detail": str(CONFIG)}))
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    if "PASTE" in cfg.get("app_key", "PASTE") or "PASTE" in cfg.get("app_secret", "PASTE"):
        raise SystemExit(json.dumps({"error": "config_not_filled", "detail": "Put your Client ID and Secret in schwab_config.json"}))
    cfg.setdefault("callback_url", "https://127.0.0.1")
    return cfg

def _basic(cfg):
    return base64.b64encode(f"{cfg['app_key']}:{cfg['app_secret']}".encode()).decode()

def token_request(cfg, form):
    req = urllib.request.Request(
        API + "/v1/oauth/token",
        data=urllib.parse.urlencode(form).encode(),
        headers={"Authorization": "Basic " + _basic(cfg), "Content-Type": "application/x-www-form-urlencoded"},
        method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())

def save_tokens(tok, refresh_obtained_at=None):
    now = time.time()
    data = {
        "access_token": tok["access_token"],
        "refresh_token": tok["refresh_token"],
        "access_expires_at": now + int(tok.get("expires_in", 1800)) - 60,
        "refresh_obtained_at": refresh_obtained_at or now,
    }
    tmp = TOKENS.with_name(TOKENS.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(tmp, TOKENS)
    return data

def load_tokens(cfg):
    with sys._zdte_token_lock:
        return _load_tokens(cfg)

def _load_tokens(cfg):
    if not TOKENS.exists():
        raise SystemExit(json.dumps({"error": "not_logged_in", "detail": "Run: python schwab_auth.py"}))
    t = json.loads(TOKENS.read_text(encoding="utf-8"))
    if time.time() - t["refresh_obtained_at"] > REFRESH_LIFETIME_S:
        raise SystemExit(json.dumps({"error": "login_expired", "detail": "The 7-day Schwab login expired. Run: python schwab_auth.py"}))
    if time.time() >= t["access_expires_at"]:
        try:
            new = token_request(cfg, {"grant_type": "refresh_token", "refresh_token": t["refresh_token"]})
        except urllib.error.HTTPError as e:
            raise SystemExit(json.dumps({"error": "refresh_failed", "status": e.code, "detail": "Run: python schwab_auth.py"}))
        new.setdefault("refresh_token", t["refresh_token"])
        t = save_tokens(new, refresh_obtained_at=t["refresh_obtained_at"])
    return t

def login_status():
    if not TOKENS.exists():
        return {"logged_in": False}
    t = json.loads(TOKENS.read_text(encoding="utf-8"))
    left = REFRESH_LIFETIME_S - (time.time() - t["refresh_obtained_at"])
    return {"logged_in": left > 0, "login_hours_left": round(left / 3600, 1)}

def api_get(path, params, token):
    url = API + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

# ---------------------------------------------------------------- math
def pdf(x): return math.exp(-x * x / 2) / math.sqrt(2 * math.pi)
def cdf(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))
def unit(s): return 100 * s * s * 0.01  # $ of hedging per 1% move, per unit gamma per contract
YEAR_MS = 365 * 864e5
HOUR_Y = 1 / (365 * 24)

def bs_gamma(K, iv, T, s):
    if not (iv > 0 and T > 0): return 0.0
    v = iv * math.sqrt(T); d1 = (math.log(s / K) + 0.5 * v * v) / v
    return pdf(d1) / (s * v)

def bs_delta(K, cp, iv, T, s):
    v = iv * math.sqrt(max(T, 1e-9)); d1 = (math.log(s / K) + 0.5 * v * v) / v
    return cdf(d1) if cp == "C" else cdf(d1) - 1

def num(x, default=0.0):
    try:
        x = float(x)
        return default if (math.isnan(x) or x <= -998) else x
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------- gamma engine (port of the Cboe JS engine)
def flatten_chain(chain, now_ms):
    opts = []
    for cp, key in (("C", "callExpDateMap"), ("P", "putExpDateMap")):
        for exp_key, strikes in (chain.get(key) or {}).items():
            ex = exp_key.split(":")[0]
            T = (expiry_ms(ex) - now_ms) / YEAR_MS
            if T <= 0: continue
            for k, arr in strikes.items():
                for c in arr:
                    iv = num(c.get("volatility")) / 100.0
                    opts.append({"ex": ex, "cp": cp, "K": float(k), "oi": num(c.get("openInterest")),
                                 "vol": num(c.get("totalVolume")), "g": max(num(c.get("gamma")), 0.0),
                                 "iv": iv if iv > 0 else 0.0, "bid": num(c.get("bid")), "ask": num(c.get("ask")),
                                 "T": T, "delayed": bool(c.get("isDelayed"))})
    return opts

def gamma_engine(sym, S, opts):
    sign = lambda o: 1 if o["cp"] == "C" else -1
    exps = sorted({o["ex"] for o in opts}); e0 = exps[0]
    z = [o for o in opts if o["ex"] == e0]
    by, tot, tot0 = {}, 0.0, 0.0
    c0 = p0 = cv0 = pv0 = cprem0 = pprem0 = 0.0
    for o in opts:
        g = o["g"] if o["g"] > 0 else bs_gamma(o["K"], o["iv"], o["T"], S)
        o["gg"] = g
        gx = sign(o) * g * o["oi"] * unit(S); tot += gx; by[o["K"]] = by.get(o["K"], 0) + gx
        if o["ex"] == e0:
            tot0 += gx; mid = (o["bid"] + o["ask"]) / 2
            if o["cp"] == "C": c0 += o["oi"]; cv0 += o["vol"]; cprem0 += o["vol"] * mid * 100
            else: p0 += o["oi"]; pv0 += o["vol"]; pprem0 += o["vol"] * mid * 100
    callG, putG, callG0, putG0 = {}, {}, {}, {}
    for o in opts:
        if abs(o["K"] / S - 1) > 0.1: continue
        gx = o["gg"] * o["oi"] * unit(S)
        (callG if o["cp"] == "C" else putG)[o["K"]] = (callG if o["cp"] == "C" else putG).get(o["K"], 0) + gx
        if o["ex"] == e0:
            (callG0 if o["cp"] == "C" else putG0)[o["K"]] = (callG0 if o["cp"] == "C" else putG0).get(o["K"], 0) + gx
    def argmax(d, ok):
        best, bv = None, 0
        for k, v in d.items():
            if ok(k) and v > bv: best, bv = k, v
        return best
    cw = argmax(callG, lambda k: k >= S * 0.995); pw = argmax(putG, lambda k: k <= S * 1.005)
    cw0 = argmax(callG0, lambda k: k >= S * 0.995); pw0 = argmax(putG0, lambda k: k <= S * 1.005)
    near = [o for o in opts if abs(o["K"] / S - 1) < 0.25 and o["oi"] > 0 and o["iv"] > 0]
    prof = []
    x = -0.06
    while x <= 0.0601:
        s = S * (1 + x)
        prof.append((s, sum(sign(o) * bs_gamma(o["K"], o["iv"], o["T"], s) * o["oi"] * unit(s) for o in near)))
        x += 0.0025
    flip = None
    for (a1, g1), (a2, g2) in zip(prof, prof[1:]):
        if (g1 < 0 <= g2) or (g1 > 0 >= g2):
            f = a1 + (a2 - a1) * (-g1) / (g2 - g1)
            if flip is None or abs(f - S) < abs(flip - S): flip = f
    atmK = min({o["K"] for o in z}, key=lambda k: abs(k - S))
    def mid(cp):
        o = next((o for o in z if o["K"] == atmK and o["cp"] == cp), None)
        return (o["bid"] + o["ask"]) / 2 if o else float("nan")
    straddle = mid("C") + mid("P")
    ks = sorted({o["K"] for o in z if abs(o["K"] / S - 1) < 0.05})
    mp, mpv = None, float("inf")
    for k in ks:
        pay = sum(o["oi"] * (max(0, k - o["K"]) if o["cp"] == "C" else max(0, o["K"] - k)) for o in z)
        if pay < mpv: mp, mpv = k, pay
    win = lambda k: abs(k / S - 1) < 0.04
    top = sorted(((k, v) for k, v in by.items() if win(k)), key=lambda kv: -abs(kv[1]))[:8]
    strikes = sorted((k, round(v / 1e6)) for k, v in by.items() if win(k) and abs(v) > 1e7)
    topVol0 = [[("%g" % o["K"]) + o["cp"], int(o["vol"]), int(o["oi"])]
               for o in sorted((o for o in z if abs(o["K"] - S) <= 3), key=lambda o: -o["vol"])[:8]]
    # hedging-flow model: 0DTE within 1.5% of spot, flat IV implied by the ATM straddle
    T0 = z[0]["T"]; atmIV = straddle / (0.8 * S * math.sqrt(T0)) if T0 > 0 and straddle == straddle else 0.0
    nz = [o for o in z if abs(o["K"] / S - 1) < 0.015]
    def g_at(T, w): return round(sum(bs_gamma(o["K"], atmIV, T, S) * w(o) * 100 for o in nz))
    dt = min(HOUR_Y, T0 * 0.9)
    def charm(f): return round(sum((bs_delta(o["K"], o["cp"], atmIV, max(T0 - dt, 1e-7), S) - bs_delta(o["K"], o["cp"], atmIV, T0, S)) * o["vol"] * 100 for o in nz if f(o)))
    hedge = {"atmIV": round(atmIV, 3), "hoursLeft": round(T0 / HOUR_Y, 2),
             "convOI_shPer1": g_at(T0, lambda o: sign(o) * o["oi"]),
             "callVol_shPer1": g_at(T0, lambda o: o["vol"] if o["cp"] == "C" else 0),
             "putVol_shPer1": g_at(T0, lambda o: o["vol"] if o["cp"] == "P" else 0),
             "charm1h_ITMcalls": charm(lambda o: o["cp"] == "C" and o["K"] < S),
             "charm1h_OTMcalls": charm(lambda o: o["cp"] == "C" and o["K"] >= S),
             "charm1h_OTMputs": charm(lambda o: o["cp"] == "P" and o["K"] < S),
             "charm1h_ITMputs": charm(lambda o: o["cp"] == "P" and o["K"] >= S),
             "note": "shares per $1 move if dealers are short 100% of that volume; scale by the net share you assume"}
    # 0DTE quotes near the money, for entries: strike -> [callBid, callAsk, callDelta, putBid, putAsk, putDelta]
    q0 = {}
    for o in z:
        if abs(o["K"] - S) <= 6:
            row = q0.setdefault("%g" % o["K"], [None] * 6)
            dl = round(bs_delta(o["K"], o["cp"], atmIV, T0, S), 2) if atmIV > 0 and T0 > 0 else None
            if o["cp"] == "C": row[0], row[1], row[2] = o["bid"], o["ask"], dl
            else: row[3], row[4], row[5] = o["bid"], o["ask"], dl
    r = lambda v, n=2: None if v is None or v != v else round(v, n)
    return {"sym": sym, "spot": r(S), "next_expiry": e0, "quotes0dte": q0,
            "netGEX_all_$mm_per1pct": round(tot / 1e6), "netGEX_0dte_$mm": round(tot0 / 1e6),
            "regime": "no flip within ±6%" if flip is None else ("above flip (positive gamma)" if S > flip else "below flip (negative gamma)"),
            "flip": r(flip), "callWall": cw, "putWall": pw, "callWall_0dte": cw0, "putWall_0dte": pw0,
            "maxPain_0dte": mp, "atm": atmK, "straddle_0dte": r(straddle), "expMove_pct": r(straddle / S * 100),
            "oi_0dte_PC": r(p0 / c0) if c0 else None, "vol_0dte_PC": r(pv0 / cv0) if cv0 else None,
            "prem_0dte_calls_$k": round(cprem0 / 1e3), "prem_0dte_puts_$k": round(pprem0 / 1e3),
            "topGEXstrikes_$mm": [[k, round(v / 1e6)] for k, v in top], "strikesGEX_$mm": [list(x) for x in strikes],
            "topVol0dte": topVol0, "profile": [[round(s, 2), round(g / 1e6)] for i, (s, g) in enumerate(prof) if i % 4 == 0],
            "hedge": hedge, "anyDelayed": any(o["delayed"] for o in z)}

# ---------------------------------------------------------------- intraday state from 1-minute candles
def ema(arr, n):
    k = 2 / (n + 1); e = arr[0]
    for v in arr: e = v * k + e * (1 - k)
    return round(e, 2)

def intraday(sym, hist):
    bars = []
    for c in hist.get("candles", []):
        t = to_et(c["datetime"]); m = t.hour * 60 + t.minute
        if 570 <= m < 960:
            bars.append({"d": t.strftime("%Y-%m-%d"), "m": m, "o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"], "v": c.get("volume", 0)})
    days = sorted({b["d"] for b in bars})
    if not days: return {"sym": sym, "session": "no bars"}
    today = days[-1]; tb = [b for b in bars if b["d"] == today]; pdb = [b for b in bars if len(days) > 1 and b["d"] == days[-2]]
    pc = pdb[-1]["c"] if pdb else hist.get("previousClose")
    lastM = tb[-1]["m"]
    hi = lo = None; hiM = loM = 0; pv = vv = 0.0; cross = cross60 = 0; prev = None; cum = 0; bad = 0
    for b in tb:
        if b["c"] > b["h"] + 0.01 or b["c"] < b["l"] - 0.01: bad += 1
        if hi is None or b["h"] > hi: hi, hiM = b["h"], b["m"]
        if lo is None or b["l"] < lo: lo, loM = b["l"], b["m"]
        pv += (b["h"] + b["l"] + b["c"]) / 3 * b["v"]; vv += b["v"]; cum += b["v"]
        side = 1 if b["c"] >= pv / max(vv, 1) else -1
        if prev is not None and side != prev:
            cross += 1
            if b["m"] > lastM - 60: cross60 += 1
        prev = side
    orb = [b for b in tb if b["m"] < 585]
    prior = [sum(b["v"] for b in bars if b["d"] == d and b["m"] <= lastM) for d in days[:-1]]
    rel = cum / (sum(prior) / len(prior)) if prior and sum(prior) else None
    def agg(n): return [tb[min(i + n, len(tb)) - 1]["c"] for i in range(0, len(tb), n)]
    b5 = []; pv2 = vv2 = 0.0
    for i in range(0, len(tb), 5):
        s = tb[i:i + 5]
        for b in s: pv2 += (b["h"] + b["l"] + b["c"]) / 3 * b["v"]; vv2 += b["v"]
        b5.append([s[0]["m"], round(s[0]["o"], 2), round(max(b["h"] for b in s), 2), round(min(b["l"] for b in s), 2), round(s[-1]["c"], 2), round(pv2 / max(vv2, 1), 2)])
    last = tb[-1]["c"]; op = tb[0]["o"]
    return {"sym": sym, "today": today, "lastET": hm(lastM), "last": round(last, 2), "open": round(op, 2),
            "prevClose": round(pc, 2) if pc else None,
            "pdh": round(max(b["h"] for b in pdb), 2) if pdb else None, "pdl": round(min(b["l"] for b in pdb), 2) if pdb else None,
            "gapPct": round((op / pc - 1) * 100, 2) if pc else None, "chgPct": round((last / pc - 1) * 100, 2) if pc else None,
            "hi": round(hi, 2), "hiAt": hm(hiM), "lo": round(lo, 2), "loAt": hm(loM),
            "rangePct": round((hi - lo) / pc * 100, 2) if pc else None, "vwap": round(pv / max(vv, 1), 2),
            "orh": round(max(b["h"] for b in orb), 2) if orb else None, "orl": round(min(b["l"] for b in orb), 2) if orb else None,
            "relVol": round(rel, 2) if rel else None, "vwapCrosses": cross, "crossesLast60": cross60,
            "ema3": {"e9": ema(agg(3), 9), "e20": ema(agg(3), 20)}, "ema5": {"e9": ema(agg(5), 9), "e20": ema(agg(5), 20)},
            "badBars": bad, "bars5": b5}

# ---------------------------------------------------------------- order book and tape workers (zdte_stream.py)
def ensure_workers():
    """Inside the feed only: start the zdte_stream workers, or restart them if zdte_stream.py changed
    or a worker died. Any problem here is written to stream_status.json and never stops the feed."""
    if not Path(sys.argv[0] or "").name.lower().startswith("zdte_feed"):
        return
    try:
        import importlib
        import zdte_stream
        reg = getattr(sys, "_zdte_workers", None)
        m = (HERE / "zdte_stream.py").stat().st_mtime
        if reg and reg["mtime"] == m and all(t.is_alive() for t in reg["threads"]):
            return
        if reg:
            reg["kill"]()
            for t in reg["threads"]: t.join(timeout=5)
            zdte_stream = importlib.reload(zdte_stream)
        sys._zdte_workers = zdte_stream.start()
    except Exception as e:
        try:
            (HERE / "stream_status.json").write_text(json.dumps({"stream": "worker start failed: %s: %s" % (type(e).__name__, e)}), encoding="utf-8")
        except OSError:
            pass

def _read_json(name):
    p = HERE / name
    try:
        return json.loads(p.read_text(encoding="utf-8")), time.time() - p.stat().st_mtime
    except (OSError, ValueError):
        return None, None

def attach_stream_data(out, symbols):
    """Add the workers' latest files: tape (today's only), depth (if under 2 minutes old), stream status."""
    status, _ = _read_json("stream_status.json")
    if status is not None: out["stream"] = status
    tape, tage = _read_json("tape.json")
    depth, dage = _read_json("depth.json")
    today = out["asOfET"][:10]
    for sym in symbols:
        e = out["tickers"].get(sym)
        if e is None: continue
        if tape and tape.get("date") == today and (tape.get("tickers") or {}).get(sym):
            e["tape"] = dict(tape["tickers"][sym], asOfET=tape.get("asOfET"), method=tape.get("method"))
        if depth and (depth.get("tickers") or {}).get(sym):
            if dage is not None and dage <= 120:
                e["depth"] = dict(depth["tickers"][sym], asOfET=depth.get("asOfET"), note=depth.get("note"))
            else:
                e["depth"] = {"stale": True, "asOfET": depth.get("asOfET")}

# ---------------------------------------------------------------- main
def run(symbols):
    ensure_workers()
    cfg = load_config(); tok = load_tokens(cfg)["access_token"]
    now = datetime.now(timezone.utc); now_ms = int(now.timestamp() * 1000)
    et_now = to_et(now_ms)
    start = et_now.date(); end = start + timedelta(days=90)
    out = {"source": "schwab", "asOfET": et_now.strftime("%Y-%m-%d %H:%M:%S"), "login": login_status(), "tickers": {}}
    # vol indices and quotes in one call; unknown index symbols are simply missing
    try:
        q = api_get("/marketdata/v1/quotes", {"symbols": ",".join(symbols + ["$VIX", "$VIX1D", "$VIX9D", "$VXN"]), "fields": "quote", "indicative": "false"}, tok)
        out["vols"] = {k.strip("$"): [v.get("quote", {}).get("lastPrice"), v.get("quote", {}).get("closePrice")]
                       for k, v in q.items() if k.startswith("$")}
        out["quotes"] = {s: {"last": q.get(s, {}).get("quote", {}).get("lastPrice"),
                             "quoteTimeET": to_et(q[s]["quote"]["quoteTime"]).strftime("%H:%M:%S") if q.get(s, {}).get("quote", {}).get("quoteTime") else None,
                             "realtime": q.get(s, {}).get("realtime")} for s in symbols}
    except urllib.error.HTTPError as e:
        out["vols"] = {"error": e.code}
    for sym in symbols:
        entry = {}
        try:
            chain = api_get("/marketdata/v1/chains", {"symbol": sym, "contractType": "ALL", "strikeCount": 200,
                            "includeUnderlyingQuote": "true", "strategy": "SINGLE",
                            "fromDate": start.isoformat(), "toDate": end.isoformat()}, tok)
            und = chain.get("underlying") or {}
            S = num(und.get("last")) or num(chain.get("underlyingPrice")) or num(und.get("mark"))
            opts = flatten_chain(chain, now_ms)
            entry["gex"] = gamma_engine(sym, S, opts) if opts else {"error": "empty_chain"}
            entry["gex"]["prevClose"] = und.get("close")
        except urllib.error.HTTPError as e:
            entry["gex"] = {"error": "chain_http_%d" % e.code}
        try:
            hist = api_get("/marketdata/v1/pricehistory", {"symbol": sym, "periodType": "day", "period": 5,
                           "frequencyType": "minute", "frequency": 1, "needExtendedHoursData": "false",
                           "needPreviousClose": "true", "endDate": now_ms}, tok)
            entry["intra"] = intraday(sym, hist)
        except urllib.error.HTTPError as e:
            entry["intra"] = {"error": "history_http_%d" % e.code}
        out["tickers"][sym] = entry
    attach_stream_data(out, symbols)
    return out

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--check" in args:
        print(json.dumps(login_status())); sys.exit(0)
    syms = ["QQQ", "SPY"]
    if "--symbols" in args:
        syms = [s.strip().upper() for s in args[args.index("--symbols") + 1].split(",") if s.strip()]
    print(json.dumps(run(syms), separators=(",", ":")))
