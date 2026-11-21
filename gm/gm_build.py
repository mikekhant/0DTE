"""
gm_build.py - turn the feed's latest.json into gamma-map run files and a compact briefing summary,
so /0dte never retypes bars or levels by hand.

  python3 gm_build.py latest.json OUTDIR [--events events.json] [--note QQQ "text"] [--note SPY "text"]

Writes into OUTDIR:
  run_<date>.json    snapshot run for the trading day the chain describes (merge with gm_merge.py)
  close_<date>.json  only after the close: finishes the session's page to 16:00 (no snapshot)
  summary.json       every number the briefing needs, per ticker
Prints summary.json plus the file names. Notes over 90 characters are cut; when no flip is found within
+-6% the edge of the scan is stored and the note says so.
"""
import json, sys
from datetime import datetime

MON = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
DOW = "Monday Tuesday Wednesday Thursday Friday Saturday Sunday".split()

def mins(hm):
    h, m = hm.split(":"); return int(h) * 60 + int(m)

def label(d):
    dt = datetime.strptime(d, "%Y-%m-%d"); return "%s %d" % (MON[dt.month - 1], dt.day), DOW[dt.weekday()]

def main(a):
    src, outdir = a[0], a[1]
    events, notes = None, {}
    i = 2
    while i < len(a):
        if a[i] == "--events": events = json.load(open(a[i + 1], encoding="utf-8")); i += 2
        elif a[i] == "--note": notes[a[i + 1].upper()] = a[i + 2]; i += 3
        else: raise SystemExit("unknown arg " + a[i])
    J = json.load(open(src, encoding="utf-8-sig"))
    if J.get("error"): raise SystemExit(json.dumps(J))
    asof = datetime.strptime(J["asOfET"], "%Y-%m-%d %H:%M:%S")
    T = J["tickers"]; syms = [s for s in ("QQQ", "SPY") if s in T]
    exp = T[syms[0]]["gex"]["next_expiry"]
    today = T[syms[0]]["intra"].get("today")
    lastM = mins(T[syms[0]]["intra"]["lastET"]) if T[syms[0]]["intra"].get("lastET") else 0
    intraday = (today == exp)
    vols = J.get("vols") or {}
    vol = {k: (vols.get(k) or [None])[0] for k in ("VIX1D", "VIX", "VXN")}
    hm = asof.strftime("%H:%M").lstrip("0") or "0:00"
    if hm.startswith(":"): hm = "0" + hm
    if intraday: t = hm
    elif asof.strftime("%Y-%m-%d") == exp: t = hm + " pre"
    else: t = DOW[asof.weekday()][:3] + " " + hm
    out, summary, snap = {}, {"asOfET": J["asOfET"], "source": J.get("source"), "login": J.get("login"),
                              "vols": vols, "mode": "intraday" if intraday else "premarket", "snapshotT": t}, {"t": t, "vol": vol}
    run = {"date": exp, "dateLabel": label(exp)[0], "weekday": label(exp)[1], "tickers": {},
           "barsThrough": T[syms[0]]["intra"]["lastET"] if intraday else None}
    close = None
    if not intraday and today and lastM >= 15 * 60 + 59:
        close = {"date": today, "dateLabel": label(today)[0], "weekday": label(today)[1], "barsThrough": "16:00", "tickers": {}}
    for s in syms:
        g, it = T[s]["gex"], T[s]["intra"]
        q = (J.get("quotes") or {}).get(s) or {}
        if intraday:
            run["tickers"][s] = {k: it.get(k) for k in ("prevClose", "pdh", "pdl", "open", "orh", "orl")}
            run["tickers"][s]["bars"] = it.get("bars5") or []
            spot = q.get("last") or g.get("spot")
        else:
            # official close: the chain's spot right after the close, the chain's prior close the next morning
            pc = g.get("spot") if asof.strftime("%Y-%m-%d") == today else g.get("prevClose")
            run["tickers"][s] = {"prevClose": pc if pc else it.get("last"), "pdh": it.get("hi"), "pdl": it.get("lo"),
                                 "open": None, "orh": None, "orl": None, "bars": []}
            spot = g.get("spot")
            if close:
                close["tickers"][s] = {k: it.get(k) for k in ("prevClose", "pdh", "pdl", "open", "orh", "orl")}
                close["tickers"][s]["bars"] = it.get("bars5") or []
        flip, edge = g.get("flip"), False
        if flip is None:
            flip = round(g["spot"] * (1.06 if (g.get("netGEX_all_$mm_per1pct") or 0) < 0 else 0.94), 2); edge = True
        note = notes.get(s, "")
        if edge and "flip" not in note.lower():
            note = ("No flip within 6%% (shown at %g). " % flip + note).strip()
        if len(note) > 90: note = note[:89].rstrip() + "…"
        snap[s] = {"spot": round(spot, 2), "flip": flip, "gexAll": g.get("netGEX_all_$mm_per1pct"), "gex0": g.get("netGEX_0dte_$mm"),
                   "cw": g.get("callWall"), "pw": g.get("putWall"), "cw0": g.get("callWall_0dte"), "pw0": g.get("putWall_0dte"),
                   "mp": g.get("maxPain_0dte"), "straddle": g.get("straddle_0dte"), "em": g.get("expMove_pct"),
                   "oiPC": g.get("oi_0dte_PC"), "volPC": g.get("vol_0dte_PC"), "vwap": it.get("vwap") if intraday else None,
                   "strikes": g.get("strikesGEX_$mm"), "note": note}
        summary[s] = {"quote": q, "flipEdge": edge,
                      "gex": {k: v for k, v in g.items() if k not in ("strikesGEX_$mm", "profile")},
                      "intra": {k: v for k, v in it.items() if k != "bars5"}}
        for extra in ("tape", "depth"):
            if T[s].get(extra) is not None: summary[s][extra] = T[s][extra]
    if J.get("stream") is not None: summary["stream"] = J["stream"]
    run["snapshot"] = snap
    if events is not None:
        run["events"] = events
        if close: close["events"] = events
    files = []
    p = "%s/run_%s.json" % (outdir, exp); json.dump(run, open(p, "w"), ensure_ascii=False); files.append(p)
    if close:
        p = "%s/close_%s.json" % (outdir, today); json.dump(close, open(p, "w"), ensure_ascii=False); files.append(p)
    json.dump(summary, open("%s/summary.json" % outdir, "w"), ensure_ascii=False)
    print(json.dumps(summary, separators=(",", ":"), ensure_ascii=False))
    print("FILES", " ".join(files))

if __name__ == "__main__":
    main(sys.argv[1:])
