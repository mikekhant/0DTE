"""
zdte_stream.py - order-book depth and an estimated options tape for the /0dte briefing.

Nothing to launch: zdte_schwab.run() starts these workers inside the feed (zdte_feed.pyw)
and restarts them when this file changes.

Two workers:
  tape poll  Every 60 s from 9:28 to 16:02 ET on weekdays, pulls the next-expiry (0DTE) chains
             for QQQ and SPY and classifies each contract's new volume as bought (last trade at
             or near the ask), sold (at or near the bid) or mid. Needs only Market Data Production.
  streamer   Schwab's streaming API, 9:25 to 16:05 ET: Level 2 books for QQQ and SPY
             (NASDAQ_BOOK and NYSE_BOOK), books for the near-the-money 0DTE options
             (OPTIONS_BOOK), and level-one option quotes (LEVELONE_OPTIONS), which classify
             trades as they happen instead of once a minute.
             The streamer address comes from GET /trader/v1/userPreference, which belongs to the
             "Accounts and Trading Production" API product. Without that product the streamer
             stays off (stream_status.json says so) and the tape poll still runs.

Writes, in this folder only: tape.json, depth.json, stream_status.json, tape_state.json.
Never calls account, order or position endpoints, and never writes anything it reads from
userPreference (account numbers, customer IDs) to disk.

Manual tests (Command Prompt, in this folder):
  python zdte_stream.py --poll      one tape poll; prints tape.json
  python zdte_stream.py --test      about 30 s of streaming, any time of day; prints a summary
Python 3.8+ standard library only.
"""
import base64, hashlib, json, os, socket, ssl, struct, sys, threading, time, urllib.error, urllib.parse
from datetime import timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import zdte_schwab as z

TAPE_STATE = HERE / "tape_state.json"
TAPE = HERE / "tape.json"
DEPTH = HERE / "depth.json"
STATUS = HERE / "stream_status.json"
UNDERLYINGS = ["QQQ", "SPY"]
EQUITY_BOOKS = ("NASDAQ_BOOK", "NYSE_BOOK")
L1_STRIKES = 10      # strikes either side of spot streamed on LEVELONE_OPTIONS (calls and puts)
BOOK_STRIKES = 3     # strikes either side of spot streamed on OPTIONS_BOOK
WITHIN = (0.10, 0.25, 0.50, 1.00)   # $ from the mid for depth totals


# ---------------------------------------------------------------- small helpers
def now_et():
    return z.to_et(int(time.time() * 1000))

def et_label(t=None):
    return (t or now_et()).strftime("%Y-%m-%d %H:%M:%S")

def in_window(start_min, end_min):
    t = now_et(); m = t.hour * 60 + t.minute
    return t.weekday() < 5 and start_min <= m < end_min

def write_atomic(path, obj):
    """Write a temp file and rename it over the target. The temp name is unique per thread, and the
    rename is retried because Windows refuses it for a moment while another program reads the file."""
    tmp = path.with_name("%s.%d-%d.tmp" % (path.name, os.getpid(), threading.get_ident()))
    tmp.write_text(json.dumps(obj, separators=(",", ":")), encoding="utf-8")
    for i in range(10):
        try:
            os.replace(tmp, path); return
        except PermissionError:
            time.sleep(0.05 * (i + 1))
    try: tmp.unlink()
    except OSError: pass

def access_token():
    return z.load_tokens(z.load_config())["access_token"]

def classify(last, bid, ask):
    """b = at or near the ask (bought), s = at or near the bid (sold), m = in between or unknown."""
    if not last or bid is None or not ask or ask <= 0 or bid < 0:
        return "m"
    spr = ask - bid
    if spr <= 0:
        return "m"
    if last >= ask - 0.25 * spr:
        return "b"
    if last <= bid + 0.25 * spr:
        return "s"
    return "m"

def strike_label(K, cp):
    return ("%g" % K) + cp


# ---------------------------------------------------------------- tape aggregator
class Tape:
    """Cumulative-volume deltas per contract, classified by the trade price against the quote.
    Both the poll and the stream feed it; because it works from each contract's cumulative
    volume, switching between them never double-counts."""
    STALE_S = 300   # a gap longer than this (PC asleep, feed stopped) goes to 'unclassified'

    def __init__(self):
        self.lock = threading.RLock()
        self.dirty = False
        self.s = self._fresh(now_et().strftime("%Y-%m-%d"))
        try:
            old = json.loads(TAPE_STATE.read_text(encoding="utf-8"))
            if old.get("date") == self.s["date"] and old.get("v") == 1:
                self.s = old
        except Exception:
            pass

    @staticmethod
    def _fresh(d):
        return {"v": 1, "date": d, "c": {},
                "m": {u: {} for u in UNDERLYINGS}, "k": {u: {} for u in UNDERLYINGS},
                "unc": {u: 0 for u in UNDERLYINGS}, "src": {u: {"stream": 0, "poll": 0} for u in UNDERLYINGS}}

    def _roll(self):
        d = now_et().strftime("%Y-%m-%d")
        if d != self.s["date"]:
            self.s = self._fresh(d); self.dirty = True

    def recently_streamed(self, sym, within=90):
        with self.lock:
            c = self.s["c"].get(sym)
            return bool(c and c[2] == "stream" and time.time() - c[1] < within)

    def update(self, u, sym, cp, K, cum, last, bid, ask, source, now=None):
        now = now or time.time()
        with self.lock:
            self._roll()
            c = self.s["c"].get(sym)
            if c is None:   # first sight today: volume so far can't be classified
                self.s["c"][sym] = [cum, now, source]
                if cum > 0: self.s["unc"][u] += int(cum)
                self.dirty = True
                return None
            dv = cum - c[0]; gap = now - c[1]
            c[1] = now; c[2] = source
            if dv <= 0:
                if dv < 0: c[0] = cum
                return None
            c[0] = cum; self.dirty = True
            if gap > self.STALE_S:
                self.s["unc"][u] += int(dv); return None
            side = classify(last, bid, ask)
            t = z.to_et(int(now * 1000)); minute = str(t.hour * 60 + t.minute)
            row = self.s["m"][u].setdefault(minute, [0] * 10)
            base = 0 if cp == "C" else 3
            row[base + "bsm".index(side)] += int(dv)
            if side != "m":
                row[6 + (0 if cp == "C" else 2) + (0 if side == "b" else 1)] += int(round(dv * last * 100))
            k = self.s["k"][u].setdefault(strike_label(K, cp), [0, 0, 0])
            k["bsm".index(side)] += int(dv)
            self.s["src"][u][source] = self.s["src"][u].get(source, 0) + int(dv)
            return side

    @staticmethod
    def _view(rows):
        t = [0] * 10
        for r in rows:
            for i, x in enumerate(r): t[i] += x
        cb, cs, cm, pb, ps, pm, cbp, csp, pbp, psp = t
        pct = lambda a, b: round(100.0 * a / (a + b)) if a + b else None
        return {"calls": {"bought": cb, "sold": cs, "mid": cm, "boughtPct": pct(cb, cs)},
                "puts": {"bought": pb, "sold": ps, "mid": pm, "boughtPct": pct(pb, ps)},
                "premium_$k": {"callsBought": round(cbp / 1e3), "callsSold": round(csp / 1e3),
                               "putsBought": round(pbp / 1e3), "putsSold": round(psp / 1e3)},
                "netCallBought_$k": round((cbp - csp) / 1e3), "netPutBought_$k": round((pbp - psp) / 1e3)}

    def summary(self):
        with self.lock:
            self._roll()
            t = now_et(); m = t.hour * 60 + t.minute
            out = {}
            for u in UNDERLYINGS:
                mm = self.s["m"][u]
                win = lambda n: [r for k, r in mm.items() if int(k) > m - n]
                ks = [(k, v[0] - v[1]) for k, v in self.s["k"][u].items()]
                out[u] = {"session": self._view(mm.values()), "last15": self._view(win(15)), "last60": self._view(win(60)),
                          "mostBought": [[k, n] for k, n in sorted(ks, key=lambda x: -x[1])[:4] if n > 0],
                          "mostSold": [[k, n] for k, n in sorted(ks, key=lambda x: x[1])[:3] if n < 0],
                          "unclassified": self.s["unc"][u], "volBySource": dict(self.s["src"][u])}
            return {"asOfET": et_label(t), "date": self.s["date"],
                    "method": "volume classified by last trade vs bid/ask: at/near ask = bought, near bid = sold. "
                              "Poll = once a minute (coarse); stream = per update. An estimate, not trade-by-trade data.",
                    "tickers": out}

    def save(self):
        with self.lock:
            if not self.dirty: return False
            write_atomic(TAPE_STATE, self.s); self.dirty = False
        write_atomic(TAPE, self.summary())
        return True


# ---------------------------------------------------------------- shared state
class Hub:
    def __init__(self):
        self.lock = threading.RLock()
        self.tape = Tape()
        self.spot = {}          # underlying -> last price (book mid, else chain)
        self.contracts = {}     # underlying -> [(K, cp, symbol)] for the next expiry
        self.meta = {}          # option symbol -> (underlying, cp, K)
        self.books = {}         # (service, symbol) -> {"t", "recv", "bids", "asks"}
        self.obooks = {}        # option symbol -> same
        self.q = {}             # option symbol -> current level-one quote
        self.imb = {u: [] for u in UNDERLYINGS}
        self.status = {"stream": "starting", "services": {}, "tapePoll": {}}
        self.ws = None

    def set_status(self, **kw):
        with self.lock:
            self.status.update(kw); self.status["updatedET"] = et_label()
            try: write_atomic(STATUS, self.status)
            except OSError: pass

    def set_contracts(self, u, S, rows):
        with self.lock:
            if S: self.spot.setdefault(u + ":chain", S); self.spot[u + ":chain"] = S
            self.contracts[u] = [(K, cp, sym) for (K, cp, sym) in rows]
            for K, cp, sym in rows: self.meta[sym] = (u, cp, K)

    def ref_price(self, u):
        b = self.best_book(u)
        if b and b["bids"] and b["asks"]:
            return (b["bids"][0][0] + b["asks"][0][0]) / 2
        return self.spot.get(u + ":chain")

    def option_keys(self):
        """(level-one symbols, book symbols) nearest the money, from the latest chain."""
        l1, ob = [], []
        with self.lock:
            for u in UNDERLYINGS:
                S = self.ref_price(u); rows = self.contracts.get(u) or []
                if not S or not rows: continue
                ks = sorted({K for K, _, _ in rows}, key=lambda K: abs(K - S))
                near1, near2 = set(ks[:2 * L1_STRIKES + 1]), set(ks[:2 * BOOK_STRIKES + 1])
                for K, cp, sym in rows:
                    if K in near1: l1.append(sym)
                    if K in near2: ob.append(sym)
        return tuple(sorted(l1)), tuple(sorted(ob))

    # ----- books
    @staticmethod
    def _levels(arr):
        out = []
        for lv in arr or []:
            try: out.append([float(lv.get("0")), int(lv.get("1") or 0), int(lv.get("2") or 0)])
            except (TypeError, ValueError): pass
        return out

    def set_book(self, service, item):
        key = item.get("key")
        rec = {"t": item.get("1"), "recv": time.time(), "bids": self._levels(item.get("2")), "asks": self._levels(item.get("3"))}
        with self.lock:
            if service == "OPTIONS_BOOK": self.obooks[key] = rec
            else: self.books[(service, key)] = rec

    def best_book(self, u):
        best = None
        for svc in EQUITY_BOOKS:
            b = self.books.get((svc, u))
            if b and (b["bids"] or b["asks"]) and (best is None or b["recv"] > best[1]["recv"]):
                best = (svc, b)
        if best: best[1]["service"] = best[0]
        return best[1] if best else None

    @staticmethod
    def _book_stats(b):
        bids, asks = b["bids"], b["asks"]
        if not bids or not asks: return None
        bb, ba = bids[0][0], asks[0][0]; mid = (bb + ba) / 2
        within = {}
        for d in WITHIN:
            B = sum(s for p, s, _ in bids if mid - p <= d + 1e-9)
            A = sum(s for p, s, _ in asks if p - mid <= d + 1e-9)
            within["%.2f" % d] = [B, A]
        imb = lambda d: (lambda B, A: round((B - A) / (B + A), 3) if B + A else None)(*within["%.2f" % d])
        return mid, within, imb(0.25), imb(1.00)

    def sample(self):
        now = time.time()
        with self.lock:
            for u in UNDERLYINGS:
                b = self.best_book(u)
                if not b or now - b["recv"] > 10: continue
                st = self._book_stats(b)
                if not st or st[2] is None: continue
                self.imb[u].append((now, st[2], st[3]))
                self.imb[u] = [x for x in self.imb[u] if now - x[0] <= 300]

    def depth_summary(self):
        now = time.time(); out = {}
        with self.lock:
            for u in UNDERLYINGS:
                b = self.best_book(u)
                if not b: out[u] = None; continue
                st = self._book_stats(b)
                d = {"source": b.get("service"), "bookAgeS": round(now - b["recv"], 1),
                     "bids": b["bids"][:8], "asks": b["asks"][:8]}
                if st:
                    mid, within, i25, i100 = st
                    avg = lambda n, j: (lambda xs: round(sum(xs) / len(xs), 3) if xs else None)([x[j] for x in self.imb[u] if now - x[0] <= n and x[j] is not None])
                    near = lambda side, sgn: sorted(([p, s] for p, s, _ in side if abs(p - mid) <= 2), key=lambda x: -x[1])[:3]
                    d.update({"bid": b["bids"][0][0], "ask": b["asks"][0][0], "mid": round(mid, 3),
                              "sizeWithin": within, "imbalance0.25": i25, "imbalance1.00": i100,
                              "imb0.25_avg60s": avg(60, 1), "imb0.25_avg5m": avg(300, 1),
                              "imb1.00_avg60s": avg(60, 2), "imb1.00_avg5m": avg(300, 2),
                              "biggestBids2": near(b["bids"], -1), "biggestAsks2": near(b["asks"], 1)})
                opts = {}
                for sym, ob in self.obooks.items():
                    m = self.meta.get(sym)
                    if not m or m[0] != u or now - ob["recv"] > 120: continue
                    bd, ak = ob["bids"], ob["asks"]
                    opts[strike_label(m[2], m[1])] = [bd[0][0] if bd else None, bd[0][1] if bd else 0,
                                                      ak[0][0] if ak else None, ak[0][1] if ak else 0,
                                                      sum(x[1] for x in bd[:3]), sum(x[1] for x in ak[:3])]
                d["options0dte"] = dict(sorted(opts.items()))
                out[u] = d
        return {"asOfET": et_label(),
                "note": "Displayed orders on Schwab's book feeds only (no hidden orders). Sizes as Schwab sends them. "
                        "imbalance = (bid size - ask size) / total within the distance; +1 all bids, -1 all asks. "
                        "options0dte: strike -> [bid, bidSize, ask, askSize, bidSizeTop3, askSizeTop3].",
                "tickers": out}

    def save_depth(self):
        write_atomic(DEPTH, self.depth_summary())

    # ----- level-one options -> tape
    def l1_option(self, item):
        key = item.get("key")
        with self.lock:
            q = self.q.setdefault(key, {})
            pbid, pask = q.get("bid"), q.get("ask")     # quote in force before this update
            for f, n in (("2", "bid"), ("3", "ask"), ("4", "last"), ("8", "vol")):
                if f in item: q[n] = item[f]
            m = self.meta.get(key)
        if "8" in item and m:
            bid = pbid if pbid is not None else q.get("bid")
            ask = pask if pask is not None else q.get("ask")
            self.tape.update(m[0], key, m[1], m[2], z.num(q.get("vol")), z.num(q.get("last")),
                             z.num(bid), z.num(ask), "stream")


# ---------------------------------------------------------------- tape poll (Market Data only)
def next_expiry_rows(sym, tok):
    start = now_et().date()
    ch = z.api_get("/marketdata/v1/chains", {"symbol": sym, "contractType": "ALL", "strikeCount": 80,
                   "includeUnderlyingQuote": "true", "strategy": "SINGLE",
                   "fromDate": start.isoformat(), "toDate": (start + timedelta(days=6)).isoformat()}, tok)
    now_ms = int(time.time() * 1000); rows = []
    for cp, key in (("C", "callExpDateMap"), ("P", "putExpDateMap")):
        for ek, strikes in (ch.get(key) or {}).items():
            ex = ek.split(":")[0]
            if z.expiry_ms(ex) <= now_ms: continue
            for k, arr in strikes.items():
                for c in arr: rows.append((ex, cp, float(k), c))
    if not rows: return None, None, []
    e0 = min(r[0] for r in rows)
    und = ch.get("underlying") or {}
    S = z.num(und.get("last")) or z.num(ch.get("underlyingPrice"))
    return e0, S, [r for r in rows if r[0] == e0]

def poll_once(hub):
    tok = access_token(); n = 0; exps = {}
    for u in UNDERLYINGS:
        e0, S, rows = next_expiry_rows(u, tok)
        exps[u] = e0
        good = [(K, cp, c) for _, cp, K, c in rows if c.get("symbol")]
        hub.set_contracts(u, S, [(K, cp, c["symbol"]) for K, cp, c in good])
        for K, cp, c in good:
            sym = c["symbol"]
            if hub.tape.recently_streamed(sym): continue
            hub.tape.update(u, sym, cp, K, z.num(c.get("totalVolume")), z.num(c.get("last")),
                            z.num(c.get("bid")), z.num(c.get("ask")), "poll")
            n += 1
    if not hub.tape.save():
        write_atomic(TAPE, hub.tape.summary())
    return n, exps

def tape_poll_loop(hub, stop):
    first = True
    while not stop.is_set():
        if first or in_window(9 * 60 + 28, 16 * 60 + 2):
            try:
                n, exps = poll_once(hub)
                hub.set_status(tapePoll={"lastET": et_label(), "ok": True, "contractsPolled": n, "expiry": exps})
            except SystemExit as e:
                hub.set_status(tapePoll={"lastET": et_label(), "ok": False, "error": str(e.code)})
            except urllib.error.HTTPError as e:
                hub.set_status(tapePoll={"lastET": et_label(), "ok": False, "error": "chain_http_%d" % e.code})
            except Exception as e:
                hub.set_status(tapePoll={"lastET": et_label(), "ok": False, "error": "%s: %s" % (type(e).__name__, e)})
            first = False
        stop.wait(60)


# ---------------------------------------------------------------- minimal websocket client (RFC 6455)
class WS:
    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, url, timeout=45):
        u = urllib.parse.urlparse(url)
        tls = u.scheme == "wss"; host = u.hostname; port = u.port or (443 if tls else 80)
        path = (u.path or "/") + ("?" + u.query if u.query else "")
        s = socket.create_connection((host, port), timeout=timeout)
        if tls:
            s = ssl.create_default_context().wrap_socket(s, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode()
        s.sendall(("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                   "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n" % (path, host, key)).encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            ch = s.recv(4096)
            if not ch: raise ConnectionError("websocket handshake: connection closed")
            buf += ch
            if len(buf) > 65536: raise ConnectionError("websocket handshake: reply too long")
        head, _, rest = buf.partition(b"\r\n\r\n")
        first = head.split(b"\r\n", 1)[0]
        if b" 101" not in first:
            raise ConnectionError("websocket handshake refused: " + first.decode(errors="replace"))
        expect = base64.b64encode(hashlib.sha1((key + self.GUID).encode()).digest())
        if expect not in head:
            raise ConnectionError("websocket handshake: bad accept key")
        self.s = s; self.buf = bytearray(rest); self.wlock = threading.Lock()

    def _read(self, n):
        while len(self.buf) < n:
            ch = self.s.recv(65536)
            if not ch: raise ConnectionError("websocket closed by server")
            self.buf += ch
        out = bytes(self.buf[:n]); del self.buf[:n]
        return out

    def send_frame(self, opcode, payload):
        n = len(payload); hdr = bytearray([0x80 | opcode])
        if n < 126: hdr.append(0x80 | n)
        elif n < 65536: hdr.append(0x80 | 126); hdr += struct.pack("!H", n)
        else: hdr.append(0x80 | 127); hdr += struct.pack("!Q", n)
        mask = os.urandom(4); hdr += mask
        body = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        with self.wlock:
            self.s.sendall(bytes(hdr) + body)

    def send(self, text):
        self.send_frame(1, text.encode("utf-8"))

    def recv(self):
        parts = []
        while True:
            b1, b2 = self._read(2)
            fin, op, n = b1 & 0x80, b1 & 0x0F, b2 & 0x7F
            if n == 126: n = struct.unpack("!H", self._read(2))[0]
            elif n == 127: n = struct.unpack("!Q", self._read(8))[0]
            mk = self._read(4) if b2 & 0x80 else None
            data = self._read(n)
            if mk: data = bytes(b ^ mk[i & 3] for i, b in enumerate(data))
            if op == 9: self.send_frame(10, data); continue
            if op == 10: continue
            if op == 8:
                code = struct.unpack("!H", data[:2])[0] if len(data) >= 2 else None
                raise ConnectionError("websocket closed by server (code %s)" % code)
            if op in (1, 2): parts = [data]
            elif op == 0: parts.append(data)
            if fin: return b"".join(parts).decode("utf-8", errors="replace")

    def close(self):
        try: self.send_frame(8, b"")
        except Exception: pass
        try: self.s.close()
        except Exception: pass


# ---------------------------------------------------------------- streamer
class NeedsPermission(Exception):
    pass

class Streamer:
    STREAM_WINDOW = (9 * 60 + 25, 16 * 60 + 5)

    def __init__(self, hub, stop, test=False):
        self.hub, self.stop, self.test = hub, stop, test
        self.ws = None; self.info = None; self.rid = 0
        self.cur = {}

    def kill(self):
        self.stop.set()
        if self.ws: self.ws.close()

    def _req(self, service, command, params):
        self.rid += 1
        return {"service": service, "command": command, "requestid": str(self.rid),
                "SchwabClientCustomerId": self.info["schwabClientCustomerId"],
                "SchwabClientCorrelId": self.info["schwabClientCorrelId"], "parameters": params}

    def _send(self, *reqs):
        self.ws.send(json.dumps({"requests": list(reqs)}))

    def _streamer_info(self):
        tok = access_token()
        try:
            prefs = z.api_get("/trader/v1/userPreference", {}, tok)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise NeedsPermission("needs 'Accounts and Trading Production' on the Schwab app "
                                      "(userPreference returned HTTP %d); tape poll still runs" % e.code)
            raise
        if isinstance(prefs, list): prefs = prefs[0] if prefs else {}
        si = (prefs.get("streamerInfo") or [None])[0]
        if not si: raise NeedsPermission("userPreference returned no streamerInfo")
        keep = ("streamerSocketUrl", "schwabClientCustomerId", "schwabClientCorrelId",
                "schwabClientChannel", "schwabClientFunctionId")
        return {k: si.get(k) for k in keep}, tok    # held in memory only

    def probe(self):
        try:
            self._streamer_info()
            self.hub.set_status(stream="ready (streams %d:%02d-%d:%02d ET on weekdays)" % (
                self.STREAM_WINDOW[0] // 60, self.STREAM_WINDOW[0] % 60, self.STREAM_WINDOW[1] // 60, self.STREAM_WINDOW[1] % 60))
        except NeedsPermission as e:
            self.hub.set_status(stream="off: " + str(e))
        except SystemExit as e:
            self.hub.set_status(stream="off: login problem " + str(e.code))
        except Exception as e:
            self.hub.set_status(stream="probe error: %s: %s" % (type(e).__name__, e))

    def run(self):
        backoff = 5; probed = False
        while not self.stop.is_set():
            if not self.test and not in_window(*self.STREAM_WINDOW):
                if not probed: self.probe(); probed = True
                self.stop.wait(60); continue
            try:
                self.session(); backoff = 5
                if self.test: return
            except NeedsPermission as e:
                self.hub.set_status(stream="off: " + str(e))
                if self.test: return
                self.stop.wait(1800)
            except SystemExit as e:
                self.hub.set_status(stream="off: login problem " + str(e.code))
                if self.test: return
                self.stop.wait(600)
            except Exception as e:
                if self.stop.is_set(): break
                self.hub.set_status(stream="reconnecting after %s: %s" % (type(e).__name__, e))
                if self.test: return
                self.stop.wait(backoff); backoff = min(backoff * 2, 120)
            finally:
                if self.ws: self.ws.close(); self.ws = None
                self.hub.ws = None
        self.hub.set_status(stream="stopped")

    def session(self, test_seconds=30):
        self.info, tok = self._streamer_info()
        self.ws = WS(self.info["streamerSocketUrl"]); self.hub.ws = self.ws; self.cur = {}
        self._send(self._req("ADMIN", "LOGIN", {"Authorization": tok,
                                                "SchwabClientChannel": self.info["schwabClientChannel"],
                                                "SchwabClientFunctionId": self.info["schwabClientFunctionId"]}))
        deadline = time.time() + 20
        while True:
            msg = json.loads(self.ws.recv())
            done = False
            for r in msg.get("response", []):
                if r.get("service") == "ADMIN" and r.get("command") == "LOGIN":
                    c = r.get("content") or {}
                    if c.get("code") != 0:
                        raise ConnectionError("streamer login refused: code %s %s" % (c.get("code"), c.get("msg")))
                    done = True
            if done: break
            if time.time() > deadline: raise TimeoutError("no streamer login reply in 20 s")
        self.hub.set_status(stream="connected", services={}, connectedET=et_label())
        self.subscribe(force=True)
        t0 = last_sample = last_flush = last_resub = time.time()
        while not self.stop.is_set():
            if self.test and time.time() - t0 > test_seconds: break
            if not self.test and not in_window(*self.STREAM_WINDOW): break
            try:
                raw = self.ws.recv()
            except socket.timeout:
                raise TimeoutError("no data from the streamer for 45 s")
            self.handle(json.loads(raw))
            now = time.time()
            if now - last_sample >= 1: self.hub.sample(); last_sample = now
            if now - last_flush >= 5:
                self.hub.save_depth(); self.hub.tape.save()
                self.hub.set_status(lastMsgET=et_label()); last_flush = now
            if now - last_resub >= 60: self.subscribe(); last_resub = now
        try: self._send(self._req("ADMIN", "LOGOUT", {}))
        except Exception: pass
        self.hub.save_depth(); self.hub.tape.save()

    def subscribe(self, force=False):
        reqs = []
        if force:
            for svc in EQUITY_BOOKS:
                reqs.append(self._req(svc, "SUBS", {"keys": ",".join(UNDERLYINGS), "fields": "0,1,2,3"}))
        l1, ob = self.hub.option_keys()
        for svc, keys, fields in (("LEVELONE_OPTIONS", l1, "0,2,3,4,8,16,17,18"), ("OPTIONS_BOOK", ob, "0,1,2,3")):
            if keys and keys != self.cur.get(svc):
                reqs.append(self._req(svc, "SUBS", {"keys": ",".join(keys), "fields": fields}))
                self.cur[svc] = keys
        if reqs: self._send(*reqs)

    def handle(self, msg):
        for r in msg.get("response", []):
            c = r.get("content") or {}
            if r.get("service") != "ADMIN":
                with self.hub.lock:
                    self.hub.status["services"][r.get("service")] = {"code": c.get("code"), "msg": c.get("msg")}
        for n in msg.get("notify", []):
            c = n.get("content") or {}
            if n.get("service") == "ADMIN" and c.get("code") not in (None, 0):
                raise ConnectionError("streamer notice code %s: %s" % (c.get("code"), c.get("msg")))
        for d in msg.get("data", []):
            svc = d.get("service")
            for item in d.get("content") or []:
                if svc in EQUITY_BOOKS or svc == "OPTIONS_BOOK": self.hub.set_book(svc, item)
                elif svc == "LEVELONE_OPTIONS": self.hub.l1_option(item)


# ---------------------------------------------------------------- start / stop (called from zdte_schwab)
def start():
    stop = threading.Event(); hub = Hub(); st = Streamer(hub, stop)
    t1 = threading.Thread(target=tape_poll_loop, args=(hub, stop), daemon=True, name="zdte-tape")
    t2 = threading.Thread(target=st.run, daemon=True, name="zdte-stream")
    t1.start(); t2.start()
    return {"stop": stop, "kill": st.kill, "threads": [t1, t2], "mtime": Path(__file__).stat().st_mtime, "hub": hub}


if __name__ == "__main__":
    if "--poll" in sys.argv:
        h = Hub(); n, exps = poll_once(h)
        print("polled %d contracts, expiry %s" % (n, exps))
        print(TAPE.read_text(encoding="utf-8"))
    elif "--test" in sys.argv:
        h = Hub(); poll_once(h)
        ev = threading.Event(); s = Streamer(h, ev, test=True)
        print("Connecting to the Schwab streamer for about 30 seconds...")
        s.run()
        print(json.dumps(h.status, indent=1))
        print(json.dumps(h.depth_summary(), indent=1)[:4000])
    else:
        print(__doc__)
