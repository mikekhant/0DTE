"""
zdte_feed.pyw - keeps latest.json fresh for the /0dte briefing. Runs hidden (pythonw.exe).

- Refreshes right away whenever Claude drops a file named refresh.request in this folder
  (checked every 2 seconds), so /0dte gets live Schwab data on demand.
- Also refreshes in the background: every 5 minutes on weekdays 9:00-16:15 ET,
  every 30 minutes otherwise.
- Writes latest.json (same JSON as `python zdte_schwab.py`) and feed_status.json.
- Stop it with stop_feed.bat. Started at Windows logon by install_feed.bat.
Never touches anything except files in this folder.
"""
import importlib, json, os, socket, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import zdte_schwab as z

REQUEST = HERE / "refresh.request"
STOP = HERE / "stop.request"
LATEST = HERE / "latest.json"
STATUS = HERE / "feed_status.json"
PORT = 47613  # single-instance lock (localhost only)

def et_now():
    return z.to_et(int(time.time() * 1000))

def background_interval():
    t = et_now(); m = t.hour * 60 + t.minute
    return 300 if (t.weekday() < 5 and 9 * 60 <= m < 16 * 60 + 15) else 1800

def write_atomic(path, text):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)

def write_status(ok, err=None, trigger=None):
    write_atomic(STATUS, json.dumps({"lastRunET": et_now().strftime("%Y-%m-%d %H:%M:%S"), "ok": ok,
                                     "error": err, "trigger": trigger, "pid": os.getpid()}))

_mtime = [0.0]
def maybe_reload():
    """Pick up a new zdte_schwab.py without restarting the feed."""
    global z
    try:
        m = (HERE / "zdte_schwab.py").stat().st_mtime
        if _mtime[0] and m != _mtime[0]:
            z = importlib.reload(z)
        _mtime[0] = m
    except Exception:
        pass

def refresh(trigger):
    maybe_reload()
    try:
        out = z.run(["QQQ", "SPY"])
        out["feedTrigger"] = trigger
        write_atomic(LATEST, json.dumps(out, separators=(",", ":")))
        write_status(True, trigger=trigger)
    except SystemExit as e:  # login / config problems arrive as SystemExit('{"error": ...}')
        write_status(False, str(e.code), trigger)
    except Exception as e:
        write_status(False, "%s: %s" % (type(e).__name__, e), trigger)

def main():
    lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        lock.bind(("127.0.0.1", PORT))
    except OSError:
        return  # another copy is already running
    next_bg = 0.0
    while True:
        if STOP.exists():
            try: STOP.unlink()
            except OSError: pass
            write_status(True, "stopped", "stop")
            return
        if REQUEST.exists():
            try: REQUEST.unlink()
            except OSError: pass
            refresh("request")
            next_bg = time.time() + background_interval()
        elif time.time() >= next_bg:
            refresh("background")
            next_bg = time.time() + background_interval()
        time.sleep(2)

if __name__ == "__main__":
    if "--once" in sys.argv:
        refresh("manual")
    else:
        main()
