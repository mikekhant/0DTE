"""
index_tool.py - edit the 0DTE gamma-map index (claude/0dte-charts.md) in place, without retyping it.

  python3 index_tool.py INDEX.md row DATE [--artifact URL] [--snap "9:39"] [--updated "2026-11-06 09:41 ET"]
                                          [--overnight "text"] [--closed]
  python3 index_tool.py INDEX.md note "- 2026-11-06 open: ..."
  python3 index_tool.py INDEX.md show DATE

`row` creates the date's row if missing (kept in date order) and updates the given columns.
--snap appends to the snapshots column; --closed adds "(bars to 16:00)" once.
Upload the result with Projects project_write (local_path) so the doc never passes through the chat.
"""
import sys, re

def load(p): return open(p, encoding="utf-8").read().split("\n")

def find_table(lines):
    hdr = next(i for i, l in enumerate(lines) if l.startswith("| date |"))
    end = hdr + 2
    while end < len(lines) and lines[end].startswith("| "): end += 1
    return hdr + 2, end

def cells(l): return [c.strip() for c in l.strip().strip("|").split("|")]

def fmt(c): return "| " + " | ".join(c) + " |"

def main(a):
    p, cmd = a[0], a[1]
    L = load(p)
    if cmd == "note":
        L = L[:-1] + [a[2], ""] if L and L[-1] == "" else L + [a[2]]
        open(p, "w", encoding="utf-8").write("\n".join(L)); return
    date = a[2]; s, e = find_table(L)
    idx = next((i for i in range(s, e) if cells(L[i])[0] == date), None)
    if cmd == "show":
        print(L[idx] if idx is not None else "no row"); return
    if idx is None:
        row = [date, "—", "—", "—", "—"]
        pos = next((i for i in range(s, e) if cells(L[i])[0] > date), e)
        L.insert(pos, fmt(row)); idx = pos
    c = cells(L[idx]); c += ["—"] * (5 - len(c))
    i = 3
    while i < len(a):
        k = a[i]
        if k == "--artifact": c[1] = a[i + 1]; i += 2
        elif k == "--snap":
            snap = a[i + 1]
            if c[2] in ("—", ""): c[2] = snap
            else:
                tail = ""
                m = re.search(r"( \(bars to 16:00[^)]*\))$", c[2])
                if m: tail = m.group(1); c[2] = c[2][:m.start()]
                c[2] = c[2] + ", " + snap + tail
            i += 2
        elif k == "--updated": c[3] = a[i + 1]; i += 2
        elif k == "--overnight": c[4] = a[i + 1]; i += 2
        elif k == "--closed":
            if "bars to 16:00" not in c[2]: c[2] = c[2] + " (bars to 16:00)"
            i += 1
        else: raise SystemExit("unknown arg " + k)
    L[idx] = fmt(c)
    open(p, "w", encoding="utf-8").write("\n".join(L))
    print(L[idx])

if __name__ == "__main__":
    main(sys.argv[1:])
