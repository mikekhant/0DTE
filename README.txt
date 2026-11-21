SCHWAB LIVE DATA FOR /0dte  -  one-time setup (Windows)

1. FOLDER
   Create  Documents\trading\schwab  and save these four files in it:
   schwab_config.json, schwab_auth.py, zdte_schwab.py, README.txt

2. PYTHON
   Open Command Prompt and type:   python --version
   If it says "not recognized" or opens the Microsoft Store, install Python from
   https://www.python.org/downloads/  and tick "Add python.exe to PATH" in the installer.
   Nothing else to install: the scripts use only Python's standard library.

3. YOUR CLIENT ID AND SECRET  (this is the only place they go)
   Right-click schwab_config.json > Open with > Notepad.
   Replace PASTE_CLIENT_ID_HERE with your App Key / Client ID
   and PASTE_CLIENT_SECRET_HERE with your Secret. Keep the quote marks.
   The callback must match your Schwab app exactly: https://127.0.0.1
   Save. Don't paste these values into chat, email or anywhere else.

4. LOG IN (now, then once every 7 days)
   In Command Prompt:
      cd %USERPROFILE%\Documents\trading\schwab
      python schwab_auth.py
   Log in at Schwab in the browser that opens and approve. You land on a
   "can't reach 127.0.0.1" page - that's expected. Copy the whole address from the
   address bar and paste it into Command Prompt within ~30 seconds.
   It prints QQQ and SPY prices and whether they're real-time.

5. TEST
      python zdte_schwab.py --check     (shows hours left on the login)
      python zdte_schwab.py             (prints the full data as one line of JSON)

tokens.json (created in step 4) holds your login tokens. Treat it like a password.
If a run says "login_expired" or "refresh_failed", repeat step 4.

6. ORDER BOOK AND TAPE (added Nov 20, 2026; nothing to install or launch)
   The feed now also runs zdte_stream.py in the background on weekdays:
   - Tape estimate (works with Market Data Production alone): every 60 s from 9:28 to
     16:02 ET it pulls the 0DTE chains and sorts each contract's new volume into
     "bought" (traded at or near the ask), "sold" (near the bid) or "mid". It's an
     estimate from the last trade price, not true trade-by-trade data.
   - Level 2 depth and a per-trade tape (needs the streaming API, 9:25-16:05 ET):
     QQQ/SPY order books, books for the near-the-money 0DTE options, and option
     quotes that classify each trade as it happens.
     The streaming address comes from Schwab's "user preference" call, which is part of
     the "Accounts and Trading Production" API product. To turn streaming on:
       a) developer.schwab.com > Dashboard > your app > Modify, add
          "Accounts and Trading Production" next to Market Data Production, and save.
          Wait until the app shows "Ready to use" again (Schwab may review the change).
       b) Run  python schwab_auth.py  again and approve; tick your account if asked.
     The scripts only call that one endpoint for the streamer address. They never read
     balances, positions or orders, never place orders, and never save account numbers.
   Files it writes here: tape.json, depth.json, stream_status.json, tape_state.json.
   stream_status.json says whether streaming is connected, or why it's off.
   Manual checks:  python zdte_stream.py --poll    (one tape poll)
                   python zdte_stream.py --test    (30 s of streaming, any time)
