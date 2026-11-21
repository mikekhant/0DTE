"""
schwab_auth.py - log in to the Schwab Trader API once (repeat every 7 days).

1. Put your Client ID and Client Secret in schwab_config.json (same folder).
2. Run:  python schwab_auth.py
3. A browser window opens at Schwab. Log in and approve access.
4. Schwab sends you to https://127.0.0.1/?code=... and the page says it can't be reached.
   That's expected. Copy the WHOLE address from the address bar and paste it here.
   Do it within about 30 seconds: the code expires quickly. If it fails, just run this again.
"""
import json, sys, time, urllib.parse, urllib.error, webbrowser
from zdte_schwab import load_config, token_request, save_tokens, api_get, API

def main():
    cfg = load_config()
    url = API + "/v1/oauth/authorize?" + urllib.parse.urlencode({"client_id": cfg["app_key"], "redirect_uri": cfg["callback_url"]})
    print("\nOpening Schwab login in your browser. If it doesn't open, copy this link:\n\n" + url + "\n")
    webbrowser.open(url)
    returned = input("After approving, paste the full https://127.0.0.1/?code=... address here:\n> ").strip()
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(returned).query)
    code = (qs.get("code") or [None])[0]
    if not code:
        sys.exit("No code found in that address. Run schwab_auth.py again and paste the whole address.")
    try:
        tok = token_request(cfg, {"grant_type": "authorization_code", "code": code, "redirect_uri": cfg["callback_url"]})
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        sys.exit(f"Schwab refused the login ({e.code}): {body}\nUsual causes: the code expired (paste faster), "
                 "the callback URL in schwab_config.json doesn't match the app exactly, or the app isn't 'Ready to use' yet.")
    t = save_tokens(tok)
    print("\nLogged in. tokens.json saved. Login is good for 7 days (until %s)." %
          time.strftime("%a %b %d %H:%M", time.localtime(t["refresh_obtained_at"] + 7 * 24 * 3600)))
    try:
        q = api_get("/marketdata/v1/quotes", {"symbols": "QQQ,SPY", "fields": "quote"}, t["access_token"])
        for s in ("QQQ", "SPY"):
            d = q.get(s, {})
            print(f"  {s}: last {d.get('quote', {}).get('lastPrice')}  realtime={d.get('realtime')}")
    except urllib.error.HTTPError as e:
        print(f"Login worked, but the test quote failed ({e.code}). Check that the app has Market Data Production.")

if __name__ == "__main__":
    main()
