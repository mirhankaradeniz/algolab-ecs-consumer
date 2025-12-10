# consumer.py
import os, json, time, hashlib, datetime, threading, traceback
import boto3
from boto3.dynamodb.conditions import Attr
import websocket  # websocket-client

API_HOSTNAME = os.getenv("API_HOSTNAME")                 # e.g. https://www.algolab.com.tr
WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")                 # e.g. wss://www.algolab.com.tr/api/ws
DDB_LOGIN    = os.getenv("DDB_LOGIN_TABLE", "algolabLoginData")
DDB_FEED     = os.getenv("DDB_FEED_TABLE",  "algolabWsDFeed")
AWS_REGION   = os.getenv("AWS_REGION", "eu-west-3")
SYMBOLS      = os.getenv("SYMBOLS", "ALL")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
login_tbl = dynamodb.Table(DDB_LOGIN)
feed_tbl  = dynamodb.Table(DDB_FEED)

def log(msg):  # short, consistent CLoudWatch lines
    print(msg, flush=True)

def make_checker(api_key, api_hostname, endpoint, body=""):
    data = (api_key or "") + (api_hostname or "") + (endpoint or "") + (body or "")
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def load_login_settings(account_name: str):
    """Read settings row for an account; returns (token, hash, api_key)."""
    resp = login_tbl.get_item(
        Key={"accountName": account_name, "keyName": "settings"},
        ConsistentRead=True,
    )
    item = resp.get("Item") or {}
    v = (item.get("value") or {})  # map from the backend response
    token = (v.get("token") if isinstance(v, dict) else None) or (v.get("M", {}).get("token", {}).get("S"))
    auth_hash = (v.get("hash") if isinstance(v, dict) else None) or (v.get("M", {}).get("hash", {}).get("S"))
    api_key = item.get("apiKey") or (item.get("apiKey", {}).get("S") if isinstance(item.get("apiKey"), dict) else None)

    # Fallback to container env if you ever want that behavior:
    if not api_key:
        api_key = os.getenv("API_KEY")

    if not token or not auth_hash or not api_key:
        raise RuntimeError(f"incomplete login for {account_name} (token|hash|apiKey missing)")
    return token, auth_hash, api_key

def list_enabled_accounts():
    """Scan for accounts with enabled=true on the settings row."""
    # NOTE: scan is fine at small scale; move to a GSI if the table grows.
    filt = Attr("enabled").eq(True) & Attr("keyName").eq("settings")
    accounts = []
    last_key = None
    while True:
        resp = login_tbl.scan(
            FilterExpression=filt,
            ProjectionExpression="accountName,keyName,enabled"
        ) if last_key is None else login_tbl.scan(
            FilterExpression=filt,
            ProjectionExpression="accountName,keyName,enabled",
            ExclusiveStartKey=last_key
        )
        for it in resp.get("Items", []):
            acc = it.get("accountName")
            if isinstance(acc, dict):  # in case low-level form appears
                acc = acc.get("S")
            if acc:
                accounts.append(acc)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return sorted(set(accounts))

def to_feed_item(account_name, raw_msg: dict):
    # Adjust mapping to your provider's payload. Keeping it tolerant:
    s   = raw_msg.get("S") or raw_msg.get("symbol")
    lp  = raw_msg.get("L") or raw_msg.get("lastPrice")
    bid = raw_msg.get("B") or raw_msg.get("bid")
    ask = raw_msg.get("A") or raw_msg.get("ask")
    vol = raw_msg.get("V") or raw_msg.get("volume")
    ts  = datetime.datetime.utcnow().isoformat()

    item = {
        "accountName": account_name,
        "ts": ts,
        "symbol": s,
        "lastPrice": lp,
        "bid": bid,
        "ask": ask,
        "volume": vol,
        "raw": raw_msg
    }
    # Remove None fields so DDB isn't unhappy with empty attrs
    return {k: v for k, v in item.items() if v is not None}

def ws_loop_for_account(account_name: str, stop_flag: threading.Event):
    """One persistent WS loop per account; reconnects on failure."""
    log(f"[BOOT] started worker for {account_name}")
    while not stop_flag.is_set():
        try:
            # 1) credentials
            token, auth_hash, api_key = load_login_settings(account_name)

            # 2) headers (Checker for /ws + empty body)
            endpoint = "/ws"
            checker  = make_checker(api_key, API_HOSTNAME, endpoint, "")
            headers = [
                f"APIKEY: {api_key}",
                f"Authorization: {auth_hash}",
                f"Checker: {checker}"
            ]

            # 3) connect
            ws = websocket.WebSocketApp(
                WSS_ENDPOINT,
                header=headers,
                on_message=lambda _ws, message: on_message(account_name, message),
                on_error=lambda _ws, err: log(f"[WS][{account_name}] Error: {err}"),
                on_close=lambda _ws, a, b: log(f"[WS][{account_name}] Closed"),
                on_open=lambda _ws: on_open(_ws, auth_hash)
            )

            log(f"[WS][{account_name}] connecting...")
            ws.run_forever(ping_interval=30, ping_timeout=10)
            # if we’re here, connection closed; wait a bit then retry
            if not stop_flag.is_set():
                time.sleep(3)
        except Exception as e:
            log(f"[WS][{account_name}] loop exception: {e}\n{traceback.format_exc()}")
            time.sleep(5)

def send_json(ws, obj):  # compact JSON
    ws.send(json.dumps(obj, separators=(",", ":")))

def on_open(ws, auth_hash):
    log("[WS] Connected. Sending Heartbeat & D-Subscribe")
    send_json(ws, {"Type": "H", "Token": auth_hash})
    symbols = [s.strip() for s in SYMBOLS.split(",")] if SYMBOLS != "ALL" else ["ALL"]
    send_json(ws, {"Type": "D", "Token": auth_hash, "Symbols": symbols})

def on_message(account_name, message):
    try:
        data = json.loads(message)
    except Exception:
        log(f"[WS][{account_name}] non-json: {message[:200]}")
        return
    try:
        item = to_feed_item(account_name, data)
        feed_tbl.put_item(Item=item)
    except Exception as e:
        log(f"[WS][{account_name}] put_item error: {e} msg={str(data)[:200]}")

def main():
    log("[BOOT] Dynamic mode. Polling enabled users...")
    workers = {}  # account_name -> (thread, stop_event)

    while True:
        try:
            enabled_accounts = list_enabled_accounts()
            # start workers for new accounts
            for acc in enabled_accounts:
                if acc not in workers:
                    stop_ev = threading.Event()
                    t = threading.Thread(target=ws_loop_for_account, args=(acc, stop_ev), daemon=True)
                    workers[acc] = (t, stop_ev)
                    t.start()
            # (optional) stop workers whose accounts got disabled later
            for acc in list(workers.keys()):
                if acc not in enabled_accounts:
                    log(f"[BOOT] stopping worker for {acc} (disabled)")
                    t, ev = workers.pop(acc)
                    ev.set()
            time.sleep(POLL_SECONDS)
        except Exception as e:
            log(f"[BOOT] poll exception: {e}\n{traceback.format_exc()}")
            time.sleep(10)

if __name__ == "__main__":
    main()
