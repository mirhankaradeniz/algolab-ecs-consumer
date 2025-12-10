import os, json, time, hashlib, boto3, datetime
from urllib.parse import urlparse
from boto3.dynamodb.conditions import Key
import websocket  # websocket-client

API_HOSTNAME   = os.getenv("API_HOSTNAME")                     # e.g. https://www.algolab.com.tr
WSS_ENDPOINT   = os.getenv("WSS_ENDPOINT")                     # e.g. wss://www.algolab.com.tr/api/ws
DDB_LOGIN      = os.getenv("DDB_LOGIN_TABLE", "algolabLoginData")
DDB_FEED       = os.getenv("DDB_FEED_TABLE",  "algolabWsDFeed")
ACCOUNT_NAME   = os.getenv("ACCOUNT_NAME")                     # e.g. 46738329204
SYMBOLS        = os.getenv("SYMBOLS", "ALL")
AWS_REGION     = os.getenv("AWS_REGION", "eu-west-3")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
login_tbl = dynamodb.Table(DDB_LOGIN)
feed_tbl  = dynamodb.Table(DDB_FEED)

def make_checker(api_key, api_hostname, endpoint, body=""):
    data = api_key + api_hostname + endpoint + body
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def load_login_settings(account_name: str):
    """Read token, hash (inside value map) + apiKey (top-level)"""
    ddb = boto3.client("dynamodb", region_name=os.environ.get("AWS_REGION","eu-west-3"))
    resp = ddb.get_item(
        TableName=os.environ.get("DDB_LOGIN_TABLE","algolabLoginData"),
        Key={"accountName": {"S": account_name}, "keyName": {"S": "settings"}},
        ConsistentRead=True,
    )
    item = resp.get("Item", {})
    v = item.get("value", {}).get("M", {})
    token = v.get("token", {}).get("S")
    auth_hash = v.get("hash", {}).get("S")
    api_key = item.get("apiKey", {}).get("S") or os.getenv("API_KEY")

    if not token or not auth_hash:
        raise RuntimeError(f"[BOOT] Login settings incomplete for {account_name}: missing token/hash")
    if not api_key:
        raise RuntimeError(f"[BOOT] Missing apiKey for {account_name} (ne DDB'de ne de env'de var)")
    print(f"[BOOT] DDB fetched: token={'OK' if token else 'NO'}, hash={'OK' if auth_hash else 'NO'}, apiKey={'OK' if api_key else 'NO'}")
    return token, auth_hash, api_key

def to_user_friendly(raw_msg: dict):
    s   = raw_msg.get("S") or raw_msg.get("symbol")
    lp  = raw_msg.get("L") or raw_msg.get("lastPrice")
    bid = raw_msg.get("B") or raw_msg.get("bid")
    ask = raw_msg.get("A") or raw_msg.get("ask")
    vol = raw_msg.get("V") or raw_msg.get("volume")
    ts  = datetime.datetime.utcnow().isoformat()
    return {
        "accountName": ACCOUNT_NAME,
        "ts": ts,
        "symbol": s,
        "lastPrice": lp,
        "bid": bid,
        "ask": ask,
        "volume": vol,
        "raw": raw_msg
    }

def send_json(ws, obj):
    ws.send(json.dumps(obj, separators=(",", ":")))

def on_open(ws, auth_hash):
    print("[WS] Connected. Sending Heartbeat & D-Subscribe")
    # API expects lowercase 'token'
    symbols = [s.strip() for s in SYMBOLS.split(",")] if SYMBOLS != "ALL" else ["ALL"]
    send_json(ws, {"Type": "H", "token": auth_hash})
    send_json(ws, {"Type": "D", "token": auth_hash, "Symbols": symbols})

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception:
        print(f"[WS] non-json message: {message}")
        return
    try:
        item = to_user_friendly(data)
        feed_tbl.put_item(Item=item)
    except Exception as e:
        print(f"[WS] put_item error: {e}, msg={data}")

def on_error(ws, error):
    print(f"[WS] Error: {error}")

def on_close(ws, a, b):
    print("[WS] Closed")

def run():
    if not ACCOUNT_NAME:
        raise RuntimeError("[BOOT] ACCOUNT_NAME empty. Set it in task env for a single-account test.")

    # 1) Pull creds from DDB
    token, auth_hash, api_key = load_login_settings(ACCOUNT_NAME)

    # 2) Compute Checker with correct path from WSS_ENDPOINT
    if not API_HOSTNAME:
        raise RuntimeError("[BOOT] API_HOSTNAME not set (e.g. https://www.algolab.com.tr)")
    if not WSS_ENDPOINT:
        raise RuntimeError("[BOOT] WSS_ENDPOINT not set (e.g. wss://www.algolab.com.tr/api/ws)")

    parsed = urlparse(WSS_ENDPOINT)
    endpoint_path = parsed.path or "/api/ws"   # /api/ws is your server path
    checker  = make_checker(api_key, API_HOSTNAME, endpoint_path, "")
    headers = [
        f"APIKEY: {api_key}",
        f"Authorization: {auth_hash}",
        f"Checker: {checker}"
    ]
    print(f"[BOOT] endpoint_path={endpoint_path}, headers=APIKEY/Authorization/Checker set")

    ws = websocket.WebSocketApp(
        WSS_ENDPOINT,
        header=headers,
        on_open=lambda w: on_open(w, auth_hash),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # 3) Reconnect loop
    print(f"[BOOT] starting WS loop for account={ACCOUNT_NAME}, symbols={SYMBOLS}")
    while True:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[WS] run_forever exception: {e}")
        time.sleep(3)

if __name__ == "__main__":
    print("[BOOT] starting…")
    run()
