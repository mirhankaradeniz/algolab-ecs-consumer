import os, json, time, hashlib, boto3, datetime
from boto3.dynamodb.conditions import Key
import websocket  # websocket-client

API_HOSTNAME   = os.getenv("API_HOSTNAME")
WSS_ENDPOINT   = os.getenv("WSS_ENDPOINT")
DDB_LOGIN      = os.getenv("DDB_LOGIN_TABLE", "algolabLoginData")
DDB_FEED       = os.getenv("DDB_FEED_TABLE",  "algolabWsDFeed")
API_KEY        = os.getenv("API_KEY")          # e.g. "API-XXXX"
ACCOUNT_NAME   = os.getenv("ACCOUNT_NAME")     # user/TC
SYMBOLS        = os.getenv("SYMBOLS", "ALL")
AWS_REGION     = os.getenv("AWS_REGION", "eu-west-3")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
login_tbl = dynamodb.Table(DDB_LOGIN)
feed_tbl  = dynamodb.Table(DDB_FEED)

def make_checker(api_key, api_hostname, endpoint, body=""):
    data = api_key + api_hostname + endpoint + body
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def load_login_settings(account_name):
    resp = login_tbl.get_item(Key={"accountName": account_name, "keyName": "settings"})
    if "Item" not in resp:
        raise RuntimeError("Login settings not found in DynamoDB for accountName=" + str(account_name))
    v = resp["Item"]["value"]
    return v.get("token"), v.get("hash")

def to_user_friendly(raw_msg: dict):
    # Sağlayıcının D paketi alanlarını ihtiyacına göre uyarlayabilirsin
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
        "raw": raw_msg  # debug amaçlı
    }

def send_json(ws, obj):
    ws.send(json.dumps(obj, separators=(",", ":")))

def on_open(ws, auth_hash):
    print("[WS] Connected. Sending Heartbeat & D-Subscribe")
    # Dokümana göre Token = Authorization (hash)
    send_json(ws, {"Type": "H", "Token": auth_hash})
    symbols = [s.strip() for s in SYMBOLS.split(",")] if SYMBOLS != "ALL" else ["ALL"]
    send_json(ws, {"Type": "D", "Token": auth_hash, "Symbols": symbols})

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception as e:
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
    # Hash/token’ı mevcut login’den alıyoruz
    token, auth_hash = load_login_settings(ACCOUNT_NAME)

    # WS handshake headers (Checker: /ws + empty body)
    endpoint = "/ws"
    checker  = make_checker(API_KEY, API_HOSTNAME, endpoint, "")
    headers = [
        f"APIKEY: {API_KEY}",
        f"Authorization: {auth_hash}",
        f"Checker: {checker}"
    ]

    ws = websocket.WebSocketApp(
        WSS_ENDPOINT,
        header=headers,
        on_open=lambda w: on_open(w, auth_hash),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # Basit reconnect döngüsü
    while True:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[WS] run_forever exception: {e}")
        time.sleep(3)

if __name__ == "__main__":
    run()
