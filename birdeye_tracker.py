import asyncio
import websockets
import json
import logging
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from google.cloud import storage
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

# 🔹 Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "birdeye-tracker-bucket")
API_KEY = os.getenv("BIRDEYE_API_KEY")
CHAIN = os.getenv("BIRDEYE_CHAIN", "solana")
PORT = int(os.getenv("PORT", 8080))

if not API_KEY:
    raise ValueError("Error: API key BIRDEYE_API_KEY is not set!")

WEBSOCKET_URL = f"wss://public-api.birdeye.so/socket/{CHAIN}?x-api-key={API_KEY}"

# 🔹 Google Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

# 🔹 Storing a list of all tracked tokens
all_tokens = set()  # Use set() for uniqueness of tokens

# 🔹 HTTP server for Cloud Run
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

def start_http_server():
    """Starts the HTTP server for Cloud Run"""
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
        logging.info(f"✅ HTTP server is running on port {PORT}")
        server.serve_forever()
    except Exception as e:
        logging.error(f"❌ HTTP server error: {e}")

async def connect_and_track():
    """Connects to WebSocket and subscribes to all tokens"""
    global all_tokens

    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, subprotocols=["echo-protocol"]) as ws:
                logging.info(f"✅ Connected to Birdeye WebSocket (chain={CHAIN})")

                # Subscribe to new tokens
                subscribe_message = {"type": "SUBSCRIBE_TOKEN_NEW_LISTING"}
                await ws.send(json.dumps(subscribe_message))
                logging.info(f"📤 Sending a subscription: {subscribe_message}")

                # If there are already tokens in memory, subscribe to them
                if all_tokens:
                    await subscribe_to_multiple_trades(ws, list(all_tokens))

                async for message in ws:
                    data = json.loads(message)
                    logging.info(f"🔹 Message received: {data}")

                    if data.get("type") == "TOKEN_NEW_LISTING_DATA":
                        token_address = data["data"].get("address")
                        if token_address and token_address not in all_tokens:
                            all_tokens.add(token_address)
                            await subscribe_to_multiple_trades(ws, [token_address])

                    elif data.get("type") == "TXS_DATA":
                        tx_data = safe_parse_transaction(data.get("data"))

                        if tx_data:
                            await process_transaction(tx_data)
                        else:
                            logging.warning(f"⚠️ Transaction missed due to incorrect formatting.")

        except (ConnectionClosed, ConnectionClosedError) as e:
            logging.error(f"⚠️ Ошибка WebSocket: {e}. Reconnect after 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"❌ WebSocket error: {e}")

async def subscribe_to_multiple_trades(ws, token_addresses):
    """Subscribes to transactions at once for 100 tokens in one request"""
    CHUNK_SIZE = 100  # Maximum number of tokens in one subscription
    token_groups = [token_addresses[i:i + CHUNK_SIZE] for i in range(0, len(token_addresses), CHUNK_SIZE)]

    for group in token_groups:
        query = " OR ".join([f"address = {token}" for token in group])
        subscription_message = {
            "type": "SUBSCRIBE_TXS",
            "data": {
                "queryType": "complex",
                "query": query
            }
        }
        await ws.send(json.dumps(subscription_message))
        logging.info(f"📡 Subscription for {len(group)} tokens has been sent.")

def safe_parse_transaction(data):
    """
    Takes `data` from the TXS_DATA event and returns dict or None.
    Processes the string, the dictionary, and throws away the garbage.
    """
    try:
        # A dictionary already? - Okay.
        if isinstance(data, dict):
            return data

        # A string? - parse
        if isinstance(data, str):
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                return parsed
            else:
                logging.error(f"❌ After json.loads() the result is not a dictionary! Type: {type(parsed)} | Value: {parsed}")
                return None

        # Inappropriate type
        logging.error(f"⚠️ Unsupported data type in TXS_DATA: {type(data)} | Value: {data}")
        return None

    except json.JSONDecodeError as e:
        logging.error(f"❌ JSON decode error: {e} | String: {data}")
        return None
    except Exception as e:
        logging.error(f"❌ Unknown parsing error TXS_DATA: {e} | Type: {type(data)} | Value: {data}")
        return None


async def process_transaction(transaction_data):
    """Processes the transaction and stores it in GCS."""
    try:
        # 🔹 Check that transaction_data is a dictionary
        if not isinstance(transaction_data, dict):
            logging.error(f"⚠️ transaction_data is not a dictionary: {type(transaction_data)} - {transaction_data}")
            return

        # 🔹 Define tokenAddress
        token_address = transaction_data.get("tokenAddress")  # If there is a tokenAddress in the structure

        if not token_address:
            to_data = transaction_data.get("to")
            if isinstance(to_data, dict):  # Check if “to” is a dictionary
                token_address = to_data.get("address")

        if not token_address:
            logging.error(f"⚠️ Failed to determine 'tokenAddress', skip: {transaction_data}")
            return

        # 🕒 Processing the timestamp
        block_time = transaction_data.get("blockUnixTime")
        if not isinstance(block_time, (int, float)):
            logging.error(f"⚠️ Incorrect blockUnixTime timestamp: {block_time}")
            return
        
        timestamp = datetime.fromtimestamp(block_time, tz=timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

        # 📂 Forming the file path
        filename = f"transactions/{token_address}/{timestamp}.json"
        logging.info(f"📂 Preparing to upload a file: {filename}")

        # ✅ Check JSON correctness before saving
        try:
            json_data = json.dumps(transaction_data, indent=2, default=str)  # Insuring against incorrect types
        except Exception as e:
            logging.error(f"❌ JSON serialization error: {e}, data: {transaction_data}")
            return

        # ✅ Upload to Google Cloud Storage
        try:
            blob = bucket.blob(filename)
            logging.info(f"INFO: Тип blob: {type(blob)}, Value: {blob}")

            blob.upload_from_string(json_data)

            logging.info(f"✅ File successfully uploaded to GCS: {filename}")

        except Exception as e:
            logging.error(f"❌ File upload error in GCS: {e}")

    except Exception as e:
        logging.error(f"❌ Transaction Processing Error ({type(e).__name__}): {e}, data: {transaction_data}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # HTTP server in a separate thread
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    logging.info("🚀 Starting a WebSocket Connection...")
    asyncio.run(connect_and_track())
