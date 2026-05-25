import pathlib
import os
import random
import sys
import time
import json
from datetime import datetime
from pathlib import Path

# ---- Import Azure Event Hub SDK ─────────────────────────────────────────────
try:
    from azure.eventhub import EventData, EventHubProducerClient
except ImportError:
    print(" azure-eventhub not installed. Run: pip install azure-eventhub")
    sys.exit(1)

# ---- Importing azure eventhub config ─────────────────────────────────────────

from dotenv import load_dotenv
import os

load_dotenv()

EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
#Endpoint = os.getenv("Endpoint")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")


# ---- Local generator imports ────────────────────────────────────────────────
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
try:
    from generators import CFG, seed_all
    BATCH_SIZE = CFG.get("streaming_batch_size", 100)
except ImportError:
    BATCH_SIZE = 100  # Fallback if config isn't found

# ---- CONFIG for Paths ────────────────────────────────────────────────────────
TRANSACTIONS_FILE = Path(__file__).parent.parent /"output"/"transactions.json1"
CHECKPOINT_FILE = Path("./checkpoints/transaction_checkpoint.json")


def update_checkpoint(transaction_idx: int):
    """Safely updates or creates the checkpoint file tracking the line index."""
    # Ensure the parent directory (./checkpoints/) exists
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    checkpoint_data = {
        "last_processed_line": transaction_idx,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC")
    }
    
    # Atomic save using a temp file to prevent corruption on sudden crash
    temp_checkpoint = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(temp_checkpoint, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
    temp_checkpoint.replace(CHECKPOINT_FILE)


def check_checkpoint() -> int:
    """Returns the last processed line index, or 0 if it doesn't exist/is broken."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                file_data = json.load(f)
                return file_data.get("last_processed_line", 0)
        except (json.JSONDecodeError, KeyError, TypeError):
            print(" Checkpoint file corrupted. Overwriting and starting from 0.")
    
    # Initialize a clean checkpoint file at 0 if missing or corrupt
    update_checkpoint(0)
    return 0


def stream_to_eventhub(interval_seconds=3, max_transactions=None):
    if not EVENTHUB_CONNECTION_STRING:
        print(" Please set a valid EVENTHUB_CONNECTION_STRING before running.")
        return

    if not TRANSACTIONS_FILE.exists():
        print(f" Transactions file missing at: {TRANSACTIONS_FILE}")
        return

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    
    # Find out where we left off
    start_line = check_checkpoint()
    print(f"\n Connected to Event Hub: {EVENTHUB_NAME}")
    print(f" Resuming stream from line index: {start_line:,}\n")
    
    transaction_count = 0
    
    try:
        with open(TRANSACTIONS_FILE, "r") as f:
            for current_idx, line in enumerate(f):
                
                # 1. Skip previously processed lines
                if current_idx < start_line:
                    continue

                clean_line = line.strip()
                if not clean_line:
                    continue

                try:
                    # 2. Parse the individual JSON row
                    transaction = json.loads(clean_line)
                    
                    # 3. Package and stream to Event Hub
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(EventData(clean_line)) # Passing raw string bytes is faster
                    producer.send_batch(event_data_batch)
                    
                    transaction_count += 1
                    
                    # 4. Print metrics to screen
                    print(f"[{transaction_count}] Sent Line Index: {current_idx}")
                    print(f"   ID: {transaction.get('transaction_id')} | "
                          f"Currency: {transaction.get('currency')} | "
                          f"Amount: {transaction.get('amount')}")
                    
                    # 5. Commit checkpoint immediately after successful transmit
                    update_checkpoint(current_idx + 1)
                    
                    # 6. Evaluate exit conditions
                    if max_transactions and transaction_count >= max_transactions:
                        print(f"\n Reached max target limit of {max_transactions} transactions.")
                        break
                    
                    time.sleep(interval_seconds)

                except json.JSONDecodeError:
                    print(f" Corrupt JSON found at line {current_idx}. Skipping row.")
                    continue
                except Exception as e:
                    print(f" Failed to process line {current_idx}. Error: {e}")
                    return

    except KeyboardInterrupt:
        print("\n Stream paused by user via Ctrl+C.")
    finally:
        producer.close()
        print("🔌 Event Hub Producer connection cleanly closed.")


if __name__ == "__main__":
    # Example: stream transactions every 3 seconds until you press Ctrl+C
    stream_to_eventhub(interval_seconds=3)