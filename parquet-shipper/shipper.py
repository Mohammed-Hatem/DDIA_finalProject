import os
import time
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

PARQUET_DIR = os.environ.get("PARQUET_DIR", "/data/parquet")
STATE_FILE = os.environ.get("STATE_FILE", "/data/parquet_shipper_state.txt")
ES_HOST = os.environ.get("ES_HOST", "http://elasticsearch:9200")
INDEX_NAME = "weather-status"

def get_processed_files():
    if not os.path.exists(STATE_FILE):
        return set()
    with open(STATE_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())

def add_processed_file(filepath):
    with open(STATE_FILE, "a") as f:
        f.write(f"{filepath}\n")

def connect_es():
    for _ in range(30):
        try:
            es = Elasticsearch(ES_HOST)
            if es.ping():
                print(f"[Shipper] Connected to Elasticsearch at {ES_HOST}")
                return es
        except Exception as e:
            print(f"[Shipper] Waiting for Elasticsearch... ({e})")
        time.sleep(2)
    raise Exception("Could not connect to Elasticsearch")

def process_new_files(es):
    processed = get_processed_files()
    
    for root, dirs, files in os.walk(PARQUET_DIR):
        for file in files:
            if file.endswith(".parquet"):
                filepath = os.path.join(root, file)
                if filepath not in processed:
                    print(f"[Shipper] Processing new file: {filepath}")
                    try:
                        df = pd.read_parquet(filepath)
                        if "message_dropped" not in df.columns:
                            df["message_dropped"] = False
                        # Prepare data for ES
                        actions = []
                        for _, row in df.iterrows():
                            doc_id = f"{row['station_id']}_{row['s_no']}"
                            
                            doc = row.to_dict()
                            
                            action = {
                                "_index": INDEX_NAME,
                                "_id": doc_id,
                                "_source": doc
                            }
                            actions.append(action)
                        
                        if actions:
                            success, _ = bulk(es, actions)
                            print(f"[Shipper] Successfully indexed {success} documents from {filepath}")
                        
                        add_processed_file(filepath)
                    except Exception as e:
                        print(f"[Shipper] Failed to process {filepath}: {e}")

if __name__ == "__main__":
    print("[Shipper] Starting Parquet to Elasticsearch Shipper...")
    es = connect_es()
    
    while True:
        process_new_files(es)
        time.sleep(10)
