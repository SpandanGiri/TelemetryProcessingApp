import requests
import time
from datasets import load_dataset

# 1. Load the dataset from Hugging Face
print("Downloading/Loading dataset from Hugging Face...")
# The datasets library will cache this locally after the first download
dataset = load_dataset("tarekmasryo/llm-system-ops-production-telemetry-sft-data", split="train")

url = "http://localhost:8000/ingest"

print(f"Dataset loaded successfully! Found {len(dataset)} rows.")
print("Starting telemetry stream (first 10 rows)...\n")

for i, row in enumerate(dataset.select(range(10))):
# Using .get() ensures the script doesn't crash if a specific column is named differently.
    payload = {
        "user_id": str(row.get("user_id", f"user_hf_{i}")),
        "model_name": str(row.get("model_name", row.get("model", "gpt-4"))),
        "input_tokens": int(row.get("prompt_tokens", row.get("input_tokens", 150))),
        "output_tokens": int(row.get("completion_tokens", row.get("output_tokens", 50)))
    }

    # 3. Send the POST request to your local Docker container
    try:
        response = requests.post(url, json=payload)
        print(f"[{i+1}/10] Sent: {payload['model_name']} | Status: {response.status_code} | API Reply: {response.json()}")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to FastAPI. Is your Docker container running?")
        break
    
    # 1-second delay to simulate real-time ingestion and make terminal output readable
    time.sleep(1)

print("\nFinished sending test batch.")
