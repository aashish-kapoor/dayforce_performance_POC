import csv
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

jtl_path = sys.argv[1]
endpoint = os.environ["DCE_INGESTION_URL"]
dcr_immutable_id = os.environ["DCR_IMMUTABLE_ID"]
stream_name = os.environ["STREAM_NAME"]
access_token = os.environ["ACCESS_TOKEN"]
test_run_id = os.environ["TEST_RUN_ID"]

BATCH_SIZE = 1000

def epoch_ms_to_iso(ts_ms):
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def push_batch(records):
    if not records:
        return
    url = f"{endpoint}/dataCollectionRules/{dcr_immutable_id}/streams/{stream_name}?api-version=2023-01-01"
    body = json.dumps(records).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"Pushed batch of {len(records)} records — HTTP {resp.status}")
    except urllib.error.HTTPError as e:
        print(f"Failed to push batch: HTTP {e.code} — {e.read().decode()}")
        sys.exit(1)

records = []
total = 0
with open(jtl_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        record = {
            "TimeGenerated": epoch_ms_to_iso(row["timeStamp"]),
            "SamplerLabel": row.get("label", ""),
            "ResponseTimeMs": float(row.get("elapsed", 0)),
            "Success": row.get("success", "false").lower() == "true",
            "ResponseCode": row.get("responseCode", ""),
            "ThreadName": row.get("threadName", ""),
            "TestRunId": test_run_id
        }
        records.append(record)
        total += 1
        if len(records) >= BATCH_SIZE:
            push_batch(records)
            records = []

push_batch(records)
print(f"Done. Total records processed: {total}")