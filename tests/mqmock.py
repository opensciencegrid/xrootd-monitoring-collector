#!/usr/bin/env python

import argparse
import json
import sys
import time
import requests

QUEUE_IN = "xrd.push"
EXCHANGES_OUT = [
    "xrd.detailed",
    "xrd.wlcg_format",
    "xrd.tcp_exchange",
    "xrd.cache.stash",
    "xrd.cache.wlcg",
    "xrd.tpc",
    "xrd.tpc.wlcg",
]


def main():
    args = parse_args()

    # Send input records into mock MQ
    rec_count = 0
    for line in args.input:
        rec_count += 1
        data = json.loads(line)

        msg = {
            "value": data,
            "exchange": "amq.default",
            "routing_key": QUEUE_IN,
            "properties": None,
        }

        resp = requests.post(f"{args.mock_mq_url}/queues/{QUEUE_IN}/messages", json=msg)
        resp.raise_for_status()

    print(f"Sent {rec_count} records to mock MQ", file=sys.stderr)
    print(f"Waiting {args.wait}s for processing", file=sys.stderr)
    time.sleep(args.wait)

    # Retrieve records
    for ex in sorted(EXCHANGES_OUT):
        print(f"Getting messages from exchange {ex}", file=sys.stderr)
        resp = requests.get(f"{args.mock_mq_url}/exchanges/{ex}/messages")
        resp.raise_for_status()

        for rec in resp.json():
            # Remove random value
            del rec["id"]

            # Remove WLCG non-deterministic values
            if rec["exchange"] == "xrd.wlcg_format":
                del rec["value"]["metadata"]["_id"]
                del rec["value"]["metadata"]["timestamp"]
                del rec["value"]["unique_id"]

            print(json.dumps(rec, sort_keys=True))

        # Delete messages from exchange
        resp = requests.delete(f"{args.mock_mq_url}/exchanges/{ex}/messages")
        resp.raise_for_status()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mock-mq-url", default="http://mock-mq")
    parser.add_argument("--wait", default=5)
    parser.add_argument(
        "input", type=argparse.FileType("r"), help="records in JSONL format"
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
