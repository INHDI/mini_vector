#!/usr/bin/env python3
"""
Simple HTTP benchmark: sends N JSON events to the HTTP source.

Usage:
  python scripts/bench_http.py --url http://127.0.0.1:9000/ingest --count 10000 --concurrency 4
"""
import argparse
import asyncio
import json
import time

import aiohttp


async def worker(session, url, payload, n, results):
    sent = 0
    while sent < n:
        await session.post(url, json=payload)
        sent += 1
    results.append(sent)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="HTTP ingest URL")
    parser.add_argument("--count", type=int, default=10000, help="Total events to send")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent senders")
    args = parser.parse_args()

    payload = {"message": "bench event", "tenant": "bench"}
    per_worker = args.count // args.concurrency
    remainder = args.count % args.concurrency

    async with aiohttp.ClientSession() as session:
        tasks = []
        results = []
        start = time.perf_counter()
        for i in range(args.concurrency):
            n = per_worker + (1 if i < remainder else 0)
            tasks.append(asyncio.create_task(worker(session, args.url, payload, n, results)))
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

    total = sum(results)
    rate = total / elapsed if elapsed > 0 else 0
    print(f"sent={total} elapsed={elapsed:.2f}s rate={rate:.0f} eps")


if __name__ == "__main__":
    asyncio.run(main())
