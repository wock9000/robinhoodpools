"""Open-loop HTTP load generator that replays the LP terminal's polling mix.

    uv run --with aiohttp tools/lp_load.py https://rhpools.lol --rate 50 200 500
    uv run --with aiohttp tools/lp_load.py http://127.0.0.1:8196 --mix heavy --json out.json

Requests are issued at a fixed rate regardless of how fast the server answers,
so a saturated server shows up as rising latency and 503/timeout counts rather
than as a lower request rate. The default mix is one terminal visitor's traffic
over twelve seconds: twelve `/status` polls, one `/overview`, one `/pools`, and
0.8 `/tape`. The heavy mix adds the owners projections for every window and the
dislocations scan, which is what an open research tab generates.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import random
import sys
import time

import aiohttp

_POOLS = "/api/lp/pools?window={w}&limit=100&offset=0&sort=fees&order=desc"
_TAPE = "/api/lp/tape?window={w}&limit=150&offset=0&kind=lp"
_OWNERS = "/api/lp/owners?window={w}&limit=100&offset=0"
_DISLOCATIONS = "/api/lp/dislocations?min_bps=25&min_depth_usd=100&max_age_s=3600&max_stale_s=86400&sort=net&limit=100"

MIXES = {
    "terminal": {
        "status": ("/api/lp/status", 12.0),
        "overview": ("/api/lp/overview?window=24h", 1.0),
        "pools": (_POOLS.format(w="24h"), 1.0),
        "tape": (_TAPE.format(w="24h"), 0.8),
    },
    "heavy": {
        "status": ("/api/lp/status", 12.0),
        "overview": ("/api/lp/overview?window=24h", 1.0),
        "pools": (_POOLS.format(w="24h"), 1.0),
        "tape": (_TAPE.format(w="24h"), 0.8),
        "dislocations": (_DISLOCATIONS, 0.5),
        "owners-24h": (_OWNERS.format(w="24h"), 0.5),
        "owners-7d": (_OWNERS.format(w="7d"), 0.5),
        "owners-30d": (_OWNERS.format(w="30d"), 0.5),
    },
}
HEADERS = {"Accept": "application/json", "Accept-Encoding": "gzip"}


def percentile(values, fraction):
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))]


async def fetch(session, base, name, url, samples, in_flight):
    started = time.perf_counter()
    status = 0
    try:
        async with session.get(base + url, headers=HEADERS) as response:
            status = response.status
            await response.read()
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
        status = 0
    finally:
        samples.append((name, status, time.perf_counter() - started))
        in_flight.release()


async def run(base, rate, duration, mix, timeout, in_flight_cap):
    names = list(mix)
    weights = [mix[name][1] for name in names]
    samples = []
    in_flight = asyncio.Semaphore(in_flight_cap)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600)
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    tasks = set()
    total = int(rate * duration)
    async with aiohttp.ClientSession(connector=connector, timeout=client_timeout) as session:
        origin = time.perf_counter()
        for index in range(total):
            due = origin + index / rate
            delay = due - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            name = random.choices(names, weights)[0]
            await in_flight.acquire()
            task = asyncio.create_task(fetch(session, base, name, mix[name][0], samples, in_flight))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        if tasks:
            await asyncio.wait(tasks)
        elapsed = time.perf_counter() - origin
    return summarize(rate, duration, elapsed, samples)


def summarize(rate, duration, elapsed, samples):
    latencies = [seconds * 1000 for _, _, seconds in samples]
    statuses = collections.Counter(status for _, status, _ in samples)
    per_route = {}
    for name in sorted({name for name, _, _ in samples}):
        route_latencies = [s * 1000 for n, _, s in samples if n == name]
        route_statuses = collections.Counter(st for n, st, _ in samples if n == name)
        per_route[name] = {
            "count": len(route_latencies),
            "p50_ms": percentile(route_latencies, 0.5),
            "p95_ms": percentile(route_latencies, 0.95),
            "p99_ms": percentile(route_latencies, 0.99),
            "statuses": dict(sorted(route_statuses.items())),
        }
    return {
        "target_rps": rate,
        "duration_s": duration,
        "achieved_rps": len(samples) / elapsed if elapsed else 0.0,
        "requests": len(samples),
        "p50_ms": percentile(latencies, 0.5),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "statuses": dict(sorted(statuses.items())),
        "routes": per_route,
    }


def row(result):
    statuses = " ".join(f"{code}:{count}" for code, count in result["statuses"].items())
    return (
        f"| {result['target_rps']:>4} | {result['achieved_rps']:7.1f} | {result['p50_ms']:8.1f} "
        f"| {result['p95_ms']:8.1f} | {result['p99_ms']:8.1f} | {statuses} |"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base", help="origin such as https://rhpools.lol or http://127.0.0.1:8196")
    ap.add_argument("--rate", type=float, nargs="+", default=[50, 200, 500], help="requests per second, one run per value")
    ap.add_argument("--duration", type=float, default=60.0, help="seconds per run")
    ap.add_argument("--mix", choices=sorted(MIXES), default="terminal")
    ap.add_argument("--timeout", type=float, default=20.0, help="per-request deadline in seconds; a timeout counts as status 0")
    ap.add_argument("--in-flight", type=int, default=8192, help="cap on concurrent requests")
    ap.add_argument("--json", help="write every run's summary to this file")
    args = ap.parse_args()
    results = []
    print("| rps | achieved | p50 ms | p95 ms | p99 ms | statuses |")
    print("|---:|---:|---:|---:|---:|---|")
    for rate in args.rate:
        result = asyncio.run(run(args.base.rstrip("/"), rate, args.duration, MIXES[args.mix], args.timeout, args.in_flight))
        results.append(result)
        print(row(result), flush=True)
    for result in results:
        for name, stats in result["routes"].items():
            print(
                f"{result['target_rps']:>6} rps {name:<14} n={stats['count']:<6} "
                f"p50={stats['p50_ms']:8.1f} p95={stats['p95_ms']:8.1f} p99={stats['p99_ms']:8.1f} {stats['statuses']}",
                file=sys.stderr,
            )
    if args.json:
        with open(args.json, "w") as handle:
            json.dump({"base": args.base, "mix": args.mix, "runs": results}, handle, indent=1)


if __name__ == "__main__":
    main()
