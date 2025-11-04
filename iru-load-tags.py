#!/usr/bin/env python3
# File: iru-load-tags.py
"""
Iru Kandji tag bulk creator with duplicate checks, optional prefetch, retries, concurrency,
rate-limiting, and CSV reporting. Stdlib only.

Env:
  API_TOKEN=...  # required

Examples:
  python iru-load-tags.py -f tags.txt --prefetch --concurrency 8 --qps 5 --report-path report.csv
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from typing import Dict, Iterable, List, Optional, Tuple

API_BASE_URL = "https://REPLACE.api.kandji.io/api/v1/tags"


# ---------- Utilities ----------

class RateLimiter:
    """Global, simple spacing to keep QPS under control."""
    def __init__(self, qps: Optional[float]):
        self.interval = 1.0 / qps if qps and qps > 0 else 0.0
        self.lock = threading.Lock()
        self.last = 0.0

    def acquire(self) -> None:
        if self.interval <= 0:
            return
        with self.lock:
            now = time.time()
            wait = max(0.0, (self.last + self.interval) - now)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


def read_tags_file(path: str) -> List[str]:
    out: List[str] = []
    seen = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


# ---------- HTTP with retries ----------

def http_request(
    method: str,
    url: str,
    token: str,
    json_body: Optional[dict],
    timeout: int,
    max_retries: int,
    rate_limiter: RateLimiter,
    verbose: bool,
) -> Tuple[int, Optional[dict], str]:
    """
    Returns: (status_code, parsed_json_or_None, raw_text)
    Retries on 429 and 5xx with exponential backoff.
    """
    backoff = 1.0
    attempt = 0
    data: Optional[bytes] = None
    if json_body is not None:
        data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")

    while True:
        attempt += 1
        rate_limiter.acquire()  # avoid bursts
        req = urllib.request.Request(url=url, method=method.upper())
        req.add_header("Authorization", f"Bearer {token}")
        if json_body is not None:
            req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        try:
            with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
                status = getattr(resp, "status", resp.getcode())
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            status = e.code
            raw = e.read().decode("utf-8", errors="replace")
        except Exception as e:
            # Network/timeout → treat as 599 for visibility.
            status, raw = 599, str(e)

        parsed: Optional[dict] = None
        if raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None

        if verbose:
            sys.stderr.write(f"{now_iso()} [{method} {url}] attempt={attempt} status={status}\n")

        if status == 429 or (500 <= status <= 599):
            if attempt <= max_retries:
                time.sleep(backoff)
                backoff *= 2
                continue
        return status, parsed, raw


# ---------- Tag logic ----------

class TagCache:
    """Thread-safe name→id cache honoring case sensitivity."""
    def __init__(self, case_sensitive: bool):
        self.case_sensitive = case_sensitive
        self._map: Dict[str, str] = {}
        self._lock = threading.Lock()

    def _key(self, name: str) -> str:
        return name if self.case_sensitive else name.lower()

    def get(self, name: str) -> Optional[str]:
        with self._lock:
            return self._map.get(self._key(name))

    def put(self, name: str, tag_id: str) -> None:
        with self._lock:
            self._map[self._key(name)] = tag_id

    def bulk_put(self, items: Iterable[Tuple[str, str]]) -> None:
        with self._lock:
            for name, tag_id in items:
                self._map[self._key(name)] = tag_id


def paginate_all_tags(
    token: str,
    timeout: int,
    max_retries: int,
    rate_limiter: RateLimiter,
    verbose: bool,
) -> Iterable[dict]:
    """Generator of tag objects across pages."""
    next_url = API_BASE_URL
    while next_url:
        status, parsed, raw = http_request(
            "GET", next_url, token, None, timeout, max_retries, rate_limiter, verbose
        )
        if status < 200 or status >= 300 or not isinstance(parsed, dict):
            sys.stderr.write(f"{now_iso()} WARN prefetch failed status={status}\n")
            return
        for item in (parsed.get("results") or []):
            yield item
        next_url = parsed.get("next")


def find_existing_id(
    tag: str,
    cache: TagCache,
    token: str,
    timeout: int,
    max_retries: int,
    rate_limiter: RateLimiter,
    verbose: bool,
) -> Optional[str]:
    # Cache hit
    cached = cache.get(tag)
    if cached:
        return cached

    # On-demand search
    q = urllib.parse.quote(tag, safe="")
    url = f"{API_BASE_URL}?search={q}"
    status, parsed, raw = http_request(
        "GET", url, token, None, timeout, max_retries, rate_limiter, verbose
    )
    if status < 200 or status >= 300 or not isinstance(parsed, dict):
        # On search failure, return None so caller can decide (we'll attempt create unless --force=0 & we really need proof)
        return None

    results = parsed.get("results") or []
    for item in results:
        name = item.get("name") or ""
        if (name == tag) if cache.case_sensitive else (name.lower() == tag.lower()):
            tag_id = item.get("id")
            if tag_id:
                cache.put(name, tag_id)
            return tag_id
    return None


def create_tag(
    tag: str,
    token: str,
    timeout: int,
    max_retries: int,
    rate_limiter: RateLimiter,
    verbose: bool,
) -> Tuple[int, Optional[str], str]:
    body = {"name": tag}
    status, parsed, raw = http_request(
        "POST", API_BASE_URL, token, body, timeout, max_retries, rate_limiter, verbose
    )
    tag_id = None
    if isinstance(parsed, dict):
        tag_id = parsed.get("id")
    return status, tag_id, raw


# ---------- CSV reporting ----------

class CsvReporter:
    def __init__(self, path: str):
        self.path = path
        self._fp = open(path, "w", newline="", encoding="utf-8")
        self._w = csv.writer(self._fp)
        self._w.writerow(["timestamp", "tag", "action", "http_code", "id", "message"])
        self._lock = threading.Lock()

    def row(self, tag: str, action: str, http_code: Optional[int], _id: str, message: str) -> None:
        # Lock avoids row interleaving across threads.
        with self._lock:
            self._w.writerow([now_iso(), tag, action, (http_code or ""), _id, message])

    def close(self) -> None:
        self._fp.close()


# ---------- Worker ----------

def process_one(
    tag: str,
    *,
    dry_run: bool,
    force: bool,
    sleep_secs: float,
    cache: TagCache,
    token: str,
    timeout: int,
    max_retries: int,
    rate_limiter: RateLimiter,
    reporter: CsvReporter,
    verbose: bool,
) -> bool:
    """Returns True on success."""
    existing_id = None if force else find_existing_id(
        tag, cache, token, timeout, max_retries, rate_limiter, verbose
    )

    if existing_id and not force:
        # Why: preserve idempotency & avoid API noise.
        reporter.row(tag, "skip_exists" if not dry_run else "dry_run_skip_exists", 200, existing_id, "Already exists")
        if verbose:
            sys.stderr.write(f"{now_iso()} SKIP exists tag='{tag}' id='{existing_id}'\n")
        return True

    if dry_run:
        reporter.row(tag, "dry_run_create", None, "", "POST suppressed")
        if verbose:
            sys.stderr.write(f"{now_iso()} DRY-RUN create tag='{tag}'\n")
        time.sleep(max(0.0, sleep_secs))
        return True

    status, new_id, raw = create_tag(tag, token, timeout, max_retries, rate_limiter, verbose)
    ok = 200 <= status < 300
    if ok:
        reporter.row(tag, "created", status, new_id or "", "OK")
        if new_id:
            cache.put(tag, new_id)
    else:
        reporter.row(tag, "fail", status, "", (raw or "")[:200])
        sys.stderr.write(f"{now_iso()} FAIL tag='{tag}' status={status} body={(raw or '')[:400]}\n")

    time.sleep(max(0.0, sleep_secs))
    return ok


# ---------- Main ----------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk create Kandji tags with duplicate checks.")
    p.add_argument("-f", "--file", required=True, help="Path to tags file (one tag per line).")
    p.add_argument("--dry-run", action="store_true", help="Do not POST; log intended actions.")
    p.add_argument("--concurrency", type=int, default=1, help="Parallel workers (default: 1).")
    p.add_argument("--timeout", type=int, default=20, help="HTTP timeout per attempt (s).")
    p.add_argument("--max-retries", type=int, default=5, help="Retries on 429/5xx.")
    p.add_argument("--sleep", type=float, default=0.3, help="Pause after each tag (s).")
    p.add_argument("--qps", type=float, default=0.0, help="Max requests per second (process-wide).")
    p.add_argument("--verbose", action="store_true", help="Verbose stderr logs.")
    p.add_argument("--force", action="store_true", help="Create even if exact-name exists.")
    p.add_argument("--case-sensitive", action="store_true", help="Exact-match uses case sensitivity.")
    p.add_argument("--prefetch", action="store_true", help="Fetch all tags once at start.")
    p.add_argument("--report-path", default="", help="CSV path (default autogenerated).")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    token = os.environ.get("API_TOKEN")
    if not token:
        sys.stderr.write("Error: API_TOKEN env var not set. Be sure to 'export API_TOKEN='\n")
        return 2
    if not os.path.isfile(args.file):
        sys.stderr.write(f"Error: file not found: {args.file}\n")
        return 2
    if args.concurrency < 1:
        sys.stderr.write("Error: --concurrency must be >= 1\n")
        return 2

    tags = read_tags_file(args.file)
    if not tags:
        sys.stderr.write("No tags to process.\n")
        return 0

    report_path = args.report_path or f"kandji_tag_report_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    reporter = CsvReporter(report_path)
    cache = TagCache(case_sensitive=args.case_sensitive)
    rate_limiter = RateLimiter(args.qps if args.qps > 0 else None)

    # Prefetch once (optional)
    if args.prefetch:
        if args.verbose:
            sys.stderr.write(f"{now_iso()} Prefetching all tags...\n")
        items = []
        for obj in paginate_all_tags(token, args.timeout, args.max_retries, rate_limiter, args.verbose):
            name, tag_id = (obj.get("name") or ""), (obj.get("id") or "")
            if name and tag_id:
                items.append((name, tag_id))
        cache.bulk_put(items)
        if args.verbose:
            sys.stderr.write(f"{now_iso()} Prefetch complete: cached {len(items)} tag(s)\n")

    ok_all = True
    if args.concurrency == 1:
        for tag in tags:
            ok = process_one(
                tag,
                dry_run=args.dry_run,
                force=args.force,
                sleep_secs=args.sleep,
                cache=cache,
                token=token,
                timeout=args.timeout,
                max_retries=args.max_retries,
                rate_limiter=rate_limiter,
                reporter=reporter,
                verbose=args.verbose,
            )
            ok_all = ok_all and ok
    else:
        # Thread pool; shared cache/report are guarded.
        with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futs = [
                ex.submit(
                    process_one,
                    tag,
                    dry_run=args.dry_run,
                    force=args.force,
                    sleep_secs=args.sleep,
                    cache=cache,
                    token=token,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    rate_limiter=rate_limiter,
                    reporter=reporter,
                    verbose=args.verbose,
                )
                for tag in tags
            ]
            for fut in cf.as_completed(futs):
                ok_all = ok_all and bool(fut.result())

    reporter.close()
    if args.verbose:
        sys.stderr.write(f"{now_iso()} Report: {report_path}\n")
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
