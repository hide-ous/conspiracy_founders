#!/usr/bin/env python3
"""
Internet Archive Moderator History Scraper
Fetches moderator lists from Internet Archive (Wayback Machine) snapshots.

Usage:
    python archive_scraper.py --output results.json
    python archive_scraper.py --output results.json --resume

Requirements:
    pip install aiohttp python-dotenv
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Internet Archive CDX API
IA_CDX_BASE = "https://web.archive.org/cdx/search/cdx"
IA_WAYBACK_BASE = "https://web.archive.org/web"
IA_CONCURRENCY = 8
IA_TIMEOUT = 30
IA_MAX_RETRIES = 3
IA_RETRY_SLEEP = 2.0

# Rate limiting: 1 request per second globally
RATE_LIMIT_DELAY = 1.0  # seconds between requests

# Backoff settings for rate limits and Cloudflare blocks
RATE_LIMIT_BACKOFF = 60  # Initial backoff when rate limited (seconds)
CLOUDFLARE_BACKOFF = 300  # Initial backoff for Cloudflare blocks (5 minutes)
MAX_BACKOFF = 3600  # Maximum backoff time (1 hour)

USER_AGENT = os.getenv("USER_AGENT")
if not USER_AGENT:
    print("Error: USER_AGENT not found in .env file", file=sys.stderr)
    sys.exit(1)

# IA URL templates to snapshot-search
IA_URL_TEMPLATES = [
    "https://old.reddit.com/r/{sub}/about/moderators",
    "https://old.reddit.com/r/{sub}/about/moderators/.json",
    "https://www.reddit.com/r/{sub}/about/moderators",
    "https://www.reddit.com/mod/{sub}/moderators"
]

# Early stopping: 7 days in seconds
EARLY_STOP_THRESHOLD = 7 * 24 * 60 * 60

# Progress tracking directories
PROGRESS_DIR = Path("progress")
RESULTS_DIR = Path("results")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate Limiter with Backoff Support
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Global rate limiter with support for temporary pauses due to rate limits
    or Cloudflare blocks.
    """

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()
        self.paused_until = 0.0
        self.pause_lock = asyncio.Lock()

    async def acquire(self):
        """Wait until we're allowed to make another request."""
        async with self.lock:
            # Check if we're in a pause period
            now = time.time()
            if now < self.paused_until:
                wait_time = self.paused_until - now
                log.warning(f"⏸️  Rate limiter paused, waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

            # Normal rate limiting
            now = time.time()
            time_since_last = now - self.last_request_time

            if time_since_last < self.delay:
                wait_time = self.delay - time_since_last
                await asyncio.sleep(wait_time)

            self.last_request_time = time.time()

    async def pause(self, duration: float):
        """Pause all requests for the specified duration."""
        async with self.pause_lock:
            pause_until = time.time() + duration
            if pause_until > self.paused_until:
                self.paused_until = pause_until
                log.warning(f"⏸️  Pausing all requests for {duration:.1f} seconds")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class SubredditInfo:
    name: str
    creation_date: int
    earliest_post: int
    earliest_comment: int


@dataclass
class ModEntry:
    username: str
    added_utc: Optional[int]
    added_date: Optional[str]
    permissions: list[str] = field(default_factory=list)
    source: str = "unknown"
    snapshot_url: Optional[str] = None
    snapshot_timestamp: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_reddit_json(entry: dict, source: str, snapshot_url: str, snapshot_ts: str) -> "ModEntry":
        ts = entry.get("date")
        return ModEntry(
            username=entry.get("name", ""),
            added_utc=ts,
            added_date=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
            permissions=entry.get("mod_permissions", []),
            source=source,
            snapshot_url=snapshot_url,
            snapshot_timestamp=snapshot_ts,
        )


@dataclass
class SubredditResult:
    subreddit: str
    creation_date: int
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    moderators: list[ModEntry] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    stopped_early: bool = False
    early_stop_reason: Optional[str] = None
    snapshots_processed: int = 0
    snapshots_total: int = 0

    def merge(self, other_mods: list[ModEntry]) -> None:
        """Add mods not already present (keyed by username, prefer entries with dates)."""
        existing = {m.username.lower(): m for m in self.moderators}
        for m in other_mods:
            key = m.username.lower()
            if key not in existing:
                existing[key] = m
            elif m.added_utc and not existing[key].added_utc:
                existing[key] = m
        self.moderators = sorted(existing.values(), key=lambda x: x.added_utc or 0)

    def check_early_stop(self) -> bool:
        """Check if we found a mod within 7 days of subreddit creation."""
        for mod in self.moderators:
            if mod.added_utc:
                days_diff = (mod.added_utc - self.creation_date) / (24 * 60 * 60)
                if mod.added_utc <= self.creation_date or days_diff <= 7:
                    self.stopped_early = True
                    self.early_stop_reason = f"Found {mod.username} added within 7 days of creation (diff: {days_diff:.1f} days)"
                    return True
        return False

    def to_dict(self) -> dict:
        return {
            "subreddit": self.subreddit,
            "creation_date": self.creation_date,
            "creation_date_iso": datetime.fromtimestamp(self.creation_date, tz=timezone.utc).isoformat(),
            "fetched_at": self.fetched_at,
            "moderator_count": len(self.moderators),
            "moderators": [m.to_dict() for m in self.moderators],
            "errors": self.errors,
            "stopped_early": self.stopped_early,
            "early_stop_reason": self.early_stop_reason,
            "snapshots_processed": self.snapshots_processed,
            "snapshots_total": self.snapshots_total,
        }


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    """
    Sanitize a string to be safe for use as a filename/directory name.
    Replaces characters that are invalid on Windows/Unix filesystems.
    """
    # Replace invalid characters with underscore
    # Windows invalid chars: < > : " / \ | ? *
    # Also replace control characters (0-31) and DEL (127)
    invalid_chars = r'[<>:"/\\|?*\x00-\x1f\x7f]'
    sanitized = re.sub(invalid_chars, '_', name)

    # Remove leading/trailing spaces and dots (problematic on Windows)
    sanitized = sanitized.strip('. ')

    # Ensure the name is not empty
    if not sanitized:
        sanitized = 'unnamed'

    return sanitized


def save_progress(subreddit: str, result: SubredditResult, processed_snapshots: set[str]):
    """Save intermediate progress for a subreddit."""
    progress_file = PROGRESS_DIR / f"{sanitize_filename(subreddit)}.json"

    progress_data = {
        "result": result.to_dict(),
        "processed_snapshots": list(processed_snapshots),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

    progress_file.write_text(json.dumps(progress_data, indent=2), encoding="utf-8")
    log.debug(f"Progress saved for r/{subreddit}")


def load_progress(subreddit: str) -> tuple[Optional[SubredditResult], set[str]]:
    """Load progress for a subreddit if it exists."""
    progress_file = PROGRESS_DIR / f"{sanitize_filename(subreddit)}.json"

    if not progress_file.exists():
        return None, set()

    try:
        progress_data = json.loads(progress_file.read_text(encoding="utf-8"))

        # Reconstruct SubredditResult
        result_dict = progress_data["result"]
        result = SubredditResult(
            subreddit=result_dict["subreddit"],
            creation_date=result_dict["creation_date"],
            fetched_at=result_dict["fetched_at"],
            moderators=[ModEntry(**m) for m in result_dict["moderators"]],
            errors=result_dict["errors"],
            stopped_early=result_dict.get("stopped_early", False),
            early_stop_reason=result_dict.get("early_stop_reason"),
            snapshots_processed=result_dict.get("snapshots_processed", 0),
            snapshots_total=result_dict.get("snapshots_total", 0),
        )

        processed_snapshots = set(progress_data.get("processed_snapshots", []))

        log.info(f"Resuming r/{subreddit} - {len(processed_snapshots)} snapshots already processed")
        return result, processed_snapshots

    except (json.JSONDecodeError, KeyError) as e:
        log.warning(f"Failed to load progress for r/{subreddit}: {e}")
        return None, set()


def is_completed(subreddit: str) -> bool:
    """Check if a subreddit has been fully processed."""
    result_file = RESULTS_DIR / f"{sanitize_filename(subreddit)}.json"
    return result_file.exists()


def save_final_result(result: SubredditResult):
    """Save final result for a subreddit."""
    result_file = RESULTS_DIR / f"{sanitize_filename(result.subreddit)}.json"
    result_file.write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")

    # Clean up progress file
    progress_file = PROGRESS_DIR / f"{sanitize_filename(result.subreddit)}.json"
    if progress_file.exists():
        progress_file.unlink()

    log.info(f"Final result saved for r/{result.subreddit}")


# ---------------------------------------------------------------------------
# Internet Archive helpers
# ---------------------------------------------------------------------------

async def ia_cdx_snapshots_by_year(
        session: aiohttp.ClientSession,
        url: str,
        rate_limiter: RateLimiter,
        from_year: int = 2005,
        to_year: Optional[int] = None,
) -> list[dict]:
    """
    Query CDX API for one snapshot per year - parallelized.
    Returns a list of {timestamp, original, statuscode, mimetype} dicts.
    """
    if to_year is None:
        to_year = datetime.now().year

    async def fetch_year(year: int) -> Optional[dict]:
        params = {
            "output": "json",
            "url": url,
            "fl": "timestamp,original,statuscode,mimetype",
            "filter": "statuscode:200",
            "collapse": "timestamp:4",  # Collapse to year (YYYY)
            "limit": "1",
            "from": f"{year}0101",
            "to": f"{year}1231",
        }

        backoff_time = IA_RETRY_SLEEP

        for attempt in range(1, IA_MAX_RETRIES + 1):
            try:
                await rate_limiter.acquire()
                async with session.get(
                        IA_CDX_BASE,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
                ) as resp:
                    # Handle rate limiting
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", RATE_LIMIT_BACKOFF))
                        log.warning(f"⚠️  Rate limited (429) – pausing for {retry_after}s")
                        await rate_limiter.pause(retry_after)
                        continue

                    # Handle Cloudflare blocks
                    if resp.status in (403, 503):
                        is_cloudflare = (
                                'cloudflare' in resp.headers.get('Server', '').lower() or
                                resp.status == 503
                        )

                        if is_cloudflare:
                            backoff = min(CLOUDFLARE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
                            log.warning(
                                f"🛡️  Cloudflare block detected (HTTP {resp.status}) "
                                f"– pausing for {backoff}s (attempt {attempt}/{IA_MAX_RETRIES})"
                            )
                            await rate_limiter.pause(backoff)
                            continue

                    if resp.status != 200:
                        return None

                    rows = await resp.json(content_type=None)
                    if rows and len(rows) >= 2:
                        keys = rows[0]
                        return dict(zip(keys, rows[1]))
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.debug("CDX error for %s year %d attempt %d: %s", url, year, attempt, exc)
                if attempt < IA_MAX_RETRIES:
                    backoff_time = min(backoff_time * 2, MAX_BACKOFF)
                    await asyncio.sleep(backoff_time)
        return None

    # Fetch all years in parallel
    tasks = [fetch_year(year) for year in range(from_year, to_year + 1)]
    results = await asyncio.gather(*tasks)

    # Filter out None results
    return [snapshot for snapshot in results if snapshot is not None]


def _parse_mods_from_json(data: dict, snapshot_url: str, snapshot_ts: str) -> list[ModEntry]:
    """Extract mod entries from a /about/moderators.json snapshot."""
    mods = []
    children = data.get("data", {}).get("children", [])
    for child in children:
        entry = child if not child.get("data") else child["data"]
        if entry.get("name"):
            mods.append(ModEntry.from_reddit_json(entry, "ia_json", snapshot_url, snapshot_ts))
    return mods


def _parse_mods_from_html(html: str, snapshot_url: str, snapshot_ts: str) -> list[ModEntry]:
    """
    Extract moderator data from Reddit HTML pages.
    Handles both old.reddit (table with <time> tags) and new Reddit (faceplate-date).
    """
    mods = []
    seen = set()

    # Pattern 1: Old Reddit - <time datetime="..."> in table rows
    old_reddit_pattern = r'<tr[^>]*>.*?/user/([^/"]+).*?<time[^>]+datetime="([^"]+)".*?</tr>'

    for match in re.finditer(old_reddit_pattern, html, re.DOTALL | re.IGNORECASE):
        username = match.group(1).strip()
        datetime_str = match.group(2).strip()

        if username and username.lower() not in seen and username.lower() != "reddit":
            seen.add(username.lower())
            try:
                dt = datetime.fromisoformat(datetime_str.replace('+00:00', '+00:00'))
                added_utc = int(dt.timestamp())

                mods.append(ModEntry(
                    username=username,
                    added_utc=added_utc,
                    added_date=datetime_str,
                    source="ia_html_old",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                ))
            except (ValueError, AttributeError) as e:
                log.debug(f"Failed to parse datetime for {username}: {datetime_str} - {e}")
                mods.append(ModEntry(
                    username=username,
                    added_utc=None,
                    added_date=None,
                    source="ia_html_old",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                ))

    # Pattern 2: New Reddit - <faceplate-date ts="..."> with nearby username
    new_reddit_pattern = r'<div[^>]+class="[^"]*tablerow[^"]*"[^>]*>.*?/user/([^/"]+).*?<faceplate-date[^>]+ts="([^"]+)".*?</div>'

    for match in re.finditer(new_reddit_pattern, html, re.DOTALL | re.IGNORECASE):
        username = match.group(1).strip()
        ts_str = match.group(2).strip()

        if username and username.lower() not in seen and username.lower() != "reddit":
            seen.add(username.lower())
            try:
                ts_str_normalized = ts_str.replace('+0000', '+00:00')
                dt = datetime.fromisoformat(ts_str_normalized)
                added_utc = int(dt.timestamp())

                mods.append(ModEntry(
                    username=username,
                    added_utc=added_utc,
                    added_date=dt.isoformat(),
                    source="ia_html_new",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                ))
            except (ValueError, AttributeError) as e:
                log.debug(f"Failed to parse faceplate-date ts for {username}: {ts_str} - {e}")
                mods.append(ModEntry(
                    username=username,
                    added_utc=None,
                    added_date=None,
                    source="ia_html_new",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                ))

    # Pattern 3: Fallback
    if not mods:
        fallback_patterns = [
            r'class="[^"]*moderator[^"]*"[^>]*>\s*<a[^>]+href="/user/([^/"]+)',
            r'/user/([^/"]+)[^>]*"[^>]*>u/\1',
        ]

        for pat in fallback_patterns:
            for m in re.finditer(pat, html, re.IGNORECASE | re.DOTALL):
                username = m.group(1).strip()
                if username and username.lower() not in seen and username.lower() != "reddit":
                    seen.add(username.lower())
                    mods.append(ModEntry(
                        username=username,
                        added_utc=None,
                        added_date=None,
                        source="ia_html_fallback",
                        snapshot_url=snapshot_url,
                        snapshot_timestamp=snapshot_ts,
                    ))

    return mods


async def fetch_ia_snapshot(
        session: aiohttp.ClientSession,
        snapshot: dict,
        semaphore: asyncio.Semaphore,
        rate_limiter: RateLimiter,
        raw_dir: Path,
        subreddit: str,
) -> list[ModEntry]:
    """Fetch one IA snapshot, save raw response, and extract moderator data."""
    ts = snapshot["timestamp"]
    original = snapshot["original"]
    wayback_url = f"{IA_WAYBACK_BASE}/{ts}id_/{original}"
    is_json = "json" in snapshot.get("mimetype", "") or original.endswith(".json")

    # Check if we already have this snapshot
    ext = "json" if is_json else "html"
    raw_file = raw_dir / f"{sanitize_filename(subreddit)}_{ts}.{ext}"

    if raw_file.exists():
        log.debug(f"Using cached snapshot: {raw_file.name}")
        content = raw_file.read_bytes()

        # Parse cached content
        if is_json:
            try:
                data = json.loads(content)
                return _parse_mods_from_json(data, wayback_url, ts)
            except Exception:
                text = content.decode('utf-8', errors='replace')
                return _parse_mods_from_html(text, wayback_url, ts)
        else:
            text = content.decode('utf-8', errors='replace')
            return _parse_mods_from_html(text, wayback_url, ts)

    # Fetch new snapshot
    backoff_time = IA_RETRY_SLEEP

    async with semaphore:
        for attempt in range(1, IA_MAX_RETRIES + 1):
            try:
                await rate_limiter.acquire()
                async with session.get(
                        wayback_url,
                        timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
                        allow_redirects=True,
                ) as resp:
                    # Handle rate limiting
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", RATE_LIMIT_BACKOFF))
                        log.warning(f"⚠️  Rate limited (429) – pausing for {retry_after}s")
                        await rate_limiter.pause(retry_after)
                        continue

                    # Handle Cloudflare blocks
                    if resp.status in (403, 503):
                        is_cloudflare = (
                                'cloudflare' in resp.headers.get('Server', '').lower() or
                                resp.status == 503
                        )

                        if is_cloudflare:
                            backoff = min(CLOUDFLARE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
                            log.warning(
                                f"🛡️  Cloudflare block detected (HTTP {resp.status}) "
                                f"– pausing for {backoff}s (attempt {attempt}/{IA_MAX_RETRIES})"
                            )
                            await rate_limiter.pause(backoff)
                            continue

                    if resp.status != 200:
                        log.debug(f"Snapshot {ts} returned status {resp.status}")
                        return []

                    # Save raw response
                    content = await resp.read()
                    raw_file.write_bytes(content)

                    # Parse content
                    if is_json:
                        try:
                            data = json.loads(content)
                            return _parse_mods_from_json(data, wayback_url, ts)
                        except Exception:
                            text = content.decode('utf-8', errors='replace')
                            return _parse_mods_from_html(text, wayback_url, ts)
                    else:
                        text = content.decode('utf-8', errors='replace')
                        return _parse_mods_from_html(text, wayback_url, ts)

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.debug("IA fetch error %s attempt %d: %s", wayback_url, attempt, exc)
                if attempt < IA_MAX_RETRIES:
                    backoff_time = min(backoff_time * 2, MAX_BACKOFF)
                    await asyncio.sleep(backoff_time)
    return []


async def fetch_ia_mods(
        session: aiohttp.ClientSession,
        subreddit_info: SubredditInfo,
        semaphore: asyncio.Semaphore,
        rate_limiter: RateLimiter,
        raw_dir: Path,
        resume: bool = False,
) -> tuple[list[ModEntry], bool]:
    """
    Fetch IA snapshots as they become available, stopping early if we find
    a moderator within 7 days of subreddit creation.
    Returns (mods, stopped_early).
    """
    urls = [tpl.format(sub=subreddit_info.name) for tpl in IA_URL_TEMPLATES]

    # Determine year range
    from_year = datetime.fromtimestamp(subreddit_info.creation_date, tz=timezone.utc).year
    to_year = datetime.now().year

    # Load progress if resuming
    result, processed_snapshots = load_progress(subreddit_info.name) if resume else (None, set())

    if result is None:
        result = SubredditResult(
            subreddit=subreddit_info.name,
            creation_date=subreddit_info.creation_date
        )

    # If already stopped early, return immediately
    if result.stopped_early:
        log.info(f"r/{subreddit_info.name} – Already completed with early stop")
        return result.moderators, True

    # Create a queue for snapshots sorted by timestamp
    snapshot_queue = asyncio.Queue()
    stop_event = asyncio.Event()

    async def cdx_producer():
        """Fetch CDX data and add snapshots to queue in chronological order."""
        all_snapshots = []

        # Fetch all CDX data in parallel
        cdx_tasks = [
            ia_cdx_snapshots_by_year(session, url, rate_limiter, from_year, to_year)
            for url in urls
        ]
        cdx_results = await asyncio.gather(*cdx_tasks, return_exceptions=True)

        # Flatten results
        for cdx_result in cdx_results:
            if isinstance(cdx_result, list):
                all_snapshots.extend(cdx_result)

        # Deduplicate and sort by timestamp (oldest first)
        seen_ts: set[str] = set()
        unique_snapshots = []
        for snap in sorted(all_snapshots, key=lambda x: x["timestamp"]):
            ts = snap["timestamp"]
            if ts not in seen_ts:
                seen_ts.add(ts)
                # Skip already processed snapshots
                if ts not in processed_snapshots:
                    unique_snapshots.append(snap)

        result.snapshots_total = len(all_snapshots)
        result.snapshots_processed = len(processed_snapshots)

        log.info(
            f"  r/{subreddit_info.name} – {len(unique_snapshots)} new snapshots to fetch "
            f"({result.snapshots_processed}/{result.snapshots_total} already processed)"
        )

        # Add snapshots to queue in order
        for snap in unique_snapshots:
            if stop_event.is_set():
                break
            await snapshot_queue.put(snap)

        # Signal end of snapshots
        await snapshot_queue.put(None)

    async def snapshot_processor():
        """Process snapshots from queue with early stopping."""
        save_interval = 5  # Save progress every N snapshots
        snapshots_since_save = 0

        while not stop_event.is_set():
            snap = await snapshot_queue.get()

            # None signals end of snapshots
            if snap is None:
                break

            ts = snap["timestamp"]

            # Fetch and parse the snapshot
            mods = await fetch_ia_snapshot(session, snap, semaphore, rate_limiter, raw_dir, subreddit_info.name)

            # Mark as processed
            processed_snapshots.add(ts)
            result.snapshots_processed += 1
            snapshots_since_save += 1

            if mods:
                result.merge(mods)

                # Check for early stop condition after each snapshot
                for mod in mods:
                    if mod.added_utc:
                        if mod.added_utc <= subreddit_info.creation_date:
                            log.info(
                                f"  r/{subreddit_info.name} – Early stop: {mod.username} "
                                f"added at/before creation"
                            )
                            result.check_early_stop()
                            stop_event.set()
                            save_progress(subreddit_info.name, result, processed_snapshots)
                            return True

                        days_diff = (mod.added_utc - subreddit_info.creation_date) / (24 * 60 * 60)
                        if days_diff <= 7:
                            log.info(
                                f"  r/{subreddit_info.name} – Early stop: {mod.username} "
                                f"added {days_diff:.1f} days after creation"
                            )
                            result.check_early_stop()
                            stop_event.set()
                            save_progress(subreddit_info.name, result, processed_snapshots)
                            return True

            # Periodic progress saving
            if snapshots_since_save >= save_interval:
                save_progress(subreddit_info.name, result, processed_snapshots)
                snapshots_since_save = 0

            snapshot_queue.task_done()

        # Final save
        save_progress(subreddit_info.name, result, processed_snapshots)
        return False

    # Run producer and processor concurrently
    producer_task = asyncio.create_task(cdx_producer())
    processor_result = await snapshot_processor()

    # Wait for producer to finish
    await producer_task

    return result.moderators, processor_result


# ---------------------------------------------------------------------------
# Main pipeline per subreddit
# ---------------------------------------------------------------------------

async def process_subreddit(
        subreddit_info: SubredditInfo,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        rate_limiter: RateLimiter,
        raw_dir: Path,
        resume: bool = False,
) -> SubredditResult:
    # Check if already completed
    if not resume or not is_completed(subreddit_info.name):
        result = SubredditResult(
            subreddit=subreddit_info.name,
            creation_date=subreddit_info.creation_date
        )

        # Create raw data subdirectory for this subreddit
        safe_name = sanitize_filename(subreddit_info.name)
        sub_raw_dir = raw_dir / safe_name
        sub_raw_dir.mkdir(parents=True, exist_ok=True)

        try:
            ia_mods, stopped_early = await fetch_ia_mods(
                session, subreddit_info, semaphore, rate_limiter, sub_raw_dir, resume
            )
            log.info("r/%s – %d mod records from IA", subreddit_info.name, len(ia_mods))
            result.moderators = ia_mods

            if stopped_early:
                result.check_early_stop()

        except Exception as exc:
            msg = f"IA fetch failed: {exc}"
            log.warning("r/%s – %s", subreddit_info.name, msg)
            result.errors.append(msg)

        log.info("r/%s – final: %d unique moderators", subreddit_info.name, len(result.moderators))

        # Save final result
        save_final_result(result)

        return result
    else:
        # Load completed result
        safe_name = sanitize_filename(subreddit_info.name)

        result_file = RESULTS_DIR / f"{safe_name}.json"
        result_dict = json.loads(result_file.read_text(encoding="utf-8"))
        result = SubredditResult(
            subreddit=result_dict["subreddit"],
            creation_date=result_dict["creation_date"],
            fetched_at=result_dict["fetched_at"],
            moderators=[ModEntry(**m) for m in result_dict["moderators"]],
            errors=result_dict["errors"],
            stopped_early=result_dict.get("stopped_early", False),
            early_stop_reason=result_dict.get("early_stop_reason"),
            snapshots_processed=result_dict.get("snapshots_processed", 0),
            snapshots_total=result_dict.get("snapshots_total", 0),
        )
        log.info(f"r/{subreddit_info.name} – Loaded completed result")
        return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(subreddits: list[SubredditInfo], output_path: Optional[str], concurrency: int, resume: bool):
    # Create directories
    PROGRESS_DIR.mkdir(exist_ok=True)
    RESULTS_DIR.mkdir(exist_ok=True)
    raw_dir = Path("raw_responses")
    raw_dir.mkdir(exist_ok=True)

    # Global rate limiter: 1 request per second
    rate_limiter = RateLimiter(delay=RATE_LIMIT_DELAY)

    ia_semaphore = asyncio.Semaphore(IA_CONCURRENCY)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    ia_connector = aiohttp.TCPConnector(limit=32)

    async with aiohttp.ClientSession(headers=headers, connector=ia_connector) as session:
        sub_semaphore = asyncio.Semaphore(concurrency)

        async def bounded(sub_info):
            async with sub_semaphore:
                return await process_subreddit(sub_info, session, ia_semaphore, rate_limiter, raw_dir, resume)

        tasks = [bounded(sub_info) for sub_info in subreddits]
        results: list[SubredditResult] = []

        for coro in tqdm.as_completed(tasks, total=len(tasks), desc="Subreddits"):
            results.append(await coro)

    # Output combined results
    output_data = [r.to_dict() for r in results]
    json_str = json.dumps(output_data, indent=2, ensure_ascii=False)

    if output_path:
        Path(output_path).write_text(json_str, encoding="utf-8")
        log.info("Results written to %s", output_path)
    else:
        print(json_str)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch Reddit moderator history from Internet Archive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output", "-o",
        help="Output JSON file path (default: stdout)",
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=20,
        help="Max subreddits processed simultaneously (default: 3)",
    )
    parser.add_argument(
        "--resume", "-r",
        action="store_true",
        default=True,
        help="Resume from previous run using saved progress",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=True,
        help="Enable debug logging",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Read subreddits from subredditData.csv
    csv_file = Path("subredditData.csv")
    if not csv_file.exists():
        print("Error: subredditData.csv not found.", file=sys.stderr)
        sys.exit(1)

    subreddits = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                subreddits.append(SubredditInfo(
                    name=row['subreddit_name'].strip(),
                    creation_date=int(row['subreddit_creation_date']),
                    earliest_post=int(row['earliest_post']),
                    earliest_comment=int(row['earliest_comment']),
                ))
            except (KeyError, ValueError) as e:
                log.warning(f"Skipping invalid row: {row} - {e}")
                continue

    if not subreddits:
        print("Error: subredditData.csv contains no valid subreddit data.", file=sys.stderr)
        sys.exit(1)

    log.info("Processing %d subreddit(s)…", len(subreddits))
    asyncio.run(main(subreddits, args.output, args.concurrency, args.resume))