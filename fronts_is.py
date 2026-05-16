#!/usr/bin/env python3
"""
Reddit Front Page Moderator Scraper via Internet Archive
Fetches moderator lists from subreddit front pages (old/new Reddit) using IA snapshots.
If a subreddit is not archived, submits it to IA for future archival.

Usage:
    python mods_frontpage.py --output results_frontpage.json
    python mods_frontpage.py --output results_frontpage.json --resume

Requirements:
    pip install aiohttp python-dotenv tqdm
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

# Internet Archive URLs
IA_CDX_BASE = "https://web.archive.org/cdx/search/cdx"
IA_WAYBACK_BASE = "https://web.archive.org/web"
IA_SAVE_API = "https://web.archive.org/save"
IA_CONCURRENCY = 20
IA_TIMEOUT = 30
IA_MAX_RETRIES = 3
IA_RETRY_SLEEP = 2.0

# Rate limiting: 1 request per second globally
RATE_LIMIT_DELAY = 1.0  # seconds between requests

USER_AGENT = os.getenv("USER_AGENT")
if not USER_AGENT:
    print("Error: USER_AGENT not found in .env file", file=sys.stderr)
    sys.exit(1)

# Front page URLs to check (old and new Reddit)
FRONTPAGE_URL_TEMPLATES = [
    "https://old.reddit.com/r/{sub}/",
    "https://www.reddit.com/r/{sub}/",
]

# Progress tracking directories
PROGRESS_DIR = Path("progress_frontpage")
RESULTS_DIR = Path("results_frontpage")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Global rate limiter to ensure no more than 1 request per second."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Wait until we're allowed to make another request."""
        async with self.lock:
            now = time.time()
            time_since_last = now - self.last_request_time

            if time_since_last < self.delay:
                wait_time = self.delay - time_since_last
                await asyncio.sleep(wait_time)

            self.last_request_time = time.time()


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
    first_seen_utc: Optional[int]  # Timestamp of earliest snapshot where mod appeared
    first_seen_date: Optional[str]
    source: str = "frontpage"
    snapshot_url: Optional[str] = None
    snapshot_timestamp: Optional[str] = None
    note: Optional[str] = None  # "added_before_{date}" to indicate uncertainty

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SubredditResult:
    subreddit: str
    creation_date: int
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    moderators: list[ModEntry] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    snapshots_processed: int = 0
    snapshots_total: int = 0
    submitted_for_archival: list[str] = field(default_factory=list)

    def merge(self, other_mods: list[ModEntry]) -> None:
        """Add mods, keeping the earliest first_seen for each username."""
        existing = {m.username.lower(): m for m in self.moderators}
        for m in other_mods:
            key = m.username.lower()
            if key not in existing:
                existing[key] = m
            elif m.first_seen_utc and (
                    not existing[key].first_seen_utc or m.first_seen_utc < existing[key].first_seen_utc):
                existing[key] = m
        self.moderators = sorted(existing.values(), key=lambda x: x.first_seen_utc or float('inf'))

    def to_dict(self) -> dict:
        return {
            "subreddit": self.subreddit,
            "creation_date": self.creation_date,
            "creation_date_iso": datetime.fromtimestamp(self.creation_date, tz=timezone.utc).isoformat(),
            "fetched_at": self.fetched_at,
            "moderator_count": len(self.moderators),
            "moderators": [m.to_dict() for m in self.moderators],
            "errors": self.errors,
            "snapshots_processed": self.snapshots_processed,
            "snapshots_total": self.snapshots_total,
            "submitted_for_archival": self.submitted_for_archival,
        }


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    """Sanitize a string to be safe for use as a filename/directory name."""
    invalid_chars = r'[<>:"/\\|?*\x00-\x1f\x7f]'
    sanitized = re.sub(invalid_chars, '_', name)
    sanitized = sanitized.strip('. ')
    if not sanitized:
        sanitized = 'unnamed'
    return sanitized


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

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
        result_dict = progress_data["result"]
        result = SubredditResult(
            subreddit=result_dict["subreddit"],
            creation_date=result_dict["creation_date"],
            fetched_at=result_dict["fetched_at"],
            moderators=[ModEntry(**m) for m in result_dict["moderators"]],
            errors=result_dict["errors"],
            snapshots_processed=result_dict.get("snapshots_processed", 0),
            snapshots_total=result_dict.get("snapshots_total", 0),
            submitted_for_archival=result_dict.get("submitted_for_archival", []),
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

async def ia_cdx_snapshots_all(
        session: aiohttp.ClientSession,
        url: str,
        rate_limiter: RateLimiter,
        from_ts: Optional[str] = None,
) -> list[dict]:
    """
    Query CDX API for all available snapshots of a URL.
    Returns a list of {timestamp, original, statuscode, mimetype} dicts.
    """
    params = {
        "output": "json",
        "url": url,
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": "statuscode:200",
    }

    if from_ts:
        params["from"] = from_ts

    for attempt in range(1, IA_MAX_RETRIES + 1):
        try:
            await rate_limiter.acquire()
            async with session.get(
                    IA_CDX_BASE,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return []
                rows = await resp.json(content_type=None)
                if rows and len(rows) >= 2:
                    keys = rows[0]
                    return [dict(zip(keys, row)) for row in rows[1:]]
                return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.debug("CDX error for %s attempt %d: %s", url, attempt, exc)
            if attempt < IA_MAX_RETRIES:
                await asyncio.sleep(IA_RETRY_SLEEP * attempt)
    return []


async def submit_to_ia(session: aiohttp.ClientSession, url: str, rate_limiter: RateLimiter) -> bool:
    """
    Submit a URL to Internet Archive's Save Page Now API.
    Returns True if successfully submitted.
    """
    save_url = f"{IA_SAVE_API}/{url}"

    try:
        await rate_limiter.acquire()
        async with session.get(
                save_url,
                timeout=aiohttp.ClientTimeout(total=60),
                allow_redirects=True,
        ) as resp:
            if resp.status in (200, 302, 403):  # 403 can mean already in queue
                log.info(f"Submitted to IA: {url}")
                return True
            else:
                log.warning(f"IA submission failed for {url}: status {resp.status}")
                return False
    except Exception as exc:
        log.warning(f"IA submission error for {url}: {exc}")
        return False


def _parse_mods_from_frontpage_html(html: str, snapshot_url: str, snapshot_ts: str) -> list[ModEntry]:
    """
    Extract moderator usernames from subreddit front page HTML.
    Handles both old and new Reddit sidebar formats.
    No timestamp info is available, so we track when we first saw them.
    """
    mods = []
    seen = set()

    # Convert timestamp to datetime for note
    try:
        dt = datetime.strptime(snapshot_ts, "%Y%m%d%H%M%S")
        snapshot_utc = int(dt.replace(tzinfo=timezone.utc).timestamp())
        snapshot_date = dt.strftime("%Y-%m-%d")
    except Exception:
        snapshot_utc = None
        snapshot_date = snapshot_ts

    # Pattern 1: Old Reddit sidebar
    # <div class="sidecontentbox "><div class="title"><h1>MODERATORS</h1></div><ul class="content">
    # <li><a href="https://old.reddit.com/user/PlantyHamchuk" class="author may-blank id-t2_5mi4p">PlantyHamchuk</a>
    old_reddit_pattern = r'<div[^>]+class="[^"]*sidecontentbox[^"]*"[^>]*>.*?<h1>MODERATORS</h1>.*?<ul[^>]*>(.*?)</ul>'
    old_reddit_mod_pattern = r'<a[^>]+href="[^"]*?/user/([^/"]+)"[^>]*class="[^"]*author[^"]*"'

    for sidebar_match in re.finditer(old_reddit_pattern, html, re.DOTALL | re.IGNORECASE):
        sidebar_content = sidebar_match.group(1)
        for mod_match in re.finditer(old_reddit_mod_pattern, sidebar_content, re.IGNORECASE):
            username = mod_match.group(1).strip()
            if username and username.lower() not in seen and username.lower() not in ('automoderator', 'reddit'):
                seen.add(username.lower())
                mods.append(ModEntry(
                    username=username,
                    first_seen_utc=snapshot_utc,
                    first_seen_date=snapshot_date,
                    source="frontpage_old",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                    note=f"added_before_{snapshot_date}",
                ))

    # Pattern 2: New Reddit sidebar
    # <div class="px-md text-neutral-content-weak">
    # <h2 class="uppercase text-12 font-semibold m-0 "><div class="i18n-translatable-text">Moderators</div></h2>
    # <a class="text-neutral-content inline-flex items-center whitespace-nowrap" target="_blank" href="/user/PlantyHamchuk/">
    new_reddit_pattern = r'<div[^>]*>.*?<h2[^>]*>.*?Moderators.*?</h2>.*?<ul[^>]*>(.*?)</ul>'
    new_reddit_mod_pattern = r'<a[^>]+href="/user/([^/"]+)/"[^>]*>'

    for sidebar_match in re.finditer(new_reddit_pattern, html, re.DOTALL | re.IGNORECASE):
        sidebar_content = sidebar_match.group(1)
        for mod_match in re.finditer(new_reddit_mod_pattern, sidebar_content, re.IGNORECASE):
            username = mod_match.group(1).strip()
            if username and username.lower() not in seen and username.lower() not in ('automoderator', 'reddit'):
                seen.add(username.lower())
                mods.append(ModEntry(
                    username=username,
                    first_seen_utc=snapshot_utc,
                    first_seen_date=snapshot_date,
                    source="frontpage_new",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                    note=f"added_before_{snapshot_date}",
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

    # Check if we already have this snapshot
    raw_file = raw_dir / f"{sanitize_filename(subreddit)}_{ts}.html"

    if raw_file.exists():
        log.debug(f"Using cached snapshot: {raw_file.name}")
        content = raw_file.read_text(encoding='utf-8', errors='replace')
        return _parse_mods_from_frontpage_html(content, wayback_url, ts)

    # Fetch new snapshot
    async with semaphore:
        for attempt in range(1, IA_MAX_RETRIES + 1):
            try:
                await rate_limiter.acquire()
                async with session.get(
                        wayback_url,
                        timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
                        allow_redirects=True,
                ) as resp:
                    if resp.status != 200:
                        log.debug(f"Snapshot {ts} returned status {resp.status}")
                        return []

                    # Save raw response
                    content = await resp.text(errors='replace')
                    raw_file.write_text(content, encoding='utf-8')

                    return _parse_mods_from_frontpage_html(content, wayback_url, ts)

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.debug("IA fetch error %s attempt %d: %s", wayback_url, attempt, exc)
                if attempt < IA_MAX_RETRIES:
                    await asyncio.sleep(IA_RETRY_SLEEP * attempt)
    return []


async def fetch_frontpage_mods(
        session: aiohttp.ClientSession,
        subreddit_info: SubredditInfo,
        semaphore: asyncio.Semaphore,
        rate_limiter: RateLimiter,
        raw_dir: Path,
        resume: bool = False,
) -> SubredditResult:
    """
    Fetch front page snapshots from IA, submit missing URLs for archival.
    Returns SubredditResult with moderators found.
    """
    urls = [tpl.format(sub=subreddit_info.name) for tpl in FRONTPAGE_URL_TEMPLATES]

    # Load progress if resuming
    result, processed_snapshots = load_progress(subreddit_info.name) if resume else (None, set())

    if result is None:
        result = SubredditResult(
            subreddit=subreddit_info.name,
            creation_date=subreddit_info.creation_date
        )

    # Fetch CDX data for all URLs
    all_snapshots = []
    missing_urls = []

    for url in urls:
        # Use creation date as starting point
        from_ts = datetime.fromtimestamp(subreddit_info.creation_date, tz=timezone.utc).strftime("%Y%m%d")
        snapshots = await ia_cdx_snapshots_all(session, url, rate_limiter, from_ts=from_ts)

        if snapshots:
            all_snapshots.extend(snapshots)
        else:
            missing_urls.append(url)

    # Submit missing URLs to IA
    if missing_urls:
        log.info(f"r/{subreddit_info.name} – No snapshots found for {len(missing_urls)} URLs, submitting to IA")
        for url in missing_urls:
            if await submit_to_ia(session, url, rate_limiter):
                result.submitted_for_archival.append(url)

    # Deduplicate and sort snapshots by timestamp (oldest first)
    seen_ts: set[str] = set()
    unique_snapshots = []
    for snap in sorted(all_snapshots, key=lambda x: x["timestamp"]):
        ts = snap["timestamp"]
        if ts not in seen_ts and ts not in processed_snapshots:
            seen_ts.add(ts)
            unique_snapshots.append(snap)

    result.snapshots_total = len(all_snapshots)
    result.snapshots_processed = len(processed_snapshots)

    log.info(
        f"r/{subreddit_info.name} – {len(unique_snapshots)} new snapshots to fetch "
        f"({result.snapshots_processed}/{result.snapshots_total} already processed)"
    )

    # Process snapshots
    save_interval = 5
    snapshots_since_save = 0

    for snap in unique_snapshots:
        ts = snap["timestamp"]

        # Fetch and parse the snapshot
        mods = await fetch_ia_snapshot(session, snap, semaphore, rate_limiter, raw_dir, subreddit_info.name)

        # Mark as processed
        processed_snapshots.add(ts)
        result.snapshots_processed += 1
        snapshots_since_save += 1

        if mods:
            result.merge(mods)

        # Periodic progress saving
        if snapshots_since_save >= save_interval:
            save_progress(subreddit_info.name, result, processed_snapshots)
            snapshots_since_save = 0

    # Final save
    save_progress(subreddit_info.name, result, processed_snapshots)

    return result


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
    """Process a single subreddit."""
    # Check if already completed
    if resume and is_completed(subreddit_info.name):
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
            snapshots_processed=result_dict.get("snapshots_processed", 0),
            snapshots_total=result_dict.get("snapshots_total", 0),
            submitted_for_archival=result_dict.get("submitted_for_archival", []),
        )
        log.info(f"r/{subreddit_info.name} – Loaded completed result")
        return result

    # Create raw data subdirectory for this subreddit
    safe_name = sanitize_filename(subreddit_info.name)
    sub_raw_dir = raw_dir / safe_name
    sub_raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        result = await fetch_frontpage_mods(
            session, subreddit_info, semaphore, rate_limiter, sub_raw_dir, resume
        )
        log.info("r/%s – %d moderators found from front pages", subreddit_info.name, len(result.moderators))

    except Exception as exc:
        msg = f"Front page fetch failed: {exc}"
        log.warning("r/%s – %s", subreddit_info.name, msg)
        result = SubredditResult(
            subreddit=subreddit_info.name,
            creation_date=subreddit_info.creation_date
        )
        result.errors.append(msg)

    log.info("r/%s – final: %d unique moderators", subreddit_info.name, len(result.moderators))

    # Save final result
    save_final_result(result)

    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(subreddits: list[SubredditInfo], output_path: Optional[str], concurrency: int, resume: bool):
    # Create directories
    PROGRESS_DIR.mkdir(exist_ok=True)
    RESULTS_DIR.mkdir(exist_ok=True)
    raw_dir = Path("raw_responses_frontpage")
    raw_dir.mkdir(exist_ok=True)

    # Global rate limiter: 1 request per second
    rate_limiter = RateLimiter(delay=RATE_LIMIT_DELAY)

    ia_semaphore = asyncio.Semaphore(IA_CONCURRENCY)
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
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
        description="Fetch Reddit moderators from front page snapshots via Internet Archive",
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
        default=5,
        help="Max subreddits processed simultaneously (default: 5)",
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