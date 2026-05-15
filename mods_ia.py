#!/usr/bin/env python3
"""
Internet Archive Moderator History Scraper
Fetches moderator lists from Internet Archive (Wayback Machine) snapshots.

Usage:
    python archive_scraper.py --output results.json

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

USER_AGENT = os.getenv("USER_AGENT")
if not USER_AGENT:
    print("Error: USER_AGENT not found in .env file", file=sys.stderr)
    sys.exit(1)

# IA URL templates to snapshot-search
IA_URL_TEMPLATES = [
    "https://old.reddit.com/r/{sub}/about/moderators",
    "https://old.reddit.com/r/{sub}/about/moderators/.json",
    "https://www.reddit.com/r/{sub}/about/moderators",
]

# Early stopping: 7 days in seconds
EARLY_STOP_THRESHOLD = 7 * 24 * 60 * 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


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
        }


# ---------------------------------------------------------------------------
# Internet Archive helpers
# ---------------------------------------------------------------------------

async def ia_cdx_snapshots_by_year(
        session: aiohttp.ClientSession,
        url: str,
        from_year: int = 2005,
        to_year: Optional[int] = None,
) -> list[dict]:
    """
    Query CDX API for one snapshot per year.
    Returns a list of {timestamp, original, statuscode, mimetype} dicts.
    """
    if to_year is None:
        to_year = datetime.now().year

    all_snapshots = []

    for year in range(from_year, to_year + 1):
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

        for attempt in range(1, IA_MAX_RETRIES + 1):
            try:
                async with session.get(
                        IA_CDX_BASE,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
                ) as resp:
                    if resp.status != 200:
                        break
                    rows = await resp.json(content_type=None)
                    if rows and len(rows) >= 2:
                        keys = rows[0]
                        snapshot = dict(zip(keys, rows[1]))
                        all_snapshots.append(snapshot)
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.debug("CDX error for %s year %d attempt %d: %s", url, year, attempt, exc)
                if attempt < IA_MAX_RETRIES:
                    await asyncio.sleep(IA_RETRY_SLEEP * attempt)

    return all_snapshots


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
                # Parse ISO-8601 datetime
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
                # Fall back to entry without date
                mods.append(ModEntry(
                    username=username,
                    added_utc=None,
                    added_date=None,
                    source="ia_html_old",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                ))

    # Pattern 2: New Reddit - <faceplate-date ts="..."> with nearby username
    # Look for blocks containing both username and faceplate-date
    new_reddit_pattern = r'<div[^>]+class="[^"]*tablerow[^"]*"[^>]*>.*?/user/([^/"]+).*?<faceplate-date[^>]+ts="([^"]+)".*?</div>'

    for match in re.finditer(new_reddit_pattern, html, re.DOTALL | re.IGNORECASE):
        username = match.group(1).strip()
        ts_str = match.group(2).strip()

        if username and username.lower() not in seen and username.lower() != "reddit":
            seen.add(username.lower())
            try:
                # Parse timestamp string - format: "2016-03-05T21:36:06.614000+0000"
                # Handle the +0000 timezone format
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
                # Fall back to entry without date
                mods.append(ModEntry(
                    username=username,
                    added_utc=None,
                    added_date=None,
                    source="ia_html_new",
                    snapshot_url=snapshot_url,
                    snapshot_timestamp=snapshot_ts,
                ))

    # Pattern 3: Fallback - just find usernames in moderator contexts (without dates)
    if not mods:
        fallback_patterns = [
            r'class="[^"]*moderator[^"]*"[^>]*>\s*<a[^>]+href="/user/([^/"]+)',
            r'/user/([^/"]+)[^>]*"[^>]*>u/\1',  # Match u/username links
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
        raw_dir: Path,
        subreddit: str,
) -> list[ModEntry]:
    """Fetch one IA snapshot, save raw response, and extract moderator data."""
    ts = snapshot["timestamp"]
    original = snapshot["original"]
    wayback_url = f"{IA_WAYBACK_BASE}/{ts}id_/{original}"
    is_json = "json" in snapshot.get("mimetype", "") or original.endswith(".json")

    async with semaphore:
        for attempt in range(1, IA_MAX_RETRIES + 1):
            try:
                async with session.get(
                        wayback_url,
                        timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
                        allow_redirects=True,
                ) as resp:
                    if resp.status != 200:
                        return []

                    # Save raw response
                    content = await resp.read()
                    ext = "json" if is_json else "html"
                    raw_file = raw_dir / f"{subreddit}_{ts}.{ext}"
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
                    await asyncio.sleep(IA_RETRY_SLEEP * attempt)
    return []


async def fetch_ia_mods(
        session: aiohttp.ClientSession,
        subreddit_info: SubredditInfo,
        semaphore: asyncio.Semaphore,
        raw_dir: Path,
) -> tuple[list[ModEntry], bool]:
    """
    Fetch IA snapshots one year at a time, stopping early if we find
    a moderator within 7 days of subreddit creation.
    Returns (mods, stopped_early).
    """
    urls = [tpl.format(sub=subreddit_info.name) for tpl in IA_URL_TEMPLATES]

    # Determine year range
    from_year = datetime.fromtimestamp(subreddit_info.creation_date, tz=timezone.utc).year
    to_year = datetime.now().year

    all_mods: list[ModEntry] = []

    # Step 1: Get snapshots by year for all URLs
    cdx_tasks = [
        ia_cdx_snapshots_by_year(session, url, from_year, to_year)
        for url in urls
    ]
    cdx_results = await asyncio.gather(*cdx_tasks, return_exceptions=True)

    # Flatten and sort by timestamp (oldest first)
    all_snapshots: list[dict] = []
    for result in cdx_results:
        if isinstance(result, list):
            all_snapshots.extend(result)

    # Deduplicate by timestamp
    seen_ts: set[str] = set()
    unique_snapshots = []
    for snap in sorted(all_snapshots, key=lambda x: x["timestamp"]):
        if snap["timestamp"] not in seen_ts:
            seen_ts.add(snap["timestamp"])
            unique_snapshots.append(snap)

    if not unique_snapshots:
        return [], False

    log.info("  r/%s – %d IA snapshots to fetch", subreddit_info.name, len(unique_snapshots))

    # Step 2: Fetch snapshots one at a time (in chronological order) with early stopping
    for snap in unique_snapshots:
        mods = await fetch_ia_snapshot(session, snap, semaphore, raw_dir, subreddit_info.name)

        if mods:
            all_mods.extend(mods)

            # Check for early stop condition
            for mod in mods:
                if mod.added_utc:
                    if mod.added_utc <= subreddit_info.creation_date:
                        log.info(f"  r/{subreddit_info.name} – Early stop: {mod.username} added at/before creation")
                        return all_mods, True

                    days_diff = (mod.added_utc - subreddit_info.creation_date) / (24 * 60 * 60)
                    if days_diff <= 7:
                        log.info(
                            f"  r/{subreddit_info.name} – Early stop: {mod.username} added {days_diff:.1f} days after creation")
                        return all_mods, True

    return all_mods, False


# ---------------------------------------------------------------------------
# Main pipeline per subreddit
# ---------------------------------------------------------------------------

async def process_subreddit(
        subreddit_info: SubredditInfo,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        raw_dir: Path,
) -> SubredditResult:
    result = SubredditResult(
        subreddit=subreddit_info.name,
        creation_date=subreddit_info.creation_date
    )

    # Create raw data subdirectory for this subreddit
    sub_raw_dir = raw_dir / subreddit_info.name
    sub_raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        ia_mods, stopped_early = await fetch_ia_mods(session, subreddit_info, semaphore, sub_raw_dir)
        log.info("r/%s – %d mod records from IA", subreddit_info.name, len(ia_mods))
        result.merge(ia_mods)

        if stopped_early:
            result.check_early_stop()

    except Exception as exc:
        msg = f"IA fetch failed: {exc}"
        log.warning("r/%s – %s", subreddit_info.name, msg)
        result.errors.append(msg)

    log.info("r/%s – final: %d unique moderators", subreddit_info.name, len(result.moderators))
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(subreddits: list[SubredditInfo], output_path: Optional[str], concurrency: int):
    ia_semaphore = asyncio.Semaphore(IA_CONCURRENCY)

    # Create raw data directory
    raw_dir = Path("raw_responses")
    raw_dir.mkdir(exist_ok=True)

    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    ia_connector = aiohttp.TCPConnector(limit=32)

    async with aiohttp.ClientSession(headers=headers, connector=ia_connector) as session:
        sub_semaphore = asyncio.Semaphore(concurrency)

        async def bounded(sub_info):
            async with sub_semaphore:
                return await process_subreddit(sub_info, session, ia_semaphore, raw_dir)

        tasks = [bounded(sub_info) for sub_info in subreddits]
        results: list[SubredditResult] = []

        for coro in tqdm.as_completed(tasks, total=len(tasks), desc="Subreddits"):
            results.append(await coro)

    # Output
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
        default=3,
        help="Max subreddits processed simultaneously (default: 3)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
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
    asyncio.run(main(subreddits, args.output, args.concurrency))