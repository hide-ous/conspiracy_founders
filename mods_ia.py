#!/usr/bin/env python3
"""
Internet Archive Moderator History Scraper
Fetches moderator lists from Internet Archive (Wayback Machine) snapshots.

Usage:
    python archive_scraper.py <subreddit> [<subreddit> ...]
    python archive_scraper.py --file subreddits.txt
    python archive_scraper.py pics gaming --output results.json

Requirements:
    pip install aiohttp aiofiles tqdm
"""

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from tqdm.asyncio import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Internet Archive CDX API
IA_CDX_BASE         = "https://web.archive.org/cdx/search/cdx"
IA_WAYBACK_BASE     = "https://web.archive.org/web"
IA_CONCURRENCY      = 8            # parallel IA fetches (generous, IA allows it)
IA_TIMEOUT          = 30
IA_MAX_RETRIES      = 3
IA_RETRY_SLEEP      = 2.0

USER_AGENT = (
    "ModHistoryScraper/1.0 (research tool; "
    "https://github.com/example/mod-history; contact@example.com)"
)

# IA URL templates to snapshot-search
IA_URL_TEMPLATES = [
    "https://www.reddit.com/r/{sub}",
    "https://old.reddit.com/r/{sub}",
    "https://old.reddit.com/r/{sub}/about/moderators/.json",
    "https://old.reddit.com/r/{sub}/about/moderators",
    "https://www.reddit.com/mod/{sub}/moderators",
    "https://www.reddit.com/r/{sub}/about/moderators",
]

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
class ModEntry:
    username: str
    added_utc: Optional[int]        # Unix timestamp if known, else None
    added_date: Optional[str]       # ISO-8601 string
    permissions: list[str] = field(default_factory=list)
    source: str = "unknown"         # "ia_json" | "ia_html"
    snapshot_url: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_reddit_json(entry: dict, source: str = "ia_json") -> "ModEntry":
        ts = entry.get("date")
        return ModEntry(
            username=entry.get("name", ""),
            added_utc=ts,
            added_date=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
            permissions=entry.get("mod_permissions", []),
            source=source,
        )


@dataclass
class SubredditResult:
    subreddit: str
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    moderators: list[ModEntry] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def merge(self, other_mods: list[ModEntry]) -> None:
        """Add mods not already present (keyed by username, prefer entries with dates)."""
        existing = {m.username.lower(): m for m in self.moderators}
        for m in other_mods:
            key = m.username.lower()
            if key not in existing:
                existing[key] = m
            elif m.added_utc and not existing[key].added_utc:
                existing[key] = m          # upgrade: now has a date
        self.moderators = sorted(existing.values(), key=lambda x: x.added_utc or 0)

    def to_dict(self) -> dict:
        return {
            "subreddit": self.subreddit,
            "fetched_at": self.fetched_at,
            "moderator_count": len(self.moderators),
            "moderators": [m.to_dict() for m in self.moderators],
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Internet Archive helpers
# ---------------------------------------------------------------------------

async def ia_cdx_snapshots(
    session: aiohttp.ClientSession,
    url: str,
    *,
    limit: int = 50,
    from_date: str = "20050101",
    to_date: Optional[str] = None,
) -> list[dict]:
    """
    Query the CDX API for archived snapshots of `url`.
    Returns a list of {timestamp, original, statuscode, mimetype} dicts.
    """
    params = {
        "output": "json",
        "url": url,
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": "statuscode:200",
        "collapse": "timestamp:8",   # one snapshot per day
        "limit": str(limit),
        "from": from_date,
    }
    if to_date:
        params["to"] = to_date

    for attempt in range(1, IA_MAX_RETRIES + 1):
        try:
            async with session.get(
                IA_CDX_BASE,
                params=params,
                timeout=aiohttp.ClientTimeout(total=IA_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return []
                rows = await resp.json(content_type=None)
                if not rows or len(rows) < 2:
                    return []
                keys = rows[0]
                return [dict(zip(keys, row)) for row in rows[1:]]
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.debug("CDX error for %s attempt %d: %s", url, attempt, exc)
            if attempt < IA_MAX_RETRIES:
                await asyncio.sleep(IA_RETRY_SLEEP * attempt)
    return []


def _parse_mods_from_json(data: dict, snapshot_url: str) -> list[ModEntry]:
    """Extract mod entries from a /about/moderators.json snapshot."""
    mods = []
    children = data.get("data", {}).get("children", [])
    for child in children:
        entry = child if not child.get("data") else child["data"]
        if entry.get("name"):
            mods.append(ModEntry.from_reddit_json(entry, source="ia_json"))
            mods[-1].snapshot_url = snapshot_url
    return mods


def _parse_mods_from_html(html: str, snapshot_url: str) -> list[ModEntry]:
    """
    Lightweight regex-based extraction from old.reddit HTML pages.
    Looks for mod username links in the sidebar or moderators page.
    """
    mods = []
    # Pattern matches <a href="/user/USERNAME/...">USERNAME</a> in mod lists
    patterns = [
        r'class="[^"]*moderator[^"]*"[^>]*>\s*<a[^>]+href="/user/([^/"]+)',
        r'<li[^>]*class="[^"]*even[^"]*"[^>]*>.*?/user/([^/"]+)',
        r'/user/([^/"]+)/?\s*</a>\s*</li>',
        r'"author":\s*"([^"]+)"',          # JSON-ish fragments in HTML
    ]
    seen = set()
    for pat in patterns:
        for m in re.finditer(pat, html, re.IGNORECASE | re.DOTALL):
            username = m.group(1).strip()
            if username and username.lower() not in seen and username != "reddit":
                seen.add(username.lower())
                mods.append(ModEntry(
                    username=username,
                    added_utc=None,
                    added_date=None,
                    source="ia_html",
                    snapshot_url=snapshot_url,
                ))
    return mods


async def fetch_ia_snapshot(
    session: aiohttp.ClientSession,
    snapshot: dict,
    semaphore: asyncio.Semaphore,
) -> list[ModEntry]:
    """Fetch one IA snapshot and extract moderator data from it."""
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
                    if is_json:
                        try:
                            data = await resp.json(content_type=None)
                            return _parse_mods_from_json(data, wayback_url)
                        except Exception:
                            text = await resp.text(errors="replace")
                            return _parse_mods_from_html(text, wayback_url)
                    else:
                        text = await resp.text(errors="replace")
                        return _parse_mods_from_html(text, wayback_url)

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.debug("IA fetch error %s attempt %d: %s", wayback_url, attempt, exc)
                if attempt < IA_MAX_RETRIES:
                    await asyncio.sleep(IA_RETRY_SLEEP * attempt)
    return []


async def fetch_ia_mods(
    session: aiohttp.ClientSession,
    subreddit: str,
    semaphore: asyncio.Semaphore,
    snapshots_per_url: int = 20,
) -> list[ModEntry]:
    """
    For each URL template, fetch CDX snapshots in parallel, then
    fetch all those snapshots concurrently (bounded by semaphore).
    """
    urls = [tpl.format(sub=subreddit) for tpl in IA_URL_TEMPLATES]

    # Step 1: CDX lookups (parallel, IA CDX is lightweight)
    cdx_tasks = [
        ia_cdx_snapshots(session, url, limit=snapshots_per_url)
        for url in urls
    ]
    cdx_results = await asyncio.gather(*cdx_tasks, return_exceptions=True)

    # Flatten, deduplicate by timestamp+url
    all_snapshots: list[dict] = []
    seen_ts_url: set[tuple] = set()
    for result in cdx_results:
        if isinstance(result, list):
            for snap in result:
                key = (snap["timestamp"], snap["original"])
                if key not in seen_ts_url:
                    seen_ts_url.add(key)
                    all_snapshots.append(snap)

    if not all_snapshots:
        return []

    log.info("  r/%s – %d IA snapshots to fetch", subreddit, len(all_snapshots))

    # Step 2: Fetch all snapshots concurrently (bounded by semaphore)
    fetch_tasks = [
        fetch_ia_snapshot(session, snap, semaphore)
        for snap in all_snapshots
    ]
    snapshot_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    all_mods: list[ModEntry] = []
    for r in snapshot_results:
        if isinstance(r, list):
            all_mods.extend(r)

    return all_mods


# ---------------------------------------------------------------------------
# Main pipeline per subreddit
# ---------------------------------------------------------------------------

async def process_subreddit(
    subreddit: str,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> SubredditResult:
    result = SubredditResult(subreddit=subreddit)
    sub = subreddit.lstrip("r/").strip()

    # --- Internet Archive fetch (parallelized) ---
    try:
        ia_mods = await fetch_ia_mods(session, sub, semaphore)
        log.info("r/%s – %d mod records from IA", sub, len(ia_mods))
        result.merge(ia_mods)
    except Exception as exc:
        msg = f"IA fetch failed: {exc}"
        log.warning("r/%s – %s", sub, msg)
        result.errors.append(msg)

    log.info("r/%s – final: %d unique moderators", sub, len(result.moderators))
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(subreddits: list[str], output_path: Optional[str], concurrency: int):
    ia_semaphore = asyncio.Semaphore(IA_CONCURRENCY)

    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    ia_connector = aiohttp.TCPConnector(limit=32)  # IA: can handle more

    async with aiohttp.ClientSession(headers=headers, connector=ia_connector) as session:
        # Process subreddits with a concurrency cap to avoid thundering herd
        sub_semaphore = asyncio.Semaphore(concurrency)

        async def bounded(sub):
            async with sub_semaphore:
                return await process_subreddit(sub, session, ia_semaphore)

        tasks = [bounded(sub) for sub in subreddits]
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


# ... existing code ...

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

    # Read subreddits from subreddits.txt
    subreddit_file = Path("subreddits.txt")
    if not subreddit_file.exists():
        print("Error: subreddits.txt not found.", file=sys.stderr)
        sys.exit(1)

    lines = subreddit_file.read_text().splitlines()
    subreddits = [l.strip().lstrip("r/") for l in lines if l.strip() and not l.startswith("#")]

    if not subreddits:
        print("Error: subreddits.txt is empty or contains no valid subreddit names.", file=sys.stderr)
        sys.exit(1)

    # Deduplicate, preserve order
    seen = set()
    unique_subs = []
    for s in subreddits:
        k = s.lower()
        if k not in seen:
            seen.add(k)
            unique_subs.append(s)

    log.info("Processing %d subreddit(s)…", len(unique_subs))
    asyncio.run(main(unique_subs, args.output, args.concurrency))


