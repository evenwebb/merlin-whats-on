#!/usr/bin/env python3
"""
Merlin Cinemas Cornwall what's-on scraper.

Scrapes current listings across Merlin Cornwall cinemas, optionally enriches
films with TMDb data, writes whats_on_data.json and regenerates docs/index.html
on every run. Commits (e.g. in CI) are driven by fingerprint change.
"""
import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HTTP_TIMEOUT = 60
HTTP_RETRIES = 3
HTTP_RETRY_DELAY = 1
HTTP_RETRY_MULTIPLIER = 2
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
)

MERLIN_CINEMAS = {
    "bodmin": {
        "name": "Capitol Cinema, Bodmin",
        "url": "https://bodmin.merlincinemas.co.uk/?forcechoice=true",
    },
    "helston": {
        "name": "Flora Cinema, Helston",
        "url": "https://helston.merlincinemas.co.uk/?forcechoice=true",
    },
    "falmouth": {
        "name": "Phoenix Cinema, Falmouth",
        "url": "https://falmouth.merlincinemas.co.uk/?forcechoice=true",
    },
    "redruth": {
        "name": "Regal Cinema & Theatre, Redruth",
        "url": "https://redruth.merlincinemas.co.uk/?forcechoice=true",
    },
    "st-ives": {
        "name": "Royal Cinema, St. Ives",
        "url": "https://st-ives.merlincinemas.co.uk/?forcechoice=true",
    },
    "penzance": {
        "name": "Savoy Cinema, Penzance",
        "url": "https://penzance.merlincinemas.co.uk/?forcechoice=true",
    },
    "ritz": {
        "name": "The Ritz, Penzance",
        "url": "https://ritz.merlincinemas.co.uk/?forcechoice=true",
    },
}

DATA_FILE = "whats_on_data.json"
FINGERPRINT_FILE = ".whats_on_fingerprint"
TMDB_CACHE_FILE = ".tmdb_cache.json"
MERLIN_DETAIL_CACHE_FILE = ".merlin_detail_cache.json"
CINEMA_FAILURE_STATE_FILE = ".cinema_failure_state.json"
SITE_DIR = "docs"  # GitHub Pages: only /(root) and /docs are offered; use Deploy from branch → /docs
POSTERS_DIR = "docs/posters"
CERTS_DIR = "docs/certs"
CERT_IMAGES = {"U": "cert-u.png", "PG": "cert-pg.png", "12A": "cert-12a.png", "15": "cert-15.png", "18": "cert-18.png"}
TMDB_CACHE_DAYS = 30
TMDB_DELAY_SEC = float(os.environ.get("TMDB_DELAY_SEC", "0"))
TMDB_EMPTY_CACHE_TTL_DAYS = 7
MERLIN_DETAIL_CACHE_DAYS = 21
POSTER_PLACEHOLDER_REL = "posters/placeholder.svg"
NOW_SHOWING_THRESHOLD_DAYS = 7
SHOWTIMES_MAX_DAYS_PER_FILM = 10
SHOWTIMES_MAX_SLOTS_PER_FILM = 40
TMDB_REQUEST_RETRIES = 3
HEALTHCHECK_DEFAULT_ENFORCE = "1" if os.environ.get("GITHUB_ACTIONS", "").lower() == "true" else "0"
MAX_CONSECUTIVE_CINEMA_FAILURES_DEFAULT = 2

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
HTTP_SESSION = requests.Session()

# BBFC rating pattern in titles: (15), (12A), (PG), (18), (U), (R18), (with subtitles) etc.
RATING_PATTERN = re.compile(r"\((\d+A?|U|PG|R18)\)", re.IGNORECASE)
SUBTITLE_SUFFIX = re.compile(r"\s*\(with subtitles\)\s*$", re.IGNORECASE)
AUTISM_FRIENDLY_SUFFIX = re.compile(r"\s+autism\s+friendly\s+screening\s*$", re.IGNORECASE)
# Format suffix: " - HFR 3D" (high frame rate 3D) is not part of the movie name
FORMAT_SUFFIX = re.compile(r"\s*-\s*HFR\s*3D\s*$", re.IGNORECASE)
MERLIN_DATE_SUFFIX = re.compile(r"(\d{1,2})(st|nd|rd|th)\b", re.IGNORECASE)
PERFCODE_PATTERN = re.compile(r"[?&]perfCode=(\d+)")

MERLIN_TAG_MAP = {
    "3d": "3D",
    "subtitled": "Subtitles",
    "audio": "Audio Description",
    "hoh": "Hard of Hearing",
    "mm": "Mini Merlins",
    "minimer": "Mini Merlins",
    "ss": "Silver Spoon",
    "baby": "Parent & Baby",
    "autism": "Autism Friendly",
    "access": "Wheelchair access",
    "licensed": "Licensed Bar",
    "luxury": "Luxury Seating",
    "fls": "No Free Passes",
    "box": "Private Box",
    "18": "Adults Only 18+",
    "advanced": "Advance Screening",
    "pfs": "Film Society",
    "saver": "Super Saver",
}


def _env_bool(*names: str, default: bool = False) -> bool:
    """Read first-present boolean env var among names."""
    for name in names:
        raw = os.environ.get(name)
        if raw is None:
            continue
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _env_int(*names: str, default: int) -> int:
    """Read first-present integer env var among names."""
    for name in names:
        raw = os.environ.get(name)
        if raw is None:
            continue
        try:
            return int(raw.strip())
        except ValueError:
            logger.warning("Invalid integer for %s: %r (using default %d)", name, raw, default)
            return default
    return default


def get_enabled_cinemas() -> Dict[str, Dict[str, str]]:
    """Return enabled cinemas from env (`WTW_ENABLED_CINEMAS` or `MERLIN_ENABLED_CINEMAS`)."""
    raw = (
        os.environ.get("WTW_ENABLED_CINEMAS")
        or os.environ.get("MERLIN_ENABLED_CINEMAS")
        or "all"
    ).strip()
    if not raw or raw.lower() == "all":
        return dict(MERLIN_CINEMAS)
    requested = {c.strip().lower() for c in raw.split(",") if c.strip()}
    enabled: Dict[str, Dict[str, str]] = {}
    invalid: List[str] = []
    for slug in requested:
        if slug in MERLIN_CINEMAS:
            enabled[slug] = MERLIN_CINEMAS[slug]
        else:
            invalid.append(slug)
    if invalid:
        logger.warning("Ignoring unknown cinema slug(s) in WTW_ENABLED_CINEMAS: %s", ", ".join(sorted(invalid)))
    if not enabled:
        logger.warning("No valid cinemas selected via WTW_ENABLED_CINEMAS; defaulting to all cinemas.")
        return dict(MERLIN_CINEMAS)
    logger.info("Enabled cinemas: %s", ", ".join(enabled.keys()))
    return enabled


def fetch_with_retries(url: str, retries: int = HTTP_RETRIES, timeout: int = HTTP_TIMEOUT) -> requests.Response:
    """Fetch URL with exponential backoff on failure."""
    headers = {"User-Agent": USER_AGENT}
    delay = HTTP_RETRY_DELAY
    for attempt in range(retries):
        try:
            r = HTTP_SESSION.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning("Attempt %d failed: %s", attempt + 1, e)
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= HTTP_RETRY_MULTIPLIER
    raise requests.RequestException("Max retries exceeded")


def strip_format_suffix(title: str) -> str:
    """Remove format suffixes like ' - HFR 3D' (high frame rate 3D) from the end of a title."""
    return FORMAT_SUFFIX.sub("", title).strip().strip(" -")


def extract_search_title(title: str) -> str:
    """Strip age rating, 'with subtitles', 'autism friendly screening' for search/links. E.g. 'GOAT AUTISM FRIENDLY SCREENING' -> 'GOAT'."""
    t = strip_format_suffix(title)
    t = SUBTITLE_SUFFIX.sub("", t).strip()
    t = AUTISM_FRIENDLY_SUFFIX.sub("", t).strip()
    t = RATING_PATTERN.sub("", t).strip()
    return t.strip(" -")


def extract_bbfc_rating(title: str) -> Optional[str]:
    """Extract BBFC rating from title: (15) -> 15, (12A) -> 12A, (PG) -> PG, etc."""
    m = RATING_PATTERN.search(title)
    if m:
        return m.group(1).upper().replace("R18", "R18")
    return None


def format_runtime(minutes: Optional[int]) -> str:
    """Format runtime as '121 min (2 hours 1 min)' or '90 min (1 hour 30 mins)'."""
    if not minutes:
        return ""
    parts = []
    if minutes >= 60:
        h = minutes // 60
        parts.append(f"{h} hour{'s' if h != 1 else ''}")
    m = minutes % 60
    if m > 0:
        parts.append(f"{m} min{'s' if m != 1 else ''}")
    if not parts:
        return f"{minutes} min"
    return " ".join(parts)


def parse_runtime_minutes(text: str) -> Optional[int]:
    """Parse '113 minutes' or 'Running time:113 minutes' -> 113."""
    m = re.search(r"(\d+)\s*minutes?", text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_uk_date(text: str, scrape_date: datetime) -> Optional[str]:
    """Parse 'Today 8 February 2026', 'Tomorrow 9 February', 'Tuesday 10 February 2026' -> YYYY-MM-DD."""
    text = MERLIN_DATE_SUFFIX.sub(r"\1", text.strip())
    today = scrape_date.date()
    # "Today 8 February 2026" or "Today\n8 February 2026"
    if "today" in text.lower():
        return today.isoformat()
    if "tomorrow" in text.lower():
        return (today + timedelta(days=1)).isoformat()
    # "Tuesday 10 February 2026" or "Saturday 28 February"
    m = re.search(r"(?:[A-Za-z]+\s+)?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
    if m:
        day, month_str, year = int(m.group(1)), m.group(2), int(m.group(3))
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            return dt.date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(?:[A-Za-z]+\s+)?(\d{1,2})\s+([A-Za-z]+)(?:\s|$)", text)
    if m:
        day, month_str = int(m.group(1)), m.group(2)
        year = scrape_date.year
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            if dt.date() < today:
                dt = datetime.strptime(f"{day} {month_str} {year + 1}", "%d %B %Y")
            return dt.date().isoformat()
        except ValueError:
            pass
    return None


def load_tmdb_cache() -> Dict[str, Dict]:
    """Load TMDb cache; drop expired entries."""
    if not Path(TMDB_CACHE_FILE).exists():
        return {}
    try:
        with open(TMDB_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Cache load failed: %s", e)
        return {}
    cutoff = (datetime.now() - timedelta(days=TMDB_CACHE_DAYS)).isoformat()
    return {k: v for k, v in cache.items() if v.get("cached_at", "") > cutoff}


def load_merlin_detail_cache() -> Dict[str, Dict]:
    """Load Merlin popup-detail cache; drop expired entries."""
    if not Path(MERLIN_DETAIL_CACHE_FILE).exists():
        return {}
    try:
        with open(MERLIN_DETAIL_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Merlin detail cache load failed: %s", e)
        return {}
    cutoff = (datetime.now() - timedelta(days=MERLIN_DETAIL_CACHE_DAYS)).isoformat()
    return {k: v for k, v in cache.items() if v.get("cached_at", "") > cutoff}


def save_tmdb_cache(cache: Dict[str, Dict]) -> None:
    """Persist TMDb cache."""
    try:
        with open(TMDB_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        logger.warning("Cache save failed: %s", e)


def save_merlin_detail_cache(cache: Dict[str, Dict]) -> None:
    """Persist Merlin popup-detail cache."""
    try:
        with open(MERLIN_DETAIL_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        logger.warning("Merlin detail cache save failed: %s", e)


def load_cinema_failure_state() -> Dict[str, Any]:
    """Load persisted per-cinema failure state."""
    if not Path(CINEMA_FAILURE_STATE_FILE).exists():
        return {"cinemas": {}}
    try:
        with open(CINEMA_FAILURE_STATE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            return {"cinemas": {}}
        cinemas = payload.get("cinemas")
        if not isinstance(cinemas, dict):
            payload["cinemas"] = {}
        return payload
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Cinema failure state load failed: %s", e)
        return {"cinemas": {}}


def save_cinema_failure_state(state: Dict[str, Any]) -> None:
    """Persist per-cinema failure state."""
    try:
        with open(CINEMA_FAILURE_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except OSError as e:
        logger.warning("Cinema failure state save failed: %s", e)


def slug_from_film_url(url: str) -> str:
    """Extract slug from film URL for cache key. E.g. /film/send-help/?screen=st-austell -> send-help."""
    if not url:
        return ""
    path = url.split("?")[0].rstrip("/")
    return path.split("/")[-1] if "/" in path else path


def _ensure_placeholder_poster() -> None:
    """Ensure a local placeholder poster exists for films without a TMDb poster."""
    path = Path(SITE_DIR) / POSTER_PLACEHOLDER_REL
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="420" height="630" viewBox="0 0 420 630" role="img" aria-label="Poster unavailable">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#111827"/>
      <stop offset="100%" stop-color="#1f2937"/>
    </linearGradient>
  </defs>
  <rect width="420" height="630" fill="url(#bg)"/>
  <rect x="24" y="24" width="372" height="582" rx="18" ry="18" fill="none" stroke="#334155" stroke-width="2"/>
  <circle cx="210" cy="250" r="68" fill="none" stroke="#64748b" stroke-width="8"/>
  <path d="M210 200v60m0 45h.01" stroke="#94a3b8" stroke-width="10" stroke-linecap="round"/>
  <text x="210" y="390" text-anchor="middle" fill="#cbd5e1" font-family="Arial, sans-serif" font-size="26" font-weight="700">Poster unavailable</text>
  <text x="210" y="425" text-anchor="middle" fill="#94a3b8" font-family="Arial, sans-serif" font-size="18">Merlin Cinemas Cornwall</text>
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def _parse_cached_at(value: str) -> Optional[datetime]:
    """Parse cached_at timestamp safely."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _download_poster(url: str, slug: str) -> str:
    """Download poster image and save under POSTERS_DIR; return relative path or '' on failure."""
    if not url or not url.startswith("http"):
        return ""
    slug = re.sub(r"[^a-z0-9-]", "", slug.lower()) or "poster"
    ext = "jpg"
    if ".webp" in url.lower():
        ext = "webp"
    elif ".png" in url.lower():
        ext = "png"
    path = Path(POSTERS_DIR) / f"{slug}.{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return f"posters/{slug}.{ext}"
    try:
        headers = {"User-Agent": USER_AGENT}
        if "merlincinemas.co.uk" in url:
            parsed = urlparse(url)
            headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
        r = HTTP_SESSION.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        path.write_bytes(r.content)
        return f"posters/{slug}.{ext}"  # relative to SITE_DIR for HTML
    except Exception as e:
        logger.warning("Poster download failed %s: %s", url[:50], e)
        return ""


def _tmdb_cache_key(film: Dict[str, Any]) -> str:
    """Stable cache key by search title so e.g. 'Send Help' and 'Send Help (with subtitles)' share one TMDb entry."""
    search_title = film.get("search_title") or extract_search_title(film.get("title", ""))
    if not search_title:
        slug = film.get("film_slug") or slug_from_film_url(film.get("film_url", ""))
        return slug or "unknown"
    return re.sub(r"[^a-z0-9]+", "-", search_title.lower()).strip("-") or "unknown"


def _normalize_title_for_match(title: str) -> str:
    """Normalize title for TMDb result matching: lower, collapse punctuation to space."""
    if not title:
        return ""
    return re.sub(r"[\s\-:]+", " ", title.lower()).strip()


def _pick_best_tmdb_result(results: List[Dict], search_title: str) -> Optional[Dict]:
    """Pick the TMDb result that best matches our search title (e.g. 'Avatar: Fire and Ash' not 'Avatar' 2009)."""
    if not results or not search_title:
        return results[0] if results else None
    norm_search = _normalize_title_for_match(search_title)
    if not norm_search:
        return results[0]
    best = None
    best_score = -1
    for r in results:
        title = (r.get("title") or "").strip()
        norm_title = _normalize_title_for_match(title)
        if norm_title == norm_search:
            return r  # Exact match
        score = 0
        if norm_search in norm_title:
            score = 90  # Our search is contained in result title (e.g. result "Avatar: Fire and Ash")
        elif norm_title in norm_search:
            score = 30  # Result is shorter (e.g. "Avatar" when we want "Avatar: Fire and Ash") - prefer longer
        else:
            # Partial: prefer recent films for sequel-style titles
            release = (r.get("release_date") or "")[:4]
            try:
                year = int(release) if release else 0
                if year >= 2020:
                    score = 50
                else:
                    score = 10
            except ValueError:
                score = 10
        if score > best_score:
            best_score = score
            best = r
    return best if best is not None else results[0]


def _event_cinema_fallback_queries(title: str) -> List[str]:
    """For RBO/event cinema titles, return TMDb search queries to try in order. RBO uses 'Royal Ballet & Opera 2025/26: X'; Met Opera also tries 'The Metropolitan Opera: X'."""
    if not title or "RBO" not in title.upper():
        return []
    # Match: "PRODUCTION - RBO 2025-26" or "PRODUCTION - The MET Opera - RBO 2025-26"
    m = re.search(r"^(.+?)\s+-\s+(?:The MET Opera\s+-\s+)?RBO\s+2025-26", title, re.IGNORECASE)
    if not m:
        return []
    production = m.group(1).strip().title()
    if not production:
        return []
    queries = [f"Royal Ballet & Opera 2025/26: {production}"]
    # Met Opera titles: TMDb lists as "The Metropolitan Opera: Eugene Onegin"
    if "MET Opera" in title or "Met Opera" in title:
        queries.append(f"The Metropolitan Opera: {production}")
    return queries


def _empty_tmdb_entry() -> Dict[str, Any]:
    """Empty cache entry so we never refetch after a miss or error."""
    return {
        "poster_url": "",
        "trailer_url": "",
        "runtime_min": None,
        "vote_average": None,
        "genres": [],
        "imdb_id": "",
        "overview": "",
        "director": "",
        "writer": "",
        "cast": "",
        "cached_at": datetime.now().isoformat(),
    }


def _tmdb_get(url: str, params: Dict[str, Any], timeout: int = 10) -> requests.Response:
    """TMDb GET with 429-aware backoff and optional inter-request pacing."""
    delay = 1.0
    for attempt in range(TMDB_REQUEST_RETRIES):
        if TMDB_DELAY_SEC > 0:
            time.sleep(TMDB_DELAY_SEC)
        try:
            resp = HTTP_SESSION.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "").strip()
                try:
                    wait_s = max(1.0, float(retry_after))
                except ValueError:
                    wait_s = delay
                if attempt == TMDB_REQUEST_RETRIES - 1:
                    resp.raise_for_status()
                time.sleep(wait_s)
                delay = min(delay * 2, 8.0)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == TMDB_REQUEST_RETRIES - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
    raise requests.RequestException("TMDb request retries exhausted")


def enrich_film_tmdb(
    film: Dict[str, Any],
    api_key: str,
    cache: Dict[str, Dict],
) -> None:
    """Enrich film with poster, trailer, vote_average, genres, imdb_id from TMDb. Modifies film in place."""
    search_title = film.get("search_title") or extract_search_title(film.get("title", ""))
    if not search_title:
        return
    cache_key = _tmdb_cache_key(film)

    if cache_key in cache:
        entry = cache[cache_key]
        cached_at = _parse_cached_at(entry.get("cached_at", ""))
        stale_empty_cache = False
        if not entry.get("poster_url"):
            if not cached_at:
                stale_empty_cache = True
            else:
                stale_empty_cache = datetime.now(cached_at.tzinfo) - cached_at >= timedelta(days=TMDB_EMPTY_CACHE_TTL_DAYS)
        # Refetch if we have poster but no genres (backfill for old cache entries)
        if (not (entry.get("genres") or [])) and entry.get("poster_url"):
            pass  # Fall through to API call to get genres (and refresh cache)
        # Retry empty-cache misses after TTL so new TMDb records can be picked up later
        elif stale_empty_cache:
            pass
        # Retry with event cinema fallback if cache has no poster and this is an RBO title
        elif not entry.get("poster_url") and _event_cinema_fallback_queries(film.get("title", "")):
            pass  # Fall through to API call with fallback
        else:
            film["poster_url"] = entry.get("poster_url") or film.get("poster_url") or ""
            film["trailer_url"] = entry.get("trailer_url") or ""
            if film.get("runtime_min") is None and entry.get("runtime_min") is not None:
                film["runtime_min"] = entry.get("runtime_min")
            film["vote_average"] = entry.get("vote_average")
            film["genres"] = entry.get("genres") or []
            film["imdb_id"] = entry.get("imdb_id") or ""
            film["overview"] = entry.get("overview") or ""
            film["director"] = entry.get("director") or ""
            film["writer"] = entry.get("writer") or ""
            film["cast"] = entry.get("cast") or film.get("cast") or ""
            return

    try:
        search_url = "https://api.themoviedb.org/3/search/movie"
        search_r = _tmdb_get(
            search_url,
            params={"api_key": api_key, "query": search_title, "language": "en-GB"},
            timeout=10,
        )
        data = search_r.json()
        results = data.get("results") or []
        match_title = search_title
        # Event cinema fallback: try RBO/Met Opera queries when full title returns nothing
        fallback_queries = _event_cinema_fallback_queries(film.get("title", "")) if not results else []
        for fq in fallback_queries:
            search_r = _tmdb_get(
                search_url,
                params={"api_key": api_key, "query": fq, "language": "en-GB"},
                timeout=10,
            )
            data = search_r.json()
            results = data.get("results") or []
            if results:
                match_title = fq
                break
        if not results:
            cache[cache_key] = _empty_tmdb_entry()
            return
        chosen = _pick_best_tmdb_result(results, match_title)
        if not chosen:
            cache[cache_key] = _empty_tmdb_entry()
            return
        # If best match has no poster, try next results that do (e.g. TMDb sometimes omits poster on new entries)
        movie_id = chosen.get("id")
        if not chosen.get("poster_path") and results:
            for r in results:
                if r.get("poster_path") and _normalize_title_for_match(r.get("title") or "") == _normalize_title_for_match(chosen.get("title") or ""):
                    chosen = r
                    movie_id = r.get("id")
                    break
        if not movie_id:
            cache[cache_key] = _empty_tmdb_entry()
            return

        detail_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        detail_r = _tmdb_get(
            detail_url,
            params={"api_key": api_key, "append_to_response": "videos,credits", "language": "en-GB"},
            timeout=10,
        )
        movie = detail_r.json()

        poster_path = (movie.get("poster_path") or "").lstrip("/")
        poster_url = f"https://image.tmdb.org/t/p/w342/{poster_path}" if poster_path else ""

        trailer_url = ""
        for v in (movie.get("videos", {}).get("results") or []):
            if v.get("site") == "YouTube" and v.get("type", "").lower() in ("trailer", "teaser"):
                key = v.get("key")
                if key:
                    trailer_url = f"https://www.youtube.com/watch?v={key}"
                    break

        GENRE_MAP = {
            28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
            99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
            27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
            10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
        }
        genre_list = movie.get("genres") or []
        genres = [g.get("name", "").strip() for g in genre_list if g.get("name")] if genre_list else []
        if not genres:
            # Detail response may omit genres; use genre_ids from detail or from search result
            genre_ids = movie.get("genre_ids") or chosen.get("genre_ids") or []
            genres = [GENRE_MAP[g] for g in genre_ids if g in GENRE_MAP]

        imdb_id = movie.get("imdb_id") or ""
        overview = (movie.get("overview") or "").strip()

        # Credits: director, writer, cast (with character names)
        director_names: List[str] = []
        writer_names: List[str] = []
        cast_parts: List[str] = []
        credits = movie.get("credits") or {}
        for c in credits.get("crew") or []:
            job = (c.get("job") or "").strip()
            name = (c.get("name") or "").strip()
            if not name:
                continue
            if job == "Director" and name not in director_names:
                director_names.append(name)
            if job in ("Screenplay", "Writer", "Story", "Characters", "Novel") and name not in writer_names:
                writer_names.append(name)
        for c in (credits.get("cast") or [])[:12]:
            name = (c.get("name") or "").strip()
            char = (c.get("character") or "").strip()
            if name:
                cast_parts.append(f"{name} ({char})" if char else name)
        director_str = ", ".join(director_names[:3])
        writer_str = ", ".join(writer_names[:5])
        cast_str = ", ".join(cast_parts) if cast_parts else film.get("cast") or ""

        film["poster_url"] = poster_url or film.get("poster_url") or ""
        film["trailer_url"] = trailer_url
        if film.get("runtime_min") is None and movie.get("runtime"):
            film["runtime_min"] = movie.get("runtime")
        film["vote_average"] = movie.get("vote_average")
        film["genres"] = genres
        film["imdb_id"] = imdb_id
        film["overview"] = overview
        film["director"] = director_str
        film["writer"] = writer_str
        film["cast"] = cast_str

        cache[cache_key] = {
            "poster_url": film["poster_url"],
            "trailer_url": trailer_url,
            "runtime_min": film.get("runtime_min"),
            "vote_average": film["vote_average"],
            "genres": genres,
            "imdb_id": imdb_id,
            "overview": overview,
            "director": director_str,
            "writer": writer_str,
            "cast": cast_str,
            "cached_at": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.warning("TMDb enrich failed for %s: %s", search_title, e)
        cache[cache_key] = _empty_tmdb_entry()


def _merge_subtitle_variants(films: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge '(with subtitles)' and 'Autism Friendly Screening' variants into the main film card; variant showtimes go at the bottom."""
    by_base: Dict[str, List[Dict[str, Any]]] = {}
    for f in films:
        base = (f.get("search_title") or "").strip() or extract_search_title(f.get("title", ""))
        if base not in by_base:
            by_base[base] = []
        by_base[base].append(f)

    merged: List[Dict[str, Any]] = []
    for base, group in by_base.items():
        if len(group) == 1:
            merged.append(group[0])
            continue
        # Prefer the canonical title (no variant suffix)
        def is_variant(f: Dict) -> bool:
            t = (f.get("title") or "").lower()
            return "(with subtitles)" in t or "autism friendly screening" in t
        main = next((f for f in group if not is_variant(f)), group[0])
        others = [f for f in group if f is not main]
        all_showtimes = list(main.get("showtimes") or [])
        seen_keys: set = set()
        for st in all_showtimes:
            seen_keys.add((st["date"], st["time"], st["screen"]))
        for other in others:
            for st in (other.get("showtimes") or []):
                key = (st["date"], st["time"], st["screen"])
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_showtimes.append(dict(st))
        tags = lambda s: s.get("tags") or []
        all_showtimes.sort(
            key=lambda s: (
                "Subtitles" in tags(s) or "Autism Friendly" in tags(s),
                s["date"],
                s["time"],
            )
        )
        main = dict(main)
        main["showtimes"] = all_showtimes
        merged.append(main)
    return merged


def _extract_merlin_tag_data(showtime_link: Any) -> Dict[str, Any]:
    """Extract normalized Merlin tag labels/keys and sold-out flag from a showtime anchor."""
    labels: List[str] = []
    keys: List[str] = []
    sold_out = "soldout" in ((showtime_link.get("class") or []) if hasattr(showtime_link, "get") else [])
    for img in showtime_link.select("div.tooltip img"):
        key = (img.get("data-key") or "").strip().lower()
        if key and key not in keys:
            keys.append(key)
        if key in MERLIN_TAG_MAP and MERLIN_TAG_MAP[key] not in labels:
            labels.append(MERLIN_TAG_MAP[key])
        title = (img.get("title") or "").strip().lower()
        if "sold out" in title:
            sold_out = True
        if "subtit" in title and "Subtitles" not in labels:
            labels.append("Subtitles")
        if "wheelchair" in title and "Wheelchair access" not in labels:
            labels.append("Wheelchair access")
        if "autism" in title and "Autism Friendly" not in labels:
            labels.append("Autism Friendly")
        if "parent" in title and "baby" in title and "Parent & Baby" not in labels:
            labels.append("Parent & Baby")
    if not labels:
        labels = ["2D"]
    return {"tags": labels, "tag_keys": keys, "sold_out": sold_out}


def _parse_merlin_release_date(text: str) -> str:
    """Parse Merlin release date text like '26th February, 2026' to ISO date."""
    if not text:
        return ""
    cleaned = MERLIN_DATE_SUFFIX.sub(r"\1", text).replace(",", "").strip()
    try:
        return datetime.strptime(cleaned, "%d %B %Y").date().isoformat()
    except ValueError:
        return ""


def _fetch_merlin_film_details(cinema_url: str, film_data: str) -> Dict[str, Any]:
    """Fetch Merlin popup metadata for a film id-slug."""
    details = {
        "event_id": "",
        "cinema_code": "",
        "release_date": "",
        "release_date_text": "",
        "ticket_note": "",
        "tag_keys_available": [],
        "tag_labels_available": [],
        "film_id": "",
    }
    if not film_data:
        return details
    parts = film_data.split("-", 1)
    if parts and parts[0].isdigit():
        details["film_id"] = parts[0]
    try:
        popup_url = urljoin(cinema_url, f"/ajax/film/{film_data}")
        resp = fetch_with_retries(popup_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        popup = soup.select_one(".popup")
        if popup:
            yt = (popup.get("data-youtube") or "").strip()
            if yt:
                details["trailer_url"] = f"https://www.youtube.com/watch?v={yt}"

        runtime_el = soup.select_one(".content.film_info .runtime")
        if runtime_el:
            details["runtime_min"] = parse_runtime_minutes(runtime_el.get_text(" ", strip=True))

        rel_el = soup.select_one(".content.film_info .released")
        if rel_el:
            release_text = rel_el.get_text(" ", strip=True).replace("RELEASED", "").strip()
            details["release_date_text"] = release_text
            details["release_date"] = _parse_merlin_release_date(release_text)

        genres_el = soup.select_one(".content.film_info .genres")
        if genres_el:
            g_text = genres_el.get_text(" ", strip=True).replace("GENRES", "").strip()
            details["genres"] = [g.strip() for g in g_text.split(",") if g.strip()]

        desc = soup.select_one(".content.film_info .description")
        if desc:
            cast_h3 = desc.find("h3", string=re.compile(r"cast", re.IGNORECASE))
            if cast_h3:
                cast_p = cast_h3.find_next("p")
                if cast_p:
                    details["cast"] = cast_p.get_text(" ", strip=True)
            crew_h3 = desc.find("h3", string=re.compile(r"crew", re.IGNORECASE))
            if crew_h3:
                crew_p = crew_h3.find_next("p")
                if crew_p:
                    crew_text = crew_p.get_text(" ", strip=True)
                    details["director"] = crew_text.replace("Director:", "").strip()
            # Use first long paragraph as synopsis candidate
            for p in desc.find_all("p"):
                txt = p.get_text(" ", strip=True)
                if len(txt) >= 60:
                    details["synopsis"] = txt
                    break

        note_el = soup.select_one(".content.ticket_info p.note")
        if note_el:
            details["ticket_note"] = note_el.get_text(" ", strip=True)

        select = soup.select_one(".content.ticket_info select[name='update_listings']")
        if select:
            details["event_id"] = (select.get("data-eventid") or "").strip()
            selected = select.find("option", selected=True)
            if selected:
                details["cinema_code"] = (selected.get("value") or "").strip()

        available_keys: List[str] = []
        available_labels: List[str] = []
        for img in soup.select(".content.ticket_info .filters img[data-key]"):
            key = (img.get("data-key") or "").strip().lower()
            if key and key not in available_keys:
                available_keys.append(key)
            label = MERLIN_TAG_MAP.get(key) or (img.get("title") or "").strip()
            if label and label not in available_labels:
                available_labels.append(label)
        details["tag_keys_available"] = available_keys
        details["tag_labels_available"] = available_labels
    except Exception as e:
        logger.warning("Film detail fetch failed %s: %s", film_data, e)
    return details


def _get_merlin_film_details_cached(
    cinema_slug: str,
    cinema_url: str,
    film_data: str,
    detail_cache: Dict[str, Dict[str, Any]],
    detail_updates: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Get Merlin popup details from cache when possible."""
    if not film_data:
        return _fetch_merlin_film_details(cinema_url, film_data)
    cache_key = f"{cinema_slug}:{film_data}"
    cached = detail_updates.get(cache_key) or detail_cache.get(cache_key)
    if cached:
        entry = deepcopy(cached)
        entry.pop("cached_at", None)
        return entry
    details = _fetch_merlin_film_details(cinema_url, film_data)
    detail_updates[cache_key] = {"cached_at": datetime.now().isoformat(), **details}
    return details


def _scrape_single_cinema(
    cinema_slug: str,
    cinema_info: Dict[str, str],
    scrape_date: datetime,
    detail_cache: Dict[str, Dict[str, Any]],
) -> Tuple[str, Dict[str, Any], Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Scrape one cinema page and return (slug, cinema_payload, detail_cache_updates, diagnostics)."""
    cinema_url = cinema_info["url"]
    logger.info("Fetching %s", cinema_url)
    resp = fetch_with_retries(cinema_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    film_cards = soup.select("div.filmCard")
    used_fallback_selector = False
    if not film_cards:
        fallback_cards = soup.select("div[data-film]")
        if fallback_cards:
            film_cards = fallback_cards
            used_fallback_selector = True
            logger.warning("Primary selector missing for %s; using fallback selector div[data-film]", cinema_slug)
    films_by_key: Dict[str, Dict[str, Any]] = {}
    detail_updates: Dict[str, Dict[str, Any]] = {}
    table_showtimes_count = 0
    frame_showtimes_count = 0

    for card in film_cards:
        card_classes = card.get("class") or []
        if "internal_advert" in card_classes:
            continue
        title_el = card.select_one(".img h2.overlay")
        title = (title_el.get_text(strip=True) if title_el else "").replace("\u2013", "-").replace("\u2014", "-")
        title = strip_format_suffix(title)
        if not title:
            continue

        film_data = (card.get("data-film") or "").strip()
        film_slug = ""
        film_url = ""
        site_image_url = (card.select_one(".img") or {}).get("data-src", "") if card.select_one(".img") else ""
        event_banner_el = card.select_one("img.event_banner")
        event_banner_url = urljoin(cinema_url, event_banner_el.get("src", "").strip()) if event_banner_el else ""
        if film_data:
            parts = film_data.split("-", 1)
            film_slug = parts[1] if len(parts) > 1 else film_data
            film_url = urljoin(cinema_url, f"/film/{film_data}")

        search_title = extract_search_title(title)
        key = f"{search_title.lower()}::{film_slug}"

        cert_el = card.select_one(".img .cert")
        cert = (cert_el.get("data-cert") or "").strip() if cert_el else ""
        titled = title if not cert else f"{title} ({cert})"

        row = films_by_key.get(key)
        if not row:
            details = _get_merlin_film_details_cached(
                cinema_slug=cinema_slug,
                cinema_url=cinema_url,
                film_data=film_data,
                detail_cache=detail_cache,
                detail_updates=detail_updates,
            )
            row = {
                "title": titled,
                "certificate": cert,
                "search_title": search_title,
                "film_slug": film_slug or re.sub(r"[^a-z0-9]+", "-", search_title.lower()).strip("-"),
                "film_id": details.get("film_id", ""),
                "event_id": details.get("event_id", ""),
                "cinema_code": details.get("cinema_code", ""),
                "synopsis": details.get("synopsis", ""),
                "cast": details.get("cast", ""),
                "director": details.get("director", ""),
                "runtime_min": details.get("runtime_min"),
                "release_date": details.get("release_date", ""),
                "release_date_text": details.get("release_date_text", ""),
                "ticket_note": details.get("ticket_note", ""),
                "tag_labels_available": details.get("tag_labels_available", []),
                "tag_keys_available": details.get("tag_keys_available", []),
                "film_url": film_url or cinema_url,
                "poster_url": "",
                "site_image_url": site_image_url,
                "event_banner_url": event_banner_url,
                "is_live_event": "live on stage" in cert.lower(),
                "trailer_url": details.get("trailer_url", ""),
                "genres": details.get("genres", []),
                "cinema_name": cinema_info["name"],
                "showtimes": [],
            }
            films_by_key[key] = row

        parsed_any = False
        for tr in card.select("div.listings table tr"):
            date_el = tr.find("h5")
            parsed_date = parse_uk_date(date_el.get_text(" ", strip=True), scrape_date) if date_el else None
            if not parsed_date:
                continue
            for show_a in tr.find_all("a", href=True):
                time_match = re.search(r"(\d{1,2}:\d{2})", show_a.get_text(" ", strip=True))
                if not time_match:
                    continue
                tag_data = _extract_merlin_tag_data(show_a)
                booking_url = show_a["href"].strip()
                perf_match = PERFCODE_PATTERN.search(booking_url)
                row["showtimes"].append({
                    "date": parsed_date,
                    "time": time_match.group(1),
                    "screen": cinema_info["name"],
                    "cinema_name": cinema_info["name"],
                    "cinema_url": cinema_url,
                    "booking_url": booking_url,
                    "perf_code": perf_match.group(1) if perf_match else "",
                    "sold_out": tag_data["sold_out"],
                    "tags": tag_data["tags"],
                    "tag_keys": tag_data["tag_keys"],
                })
                table_showtimes_count += 1
                parsed_any = True

        if parsed_any:
            continue

        frame = card.find_parent(attrs={"data-frame": True})
        frame_value = (frame.get("data-frame") or "").strip() if frame else ""
        frame_date = frame_value if re.match(r"^\d{4}-\d{2}-\d{2}$", frame_value) else ""
        if not frame_date:
            continue
        for show_a in card.select("div.times a[href]"):
            time_match = re.search(r"(\d{1,2}:\d{2})", show_a.get_text(" ", strip=True))
            if not time_match:
                continue
            tag_data = _extract_merlin_tag_data(show_a)
            booking_url = show_a["href"].strip()
            perf_match = PERFCODE_PATTERN.search(booking_url)
            row["showtimes"].append({
                "date": frame_date,
                "time": time_match.group(1),
                "screen": cinema_info["name"],
                "cinema_name": cinema_info["name"],
                "cinema_url": cinema_url,
                "booking_url": booking_url,
                "perf_code": perf_match.group(1) if perf_match else "",
                "sold_out": tag_data["sold_out"],
                "tags": tag_data["tags"],
                "tag_keys": tag_data["tag_keys"],
            })
            frame_showtimes_count += 1

    films = list(films_by_key.values())
    for film in films:
        seen = set()
        deduped = []
        for st in sorted(film.get("showtimes") or [], key=lambda s: (s["date"], s["time"])):
            unique_key = (st["date"], st["time"], st["screen"], st["booking_url"])
            if unique_key in seen:
                continue
            seen.add(unique_key)
            deduped.append(st)
        film["showtimes"] = deduped

    cinema_payload = {
        "name": cinema_info["name"],
        "url": cinema_url,
        "films": _merge_subtitle_variants(films),
    }
    diagnostics = {
        "film_cards_raw": len(film_cards),
        "used_fallback_selector": used_fallback_selector,
        "films_parsed": len(cinema_payload["films"]),
        "showtimes_parsed": sum(len(f.get("showtimes") or []) for f in cinema_payload["films"]),
        "table_showtimes_parsed": table_showtimes_count,
        "frame_showtimes_parsed": frame_showtimes_count,
    }
    return cinema_slug, cinema_payload, detail_updates, diagnostics


def scrape_whats_on(
    scrape_date: Optional[datetime] = None,
    detail_cache: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], bool, Dict[str, Dict[str, Any]], Dict[str, str], List[str]]:
    """Fetch whats-on pages for all configured Merlin cinemas."""
    scrape_date = scrape_date or datetime.now(timezone.utc)
    enabled_cinemas = get_enabled_cinemas()
    active_cinema_slugs = list(enabled_cinemas.keys())
    cinemas_scraped: Dict[str, Dict[str, Any]] = {}
    scrape_diagnostics: Dict[str, Dict[str, Any]] = {}
    failed_cinemas: Dict[str, str] = {}
    detail_cache = detail_cache if detail_cache is not None else {}
    detail_cache_updated = False

    max_workers = max(1, min(len(enabled_cinemas), (os.cpu_count() or 4)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_scrape_single_cinema, cinema_slug, cinema_info, scrape_date, detail_cache): cinema_slug
            for cinema_slug, cinema_info in enabled_cinemas.items()
        }
        for future in as_completed(future_map):
            cinema_slug = future_map[future]
            try:
                cinema_slug, cinema_payload, detail_updates, diagnostics = future.result()
            except Exception as e:
                failed_cinemas[cinema_slug] = str(e)
                logger.error("Cinema scrape failed for %s: %s", cinema_slug, e)
                scrape_diagnostics[cinema_slug] = {
                    "film_cards_raw": -1,
                    "used_fallback_selector": False,
                    "films_parsed": 0,
                    "showtimes_parsed": 0,
                    "table_showtimes_parsed": 0,
                    "frame_showtimes_parsed": 0,
                    "scrape_failed": True,
                }
                continue
            cinemas_scraped[cinema_slug] = cinema_payload
            scrape_diagnostics[cinema_slug] = diagnostics
            if detail_updates:
                detail_cache_updated = True
                detail_cache.update(detail_updates)

    ordered_cinemas = {
        cinema_slug: cinemas_scraped[cinema_slug]
        for cinema_slug in enabled_cinemas
        if cinema_slug in cinemas_scraped
    }

    for cinema_slug, cinema_info in enabled_cinemas.items():
        if cinema_slug not in ordered_cinemas:
            ordered_cinemas[cinema_slug] = {
                "name": cinema_info["name"],
                "url": cinema_info["url"],
                "films": [],
            }

    return {
        "updated_at": scrape_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cinemas": ordered_cinemas,
    }, detail_cache_updated, scrape_diagnostics, failed_cinemas, active_cinema_slugs


def compute_fingerprint(data: Dict[str, Any]) -> str:
    """Stable hash of data (film titles + showtime counts + dates) for change detection."""
    canonical = []
    for cinema in (data.get("cinemas") or {}).values():
        for film in cinema.get("films") or []:
            canonical.append(film.get("title", ""))
            for st in film.get("showtimes") or []:
                canonical.append(f"{st.get('date')}_{st.get('time')}_{st.get('screen')}")
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def validate_scrape_health(
    data: Dict[str, Any],
    scrape_diagnostics: Dict[str, Dict[str, Any]],
    scrape_date: datetime,
) -> None:
    """Validate scrape quality and optionally fail fast on suspected markup drift/degraded data."""
    total_films = 0
    total_showtimes = 0
    cinemas_with_films = 0
    markup_suspect_cinemas: List[str] = []
    selector_zero_cinemas: List[str] = []
    primary_selector_zero_cinemas: List[str] = []
    per_cinema_film_counts: Dict[str, int] = {}
    excluded_raw = os.environ.get("HEALTH_EXCLUDED_CINEMAS", "")
    excluded_cinemas = {c.strip().lower() for c in excluded_raw.split(",") if c.strip()}

    for cinema_slug, cinema in (data.get("cinemas") or {}).items():
        if cinema_slug.lower() in excluded_cinemas:
            logger.info("Health check: excluding cinema '%s' via HEALTH_EXCLUDED_CINEMAS", cinema_slug)
            continue
        films = cinema.get("films") or []
        film_count = len(films)
        per_cinema_film_counts[cinema_slug] = film_count
        total_films += film_count
        show_count = sum(len(f.get("showtimes") or []) for f in films)
        total_showtimes += show_count
        if film_count > 0:
            cinemas_with_films += 1

        diag = scrape_diagnostics.get(cinema_slug) or {}
        raw_cards = int(diag.get("film_cards_raw") or 0)
        parsed_films = int(diag.get("films_parsed") or 0)
        if bool(diag.get("used_fallback_selector")):
            primary_selector_zero_cinemas.append(cinema_slug)
        if raw_cards == 0:
            selector_zero_cinemas.append(cinema_slug)
        if raw_cards == 0 or (raw_cards > 0 and parsed_films == 0):
            markup_suspect_cinemas.append(cinema_slug)
            logger.warning(
                "Markup/parse warning for %s: raw film cards=%s parsed films=%s showtimes=%s",
                cinema_slug, raw_cards, parsed_films, show_count,
            )

    build_today = scrape_date.date().isoformat()
    now_cutoff = (scrape_date.date() + timedelta(days=NOW_SHOWING_THRESHOLD_DAYS)).isoformat()
    now_showing_films = 0
    for cinema in (data.get("cinemas") or {}).values():
        for film in cinema.get("films") or []:
            show_dates = [st.get("date", "") for st in (film.get("showtimes") or []) if st.get("date", "")]
            if not show_dates:
                continue
            earliest = min(show_dates)
            if build_today <= earliest <= now_cutoff:
                now_showing_films += 1

    min_total_films = _env_int("WTW_MIN_TOTAL_FILMS", "HEALTH_MIN_TOTAL_FILMS", default=10)
    min_total_showtimes = _env_int("WTW_MIN_TOTAL_SHOWTIMES", "HEALTH_MIN_TOTAL_SHOWTIMES", default=30)
    raw_min_cinemas = os.environ.get("HEALTH_MIN_CINEMAS_WITH_FILMS")
    considered_cinemas = max(0, len(per_cinema_film_counts))
    if raw_min_cinemas is None:
        min_cinemas_with_films = min(4, considered_cinemas)
    else:
        try:
            min_cinemas_with_films = int(raw_min_cinemas.strip())
        except ValueError:
            logger.warning("Invalid integer for HEALTH_MIN_CINEMAS_WITH_FILMS: %r (using default)", raw_min_cinemas)
            min_cinemas_with_films = min(4, considered_cinemas)
    min_now_showing_films = _env_int("HEALTH_MIN_NOW_SHOWING_FILMS", default=1)
    max_markup_suspect_cinemas = _env_int("HEALTH_MAX_MARKUP_SUSPECT_CINEMAS", default=1)
    min_films_per_cinema = _env_int("WTW_MIN_FILMS_PER_CINEMA", default=0)
    enforce = _env_bool("HEALTHCHECK_ENFORCE", default=HEALTHCHECK_DEFAULT_ENFORCE == "1")
    fail_on_markup_drift = _env_bool("WTW_FAIL_ON_MARKUP_DRIFT", default=False)

    problems: List[str] = []
    if total_films < min_total_films:
        problems.append(f"total films {total_films} < minimum {min_total_films}")
    if total_showtimes < min_total_showtimes:
        problems.append(f"total showtimes {total_showtimes} < minimum {min_total_showtimes}")
    if cinemas_with_films < min_cinemas_with_films:
        problems.append(f"cinemas with films {cinemas_with_films} < minimum {min_cinemas_with_films}")
    if now_showing_films < min_now_showing_films:
        problems.append(f"now-showing films {now_showing_films} < minimum {min_now_showing_films}")
    if len(markup_suspect_cinemas) > max_markup_suspect_cinemas:
        problems.append(
            f"markup suspect cinemas {len(markup_suspect_cinemas)} > allowed {max_markup_suspect_cinemas} "
            f"({', '.join(markup_suspect_cinemas)})"
        )
    if min_films_per_cinema > 0:
        underfilled = [
            f"{slug}={count}"
            for slug, count in sorted(per_cinema_film_counts.items())
            if count < min_films_per_cinema
        ]
        if underfilled:
            problems.append(
                f"cinemas below WTW_MIN_FILMS_PER_CINEMA({min_films_per_cinema}): {', '.join(underfilled)}"
            )
    if selector_zero_cinemas and fail_on_markup_drift:
        problems.append(
            f"markup drift detected (zero film nodes) for cinemas: {', '.join(sorted(selector_zero_cinemas))}"
        )
    if primary_selector_zero_cinemas and fail_on_markup_drift:
        problems.append(
            "markup drift detected (primary selector returned zero film nodes) for cinemas: "
            + ", ".join(sorted(primary_selector_zero_cinemas))
        )

    logger.info(
        "Health summary: films=%d showtimes=%d cinemas_with_films=%d now_showing=%d markup_suspect=%d",
        total_films, total_showtimes, cinemas_with_films, now_showing_films, len(markup_suspect_cinemas),
    )
    if selector_zero_cinemas and not fail_on_markup_drift:
        logger.warning(
            "Markup drift warning (WTW_FAIL_ON_MARKUP_DRIFT disabled): zero film nodes for %s",
            ", ".join(sorted(selector_zero_cinemas)),
        )
    if primary_selector_zero_cinemas and not fail_on_markup_drift:
        logger.warning(
            "Markup drift warning (WTW_FAIL_ON_MARKUP_DRIFT disabled): primary selector empty for %s",
            ", ".join(sorted(primary_selector_zero_cinemas)),
        )
    if not problems:
        return

    message = "Health checks failed: " + "; ".join(problems)
    if enforce:
        raise RuntimeError(message)
    logger.warning("%s (not enforced; HEALTHCHECK_ENFORCE=%s)", message, os.environ.get("HEALTHCHECK_ENFORCE", ""))


def apply_previous_cinema_fallback(
    data: Dict[str, Any],
    failed_cinemas: Dict[str, str],
    previous_data: Optional[Dict[str, Any]],
) -> List[str]:
    """For failed cinema scrapes, reuse previous cinema payload so one outage doesn't degrade publish."""
    if not failed_cinemas or not previous_data:
        return []
    previous_cinemas = previous_data.get("cinemas") or {}
    restored: List[str] = []
    for cinema_slug in failed_cinemas:
        prior = previous_cinemas.get(cinema_slug)
        if not prior:
            continue
        data.setdefault("cinemas", {})[cinema_slug] = prior
        restored.append(cinema_slug)
    return restored


def update_cinema_failure_state(
    prior_state: Dict[str, Any],
    failed_cinemas: Dict[str, str],
    scrape_date: datetime,
    active_cinema_slugs: List[str],
) -> Tuple[Dict[str, Any], bool]:
    """Update and return persistent per-cinema consecutive failure counters."""
    updated = deepcopy(prior_state) if prior_state else {"cinemas": {}}
    cinemas_state = updated.setdefault("cinemas", {})
    ts = scrape_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    changed = False
    for cinema_slug in active_cinema_slugs:
        row = cinemas_state.get(cinema_slug) or {}
        prev_consecutive = int(row.get("consecutive_failures") or 0)
        prev_error = (row.get("last_error") or "").strip()
        prev_success = (row.get("last_success") or "").strip()
        prev_failure = (row.get("last_failure") or "").strip()

        if cinema_slug in failed_cinemas:
            new_consecutive = prev_consecutive + 1
            new_error = (failed_cinemas.get(cinema_slug) or "").strip()
            new_row = {
                "consecutive_failures": new_consecutive,
                "last_error": new_error,
                "last_failure": ts,
                "last_success": prev_success,
            }
        else:
            # Keep stable success state to avoid needless file churn on healthy runs.
            success_ts = prev_success
            if prev_consecutive > 0 or not prev_success:
                success_ts = ts
            new_row = {
                "consecutive_failures": 0,
                "last_error": "",
                "last_failure": prev_failure,
                "last_success": success_ts,
            }

        if (
            int(new_row["consecutive_failures"]) != prev_consecutive
            or (new_row.get("last_error") or "") != prev_error
            or (new_row.get("last_success") or "") != prev_success
            or (new_row.get("last_failure") or "") != prev_failure
        ):
            changed = True
        cinemas_state[cinema_slug] = new_row

    prior_updated_at = (prior_state or {}).get("updated_at", "")
    updated["updated_at"] = ts if changed else prior_updated_at
    if updated["updated_at"] != prior_updated_at:
        changed = True
    return updated, changed


def build_html(data: Dict[str, Any]) -> str:
    """Generate single self-contained index.html with Web3 style and date filtering."""
    initial_showings_visible = _env_int("WTW_INITIAL_SHOWINGS_VISIBLE", default=SHOWTIMES_MAX_SLOTS_PER_FILM)
    if initial_showings_visible < 1:
        initial_showings_visible = 1

    def short_cinema_name(name: str) -> str:
        value = (name or "").strip()
        if not value:
            return ""
        special = {
            "Savoy Cinema, Penzance": "Savoy, PZ",
            "The Ritz, Penzance": "Ritz, PZ",
        }
        if value in special:
            return special[value]
        if "," in value:
            return value.split(",")[-1].strip()
        return value

    aggregated: Dict[str, Dict[str, Any]] = {}
    for cinema in (data.get("cinemas") or {}).values():
        cinema_name = cinema.get("name", "")
        for film in cinema.get("films") or []:
            search_title = (film.get("search_title") or extract_search_title(film.get("title", ""))).strip()
            film_id = (film.get("film_id") or "").strip()
            key = f"id:{film_id}" if film_id else f"title:{re.sub(r'[^a-z0-9]+', '-', search_title.lower()).strip('-')}"
            if key not in aggregated:
                base = dict(film)
                base["showtimes"] = [dict(st) for st in (film.get("showtimes") or [])]
                base["cinema_names"] = set([cinema_name]) if cinema_name else set()
                base["tag_labels_available"] = list(base.get("tag_labels_available") or [])
                base["genres"] = list(base.get("genres") or [])
                aggregated[key] = base
                continue

            current = aggregated[key]
            if cinema_name:
                current["cinema_names"].add(cinema_name)
            current["showtimes"].extend(dict(st) for st in (film.get("showtimes") or []))
            for field in (
                "poster_url",
                "site_image_url",
                "event_banner_url",
                "trailer_url",
                "overview",
                "synopsis",
                "cast",
                "director",
                "writer",
                "imdb_id",
                "film_url",
                "release_date_text",
                "release_date",
                "certificate",
            ):
                if not (current.get(field) or "").strip() and (film.get(field) or "").strip():
                    current[field] = film[field]
            if current.get("runtime_min") is None and film.get("runtime_min") is not None:
                current["runtime_min"] = film.get("runtime_min")
            if current.get("vote_average") is None and film.get("vote_average") is not None:
                current["vote_average"] = film.get("vote_average")
            current["genres"] = sorted(set((current.get("genres") or []) + (film.get("genres") or [])))
            current["tag_labels_available"] = sorted(
                set((current.get("tag_labels_available") or []) + (film.get("tag_labels_available") or []))
            )

    films = []
    for film in aggregated.values():
        seen_showtimes = set()
        deduped_showtimes = []
        for st in sorted(film.get("showtimes") or [], key=lambda s: (s.get("date", ""), s.get("time", ""), s.get("screen", ""), s.get("perf_code", ""))):
            k = (st.get("date"), st.get("time"), st.get("screen"), st.get("perf_code"), st.get("booking_url"))
            if k in seen_showtimes:
                continue
            seen_showtimes.add(k)
            deduped_showtimes.append(st)
        film["showtimes"] = deduped_showtimes
        cinema_names_full = sorted(film.pop("cinema_names", set()))
        cinema_names_short = sorted({short_cinema_name(x) for x in cinema_names_full if short_cinema_name(x)})
        film["cinema_names_list"] = cinema_names_short
        film["cinema_name"] = ", ".join(cinema_names_short)
        films.append(film)
    build_today = datetime.now(timezone.utc).date()
    build_today_iso = build_today.isoformat()
    now_showing_cutoff_iso = (build_today + timedelta(days=NOW_SHOWING_THRESHOLD_DAYS)).isoformat()

    # Collect unique dates for tabs
    all_dates = set()
    for f in films:
        for st in f.get("showtimes") or []:
            all_dates.add(st.get("date", ""))
    sorted_dates = sorted(all_dates) if all_dates else []

    def cert_span(rating: Optional[str]) -> str:
        """Render certificate as a compact text badge."""
        if not rating:
            return ""
        r = rating.upper()
        return f'<span class="cert cert-fallback" aria-label="{r}">{r}</span>'

    def film_card(f: Dict) -> str:
        title = f.get("title", "")
        search_title = f.get("search_title") or extract_search_title(title)
        bbfc = (f.get("certificate") or "").strip() or extract_bbfc_rating(title)
        runtime = f.get("runtime_min")
        release_date_text = (f.get("release_date_text") or "").strip()
        ticket_note = (f.get("ticket_note") or "").strip()
        film_id = (f.get("film_id") or "").strip()
        options_seed = f"{film_id}-{search_title or title}" if film_id else (search_title or title)
        options_id = re.sub(r"[^a-z0-9]+", "-", options_seed.lower()).strip("-") or "film"
        if runtime:
            runtime_str = f"{format_runtime(runtime)} ({runtime} min{'s' if runtime != 1 else ''})"
        else:
            runtime_str = ""
        cast_raw = (f.get("cast") or "").strip()
        cast_parts = [p.strip() for p in cast_raw.split(",") if p.strip()]
        cast_first = cast_parts[:6]
        cast_rest = cast_parts[6:]
        director = (f.get("director") or "").strip()
        writer = (f.get("writer") or "").strip()
        overview = (f.get("overview") or "").strip()
        synopsis = (f.get("synopsis") or "").strip()
        description = overview or synopsis
        description = description[:500] if description else ""
        film_url = f.get("film_url", "")
        film_slug = (f.get("film_slug") or "").strip()
        cinema_name = f.get("cinema_name", "")
        poster_url = f.get("poster_url", "")
        site_image_url = f.get("site_image_url", "")
        event_banner_url = f.get("event_banner_url", "")
        is_live_event = bool(f.get("is_live_event"))
        trailer_url = f.get("trailer_url", "")
        vote = f.get("vote_average")
        if vote is not None:
            pct = min(100, max(0, (vote / 10.0) * 100))
            vote_str = f'<span class="rating-wrap" title="TMDb rating"><span class="rating-bar" aria-hidden="true"><span class="rating-fill" style="width:{pct:.0f}%"></span></span><span class="rating-text">{vote:.1f}/10</span></span>'
        else:
            vote_str = ""
        genres = f.get("genres") or []
        imdb_id = f.get("imdb_id", "")
        imdb_link = f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else f"https://www.imdb.com/find/?q={quote_plus(search_title)}"
        rt_link = f"https://www.rottentomatoes.com/search?search={quote_plus(search_title)}"
        trakt_link = f"https://trakt.tv/search?query={quote_plus(search_title)}"

        all_showtimes_sorted = sorted(
            (f.get("showtimes") or []),
            key=lambda s: (s.get("date", ""), s.get("time", ""), s.get("screen", "")),
        )
        showtimes_display: List[Dict[str, Any]] = []
        showtimes_hidden: List[Dict[str, Any]] = []
        kept_dates: set = set()
        for st in all_showtimes_sorted:
            d = st.get("date", "")
            if not d:
                continue
            if d not in kept_dates and len(kept_dates) >= SHOWTIMES_MAX_DAYS_PER_FILM:
                showtimes_hidden.append(st)
                continue
            if d not in kept_dates:
                kept_dates.add(d)
            if len(showtimes_display) >= initial_showings_visible:
                showtimes_hidden.append(st)
                continue
            showtimes_display.append(st)

        def render_showtime_rows(showtimes: List[Dict[str, Any]]) -> str:
            showtimes_by_date: Dict[str, List[Dict]] = {}
            for st in showtimes:
                d = st.get("date", "")
                if d not in showtimes_by_date:
                    showtimes_by_date[d] = []
                showtimes_by_date[d].append(st)
            rows = []
            for d in sorted(showtimes_by_date.keys()):
                times = showtimes_by_date[d]
                time_parts = []
                for st in times:
                    t = st.get("time", "")
                    screen = short_cinema_name(str(st.get("screen", "")))
                    booking = st.get("booking_url", "")
                    sold_out = bool(st.get("sold_out"))
                    tags = st.get("tags") or []
                    tag_icon_ids = {
                        "Audio Description": "icon-audio-desc",
                        "Wheelchair access": "icon-wheelchair",
                        "2D": "icon-2d",
                        "3D": "icon-3d",
                        "Subtitles": "icon-subtitles",
                        "Silver Screen": "icon-silver-screen",
                        "Event cinema": "icon-event-cinema",
                        "Advance Screening": "icon-event-cinema",
                        "Strobe Light warning": "icon-strobe",
                        "Parent & Baby": "icon-parent-baby",
                        "Autism Friendly": "icon-autism-friendly",
                        "Kids Club": "icon-kids-club",
                    }
                    tag_short_labels = {
                        "Audio Description": "AD",
                        "Subtitles": "Subs",
                        "Wheelchair access": "WA",
                        "Strobe Light warning": "Strobe",
                        "Hard of Hearing": "HOH",
                        "Private Box": "Box",
                        "Super Saver": "Saver",
                    }
                    tag_tooltips = {
                        "Audio Description": "Audio description",
                        "Subtitles": "Subtitled screening",
                        "Wheelchair access": "Wheelchair accessible",
                        "2D": "Standard 2D screening",
                        "Strobe Light warning": "Strobe lighting may affect photosensitive viewers",
                        "Hard of Hearing": "Infrared hard of hearing available",
                    }
                    def tag_html(tag: str) -> str:
                        icon_id = tag_icon_ids.get(tag)
                        label = tag_short_labels.get(tag, tag)
                        tooltip = tag_tooltips.get(tag) or (tag if tag in tag_short_labels else None)
                        title_esc = (tooltip or "").replace("&", "&amp;").replace('"', "&quot;")
                        title_attr = f' title="{title_esc}"' if title_esc else ""
                        if icon_id:
                            return f'<span class="tag"{title_attr}><svg class="tag-icon" aria-hidden="true"><use href="#{icon_id}"/></svg>{label}</span>'
                        return f'<span class="tag"{title_attr}>{label}</span>'
                    tag_span = " ".join(tag_html(tag) for tag in tags[:4])
                    if booking and not sold_out:
                        time_el = f'<a href="{booking}">{t}</a>'
                    elif sold_out:
                        time_el = f'<span class="past">{t} Sold Out</span>'
                    else:
                        time_el = f'<span class="past">{t}</span>'
                    time_parts.append(
                        f'<div class="st-row">'
                        f'<span class="st-time">{time_el}</span>'
                        f'<span class="st-screen">{screen}</span>'
                        f'<span class="st-tags">{tag_span}</span>'
                        f'</div>'
                    )
                date_label = d
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    date_label = dt.strftime("%a %d %b")
                except ValueError:
                    pass
                rows.append(f'<div class="day-group"><div class="st-date">{date_label}</div>' + "".join(time_parts) + "</div>")
            return "\n".join(rows)

        showtimes_html = render_showtime_rows(showtimes_display)
        hidden_showtimes_html = render_showtime_rows(showtimes_hidden)
        all_showtime_dates = sorted({st.get("date", "") for st in all_showtimes_sorted if st.get("date", "")})
        hidden_count = len(showtimes_hidden)
        showtimes_toggle_html = ""
        if hidden_count > 0:
            extra_id = f"showtimes-extra-{options_id}"
            showtimes_toggle_html = (
                f'<div class="showtimes-actions">'
                f'<button type="button" class="showtimes-more-btn" data-target="{extra_id}" data-more-label="Show {hidden_count} more showings" data-less-label="Show fewer showings">Show {hidden_count} more showings</button>'
                f'</div>'
                f'<div id="{extra_id}" class="showtimes-extra" hidden>{hidden_showtimes_html}</div>'
            )

        has_3d = any("3D" in (st.get("tags") or []) for st in (f.get("showtimes") or []))
        poster_src = poster_url or site_image_url or POSTER_PLACEHOLDER_REL
        poster_alt = f"Poster for {title}" if (poster_url or site_image_url) else f"No poster available for {title}"
        poster_inner = f'<img src="{poster_src}" alt="{poster_alt}" loading="lazy"/>'
        if poster_url and has_3d:
            poster_inner += '<i class="icon--hints icon--3d" aria-hidden="true"></i>'
        if not poster_url and not site_image_url:
            poster_inner += '<span class="poster-fallback-label">No poster yet</span>'
        if event_banner_url:
            poster_inner += f'<img src="{event_banner_url}" class="poster-event-banner" alt="Event banner" loading="lazy"/>'
        poster_div = f'<div class="poster">{poster_inner}</div>'
        # YouTube embed URL for lightbox; use nocookie domain and add fallback watch URL for Error 153 (embed disabled)
        trailer_embed = ""
        trailer_watch_esc = ""
        if trailer_url:
            v_match = re.search(r"(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})", trailer_url)
            if v_match:
                vid = v_match.group(1)
                trailer_embed = f"https://www.youtube-nocookie.com/embed/{vid}?autoplay=1&rel=0"
                trailer_watch_esc = trailer_url.replace("&", "&amp;").replace('"', "&quot;")
        trailer_embed_esc = (trailer_embed or "").replace("&", "&amp;").replace('"', "&quot;")
        trailer_a = f'<button type="button" class="trailer trailer-lightbox-trigger" data-embed="{trailer_embed_esc}" data-watch="{trailer_watch_esc}" aria-label="Play trailer">Trailer</button>' if trailer_embed else ""
        genre_span = f'<span class="genres">{", ".join(genres[:4])}</span>' if genres else ""
        # Escape for HTML (e.g. "Smith & Jones" -> "Smith &amp; Jones")
        def esc(s: str) -> str:
            return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        cast_first_esc = ", ".join(esc(a) for a in cast_first)
        cast_rest_esc = ", ".join(esc(a) for a in cast_rest)
        director_esc = esc(director)
        writer_esc = esc(writer)
        description_esc = esc(description)
        release_line = f'<span class="release">Released: {esc(release_date_text)}</span>' if release_date_text else ""
        note_html = f'<p class="crew"><strong>Booking Notes:</strong> {esc(ticket_note)}</p>' if ticket_note else ""

        film_data_value = f"{film_id}-{film_slug}" if film_id and film_slug else film_slug
        cinema_links: Dict[str, str] = {}
        for st in f.get("showtimes") or []:
            cu = (st.get("cinema_url") or "").strip()
            cn = (st.get("cinema_name") or st.get("screen") or "").strip()
            if not cu or not cn:
                continue
            if film_data_value:
                cinema_links[short_cinema_name(cn)] = urljoin(cu, f"/film/{film_data_value}")
            else:
                cinema_links[short_cinema_name(cn)] = film_url or cu
        options_html = "".join(
            f'<a href="{u}" class="cinema-option-link" target="_blank" rel="noopener">{esc(n)}</a>'
            for n, u in sorted(cinema_links.items())
        )
        chooser_html = f'<div id="film-page-options-{options_id}" class="film-page-options" hidden>{options_html}</div>'
        meta_lines = []
        if director_esc:
            meta_lines.append(f'<p class="crew"><strong>Director:</strong> {director_esc}</p>')
        if writer_esc:
            meta_lines.append(f'<p class="crew"><strong>Writer(s):</strong> {writer_esc}</p>')
        if cast_first_esc or cast_rest_esc:
            cast_rest_html = f'<span class="cast-rest" hidden>, {cast_rest_esc}</span>' if cast_rest_esc else ""
            more_btn = f' <button type="button" class="cast-more-btn">More</button>' if cast_rest_esc else ""
            meta_lines.append(f'<p class="cast"><strong>Starring:</strong> {cast_first_esc}{cast_rest_html}{more_btn}</p>')
        crew_html = "\n      ".join(meta_lines)

        earliest = min(all_showtime_dates) if all_showtime_dates else ""
        status = "now" if earliest and build_today_iso <= earliest <= now_showing_cutoff_iso else "coming-soon"
        status_label = "Now Showing" if status == "now" else "Coming Soon"
        if is_live_event:
            status_label = "Live Event"
        cinema_line = f'<p class="crew"><strong>Cinema:</strong> {esc(cinema_name)}</p>' if cinema_name else ""
        return f"""
<article class="film-card" data-dates="{",".join(all_showtime_dates)}" data-status="{status}">
  <span class="status-pill status-pill--{status}">{status_label}</span>
  <div class="film-header">
    {poster_div}
    <div class="film-meta">
      <h2>{title} {cert_span(bbfc)}</h2>
      <div class="meta-line">{runtime_str} {release_line} {vote_str} {genre_span}</div>
      {trailer_a}
      {cinema_line}
      {crew_html}
      <p class="synopsis">{description_esc}</p>
      {note_html}
      <div class="links">
        <button type="button" class="btn book film-page-trigger" data-options-id="film-page-options-{options_id}">Film page</button>
        <a href="{imdb_link}" class="link ext-link" target="_blank" rel="noopener" title="IMDb"><svg class="ext-logo" aria-hidden="true"><use href="#imdb-logo"/></svg> IMDb</a>
        <a href="{rt_link}" class="link ext-link" target="_blank" rel="noopener" title="Rotten Tomatoes"><svg class="ext-logo" aria-hidden="true"><use href="#rt-logo"/></svg> RT</a>
        <a href="{trakt_link}" class="link ext-link" target="_blank" rel="noopener" title="Trakt"><svg class="ext-logo" aria-hidden="true"><use href="#trakt-logo"/></svg> Trakt</a>
      </div>
      {chooser_html}
    </div>
  </div>
  <div class="showtimes">{showtimes_html}{showtimes_toggle_html}</div>
</article>"""

    films_sorted = sorted(films, key=lambda f: len(f.get("showtimes") or []), reverse=True)
    now_showing = [
        f
        for f in films_sorted
        if f.get("showtimes")
        and build_today_iso <= min(st.get("date", "9999") for st in f["showtimes"]) <= now_showing_cutoff_iso
    ]
    coming_soon = [f for f in films_sorted if f not in now_showing]
    now_cards = "\n".join(film_card(f) for f in now_showing)
    coming_cards = "\n".join(film_card(f) for f in coming_soon)
    section_now = (
        f'<section class="film-section film-section--now" data-section="now">\n'
        f'  <div class="section-title-wrap">\n'
        f'    <h3 class="section-title" data-section="now">Now Showing</h3>\n'
        f'    <span class="section-count">{len(now_showing)} films</span>\n'
        f'  </div>\n'
        f'{now_cards}\n'
        f'</section>'
    ) if now_showing else ""
    section_coming = (
        f'<section class="film-section film-section--coming" data-section="coming">\n'
        f'  <div class="section-title-wrap">\n'
        f'    <h3 class="section-title" data-section="coming">Coming Soon</h3>\n'
        f'    <span class="section-count">{len(coming_soon)} films</span>\n'
        f'  </div>\n'
        f'{coming_cards}\n'
        f'</section>'
    ) if coming_soon else ""
    cards_html = "\n".join(s for s in (section_now, section_coming) if s)

    # Date filter tabs
    tabs = ['<button type="button" class="tab active" data-date="all">All</button>']
    for d in sorted_dates[:14]:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            label = dt.strftime("%a %d")
            if d == build_today_iso:
                label = "Today"
            tabs.append(f'<button type="button" class="tab" data-date="{d}">{label}</button>')
        except ValueError:
            tabs.append(f'<button type="button" class="tab" data-date="{d}">{d}</button>')
    tabs_html = "\n".join(tabs)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>What's on at Merlin Cinemas Cornwall — ratings, trailers &amp; links</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
  <style>
    :root {{
      --bg: #0a0a0f;
      --card-bg: #12121a;
      --surface: #12121a;
      --surface-2: #12121a;
      --surface-3: #1a1a24;
      --border: rgba(168,85,247,0.25);
      --text: #e2e8f0;
      --text-muted: #94a3b8;
      --cyan: #00d4ff;
      --purple: #a855f7;
      --accent: #00d4ff;
      --accent-dim: rgba(0,212,255,0.15);
      --accent-glow: rgba(0,212,255,0.25);
      --radius: 16px;
      --radius-sm: 10px;
      --radius-lg: 24px;
      --transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      font-family: 'Space Grotesk', system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.6;
      min-height: 100vh;
      overflow-x: hidden;
      -webkit-font-smoothing: antialiased;
    }}
    .bg-mesh {{
      position: fixed;
      inset: 0;
      background:
        radial-gradient(ellipse 100% 80% at 50% -30%, var(--accent-dim) 0%, transparent 50%),
        radial-gradient(ellipse 60% 50% at 80% 100%, rgba(0,212,255,0.08) 0%, transparent 40%),
        radial-gradient(ellipse 40% 40% at 10% 90%, rgba(168,85,247,0.05) 0%, transparent 50%);
      pointer-events: none;
      z-index: 0;
    }}
    .bg-grid {{
      position: fixed;
      inset: 0;
      background-image: linear-gradient(rgba(255,255,255,0.02) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.02) 1px, transparent 1px);
      background-size: 60px 60px;
      pointer-events: none;
      z-index: 0;
    }}
    .page {{ position: relative; z-index: 1; max-width: 1400px; margin: 0 auto; padding: 2rem 1.25rem 4rem; }}
    @media (min-width: 640px) {{ .page {{ padding: 3rem 2rem 5rem; }} }}
    header {{
      text-align: center;
      padding: 3rem 0 2rem;
      border-bottom: 1px solid var(--border);
      animation: fadeUp 0.8s ease-out;
    }}
    @keyframes fadeUp {{
      from {{ opacity: 0; transform: translateY(20px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    header h1 {{
      font-size: clamp(2rem, 5vw, 2.5rem);
      font-weight: 800;
      letter-spacing: -0.04em;
      background: linear-gradient(90deg, var(--cyan), var(--purple));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }}
    header p {{ color: var(--text-muted); font-size: 0.95rem; margin-top: 0.25rem; }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 0.5rem; justify-content: center; padding: 1rem 0; }}
    .tab {{
      font-family: inherit;
      background: var(--surface-2);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 0.5rem 0.75rem;
      border-radius: var(--radius-sm);
      cursor: pointer;
      font-size: 0.9rem;
      transition: all var(--transition);
    }}
    .tab:hover {{ border-color: var(--cyan); }}
    .tab.active {{
      background: linear-gradient(135deg, var(--accent-dim), rgba(168,85,247,0.15));
      border-color: var(--cyan);
    }}
    #films {{ display: grid; grid-template-columns: 1fr; gap: 1.5rem; }}
    .film-section {{
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: 1fr;
      gap: 1rem;
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      padding: 1rem;
      background: linear-gradient(160deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
    }}
    @media (min-width: 900px) {{
      .film-section {{ grid-template-columns: repeat(2, 1fr); }}
    }}
    .film-section--now {{
      border-color: rgba(0,212,255,0.45);
      box-shadow: inset 0 0 0 1px rgba(0,212,255,0.08);
    }}
    .film-section--coming {{
      border-color: rgba(168,85,247,0.45);
      box-shadow: inset 0 0 0 1px rgba(168,85,247,0.1);
    }}
    .section-title-wrap {{
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
      padding: 0.9rem 1rem;
      border-radius: var(--radius);
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.02);
    }}
    .film-section--now .section-title-wrap {{
      border-color: rgba(0,212,255,0.5);
      background: linear-gradient(90deg, rgba(0,212,255,0.2), rgba(0,212,255,0.06));
    }}
    .film-section--coming .section-title-wrap {{
      border-color: rgba(168,85,247,0.5);
      background: linear-gradient(90deg, rgba(168,85,247,0.24), rgba(168,85,247,0.08));
    }}
    .section-title {{
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      margin: 0;
      text-transform: uppercase;
    }}
    .film-section--now .section-title {{ color: var(--cyan); }}
    .film-section--coming .section-title {{ color: #cf90ff; }}
    .section-count {{ font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: var(--text); opacity: 0.95; }}
    .film-card {{
      background: linear-gradient(135deg, rgba(255,255,255,0.04) 0%, rgba(255,255,255,0.01) 100%);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1.25rem;
      transition: all var(--transition);
      position: relative;
      overflow: hidden;
      animation: fadeUp 0.6s ease-out backwards;
    }}
    .status-pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 1.7rem;
      padding: 0.2rem 0.55rem;
      border-radius: 999px;
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.07em;
      text-transform: uppercase;
      border: 1px solid transparent;
      margin-bottom: 0.7rem;
    }}
    .status-pill--now {{ background: rgba(0,212,255,0.16); border-color: rgba(0,212,255,0.45); color: #8beeff; }}
    .status-pill--coming-soon {{ background: rgba(168,85,247,0.2); border-color: rgba(168,85,247,0.5); color: #e0b8ff; }}
    .film-card::before {{
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      height: 2px;
      background: linear-gradient(90deg, transparent, var(--cyan), transparent);
      opacity: 0;
      transition: opacity var(--transition);
    }}
    .film-card:hover {{
      border-color: rgba(0,212,255,0.4);
      transform: translateY(-4px);
      box-shadow: 0 20px 40px rgba(0,0,0,0.4), 0 0 0 1px rgba(0,212,255,0.1);
    }}
    .film-card:hover::before {{ opacity: 1; }}
    .film-header {{ display: flex; gap: 1.25rem; flex-wrap: wrap; }}
    .poster {{ position: relative; flex-shrink: 0; }}
    .poster img {{ width: 210px; height: 315px; object-fit: cover; border-radius: var(--radius-sm); box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
    .poster .poster-event-banner {{ position: absolute; left: 0.4rem; right: 0.4rem; bottom: 0.4rem; width: calc(100% - 0.8rem); height: 2rem; object-fit: contain; background: rgba(0,0,0,0.45); padding: 0.2rem; border-radius: 6px; box-shadow: none; }}
    .poster-fallback-label {{
      position: absolute;
      left: 0.5rem;
      right: 0.5rem;
      bottom: 0.55rem;
      padding: 0.2rem 0.4rem;
      border-radius: 6px;
      background: rgba(0, 0, 0, 0.62);
      color: #e2e8f0;
      font-size: 0.72rem;
      text-align: center;
      letter-spacing: 0.02em;
    }}
    .poster .icon--hints {{ position: absolute; right: 0; top: 0; width: 105px; height: 105px; pointer-events: none; }}
    .poster .icon--hints.icon--3d {{ background: url(icons/3D-Performance.png) no-repeat; background-size: 100% auto; background-position: top right; }}
    .film-meta {{ flex: 1; min-width: 200px; }}
    .film-meta h2 {{ font-size: 1.25rem; margin: 0 0 0.5rem; display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }}
    .cert {{ margin-right: 6px; vertical-align: middle; }}
    .cert-fallback {{ min-width: 2.1rem; padding: 0.18rem 0.45rem; background: var(--surface-3); color: #fff; font-size: 0.65rem; font-weight: 700; display: inline-flex; align-items: center; justify-content: center; border-radius: 4px; border: 1px solid rgba(255,255,255,0.25); }}
    .meta-line {{ color: var(--text-muted); font-size: 0.9rem; margin-bottom: 0.5rem; display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 1rem; }}
    .rating-wrap {{ display: inline-flex; align-items: center; gap: 0.4rem; }}
    .rating-bar {{ display: block; width: 3rem; height: 0.5rem; background: rgba(255,255,255,0.25); border-radius: 3px; overflow: hidden; }}
    .rating-fill {{ display: block; height: 0.5rem; background: linear-gradient(90deg, #00d4ff, #a855f7); border-radius: 3px; transition: width 0.2s; }}
    .rating-text {{ font-variant-numeric: tabular-nums; font-size: 0.85em; color: var(--cyan); }}
    .genres {{ color: var(--purple); }}
    .trailer {{ display: inline-block; margin-bottom: 0.5rem; color: var(--cyan); font-size: 0.9rem; background: none; border: none; cursor: pointer; font-family: inherit; padding: 0; text-decoration: underline; }}
    .trailer:hover {{ color: var(--purple); }}
    .trailer-lightbox {{ position: fixed; inset: 0; z-index: 1000; display: none; align-items: center; justify-content: center; padding: 1rem; box-sizing: border-box; }}
    .trailer-lightbox.is-open {{ display: flex; }}
    .trailer-lightbox-backdrop {{ position: absolute; inset: 0; background: rgba(0,0,0,0.85); cursor: pointer; }}
    .trailer-lightbox-inner {{ position: relative; width: 100%; max-width: 90vw; max-height: 90vh; aspect-ratio: 16/9; background: #000; border-radius: var(--radius); box-shadow: 0 0 40px var(--accent-glow); overflow: hidden; }}
    .trailer-lightbox-inner iframe {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; border: none; }}
    .trailer-lightbox-close {{ position: absolute; top: -2.5rem; right: 0; background: var(--surface-2); border: 1px solid var(--border); color: var(--text); width: 2rem; height: 2rem; border-radius: var(--radius-sm); cursor: pointer; font-size: 1.25rem; line-height: 1; display: flex; align-items: center; justify-content: center; z-index: 1; transition: all var(--transition); }}
    .trailer-lightbox-close:hover {{ border-color: var(--cyan); color: var(--cyan); }}
    .trailer-lightbox-fallback {{ position: absolute; bottom: 0.5rem; left: 0.5rem; font-size: 0.85rem; color: var(--cyan); }}
    .trailer-lightbox-fallback:hover {{ color: var(--purple); }}
    .film-page-modal {{ position: fixed; inset: 0; z-index: 1100; display: none; align-items: center; justify-content: center; padding: 1rem; }}
    .film-page-modal.is-open {{ display: flex; }}
    .film-page-modal-backdrop {{ position: absolute; inset: 0; background: rgba(0,0,0,0.82); cursor: pointer; }}
    .film-page-modal-inner {{ position: relative; width: min(28rem, 100%); background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 1rem; box-shadow: 0 0 30px var(--accent-glow); }}
    .film-page-modal-title {{ margin: 0 0 0.65rem; font-size: 1rem; }}
    .film-page-modal-list {{ display: grid; gap: 0.5rem; }}
    .film-page-modal-list a {{ text-decoration: none; color: var(--text); border: 1px solid var(--border); border-radius: var(--radius-sm); padding: 0.55rem 0.7rem; background: rgba(255,255,255,0.03); }}
    .film-page-modal-list a:hover {{ border-color: var(--cyan); color: var(--cyan); }}
    .film-page-modal-close {{ position: absolute; top: 0.4rem; right: 0.4rem; background: transparent; border: 1px solid var(--border); color: var(--text); border-radius: 8px; width: 1.9rem; height: 1.9rem; cursor: pointer; }}
    .film-meta .crew {{ font-size: 0.9rem; color: var(--text-muted); margin: 0; padding: 0.5rem 0; border-bottom: 1px solid var(--border); }}
    .film-meta .crew:first-of-type {{ padding-top: 0; }}
    .film-meta .cast {{ font-size: 0.9rem; color: var(--text-muted); margin: 0; padding: 0.5rem 0; border-bottom: 1px solid var(--border); }}
    .film-meta .synopsis {{ font-size: 0.9rem; color: var(--text-muted); margin: 0; padding: 0.75rem 0 0.5rem; line-height: 1.5; max-width: 56em; border-top: 1px solid var(--border); }}
    .links {{ margin-top: 0.75rem; display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center; }}
    .links a, .links button {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.5rem 0.75rem;
      border-radius: var(--radius-sm);
      font-size: 0.9rem;
      text-decoration: none;
      transition: all var(--transition);
    }}
    .links .btn {{
      background: linear-gradient(135deg, var(--cyan), var(--purple));
      color: var(--bg);
      font-weight: 600;
      border: none;
      cursor: pointer;
      font-family: inherit;
    }}
    .links .btn:hover {{
      background: linear-gradient(135deg, #20dfff, #b366ff);
      transform: scale(1.02);
      box-shadow: 0 4px 20px var(--accent-glow);
    }}
    .links .link {{ color: var(--accent); background: rgba(255,255,255,0.06); border: 1px solid var(--border); }}
    .links .link:hover {{ background: rgba(0,212,255,0.12); border-color: var(--cyan); color: var(--purple); }}
    .ext-logo {{ width: 18px; height: 18px; flex-shrink: 0; }}
    .showtimes {{ margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--border); font-size: 0.9rem; }}
    .day-group {{ margin-bottom: 0.75rem; }}
    .day-group:last-child {{ margin-bottom: 0; }}
    .st-date {{ font-weight: 600; margin-bottom: 0.25rem; color: var(--text); }}
    .st-row {{ display: grid; grid-template-columns: 5.5rem minmax(8rem, 1fr) 2fr; gap: 0 0.75rem; align-items: center; margin-bottom: 0.2rem; }}
    .st-row:last-child {{ margin-bottom: 0; }}
    .st-time {{ font-variant-numeric: tabular-nums; }}
    .st-time a, .showtime a {{ color: var(--cyan); }}
    .st-time .past {{ color: var(--text-muted); }}
    .st-screen {{ color: var(--text-muted); }}
    .st-tags {{ display: flex; align-items: center; flex-wrap: wrap; gap: 0.25rem; }}
    .showtimes-actions {{ margin-top: 0.75rem; }}
    .showtimes-more-btn {{
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.05);
      color: var(--cyan);
      border-radius: 8px;
      padding: 0.35rem 0.6rem;
      font-size: 0.85rem;
      cursor: pointer;
      font-family: inherit;
    }}
    .showtimes-more-btn:hover {{ border-color: var(--cyan); background: rgba(0,212,255,0.12); }}
    .showtimes-extra {{ margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px dashed var(--border); }}
    .cast-more-btn {{ background: none; border: none; color: var(--cyan); cursor: pointer; font-size: 0.85em; padding: 0 0.25rem; font-family: inherit; }}
    .cast-more-btn:hover {{ text-decoration: underline; }}
    .tag {{ font-size: 0.75rem; color: var(--text-muted); margin-left: 0.25rem; display: inline-flex; align-items: center; gap: 0.25rem; }}
    .tag-icon {{ width: 14px; height: 14px; flex-shrink: 0; vertical-align: middle; }}
    .cal-link {{ color: var(--purple); text-decoration: none; margin-left: 0.25rem; }}
    footer {{
      margin-top: 4rem;
      padding-top: 2.5rem;
      border-top: 1px solid var(--border);
      text-align: center;
      color: var(--text-muted);
      font-size: 0.85rem;
      animation: fadeUp 0.6s ease-out backwards;
    }}
    footer a {{ color: var(--accent); text-decoration: none; font-weight: 500; transition: color var(--transition); }}
    footer a:hover {{ color: var(--purple); }}
    .footer-disclaimer {{ font-size: 0.9rem; max-width: 36rem; margin: 0 auto 1rem; line-height: 1.6; }}
    .footer-links {{ display: flex; flex-wrap: wrap; justify-content: center; gap: 0.5rem 1.5rem; margin-bottom: 1rem; }}
    .footer-attribution {{ font-size: 0.8rem; opacity: 0.85; margin: 0; line-height: 1.5; }}
  </style>
</head>
<body>
  <div class="bg-mesh"></div>
  <div class="bg-grid"></div>
  <div id="trailer-lightbox" class="trailer-lightbox" aria-hidden="true" role="dialog" aria-modal="true" aria-label="Trailer video">
    <div class="trailer-lightbox-backdrop" id="trailer-lightbox-backdrop"></div>
    <div class="trailer-lightbox-inner">
      <button type="button" class="trailer-lightbox-close" id="trailer-lightbox-close" aria-label="Close">×</button>
      <iframe id="trailer-lightbox-iframe" title="Trailer" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
      <a id="trailer-lightbox-fallback" class="trailer-lightbox-fallback" href="#" target="_blank" rel="noopener">Watch on YouTube</a>
    </div>
  </div>
  <div id="film-page-modal" class="film-page-modal" aria-hidden="true" role="dialog" aria-modal="true" aria-label="Choose cinema">
    <div class="film-page-modal-backdrop" id="film-page-modal-backdrop"></div>
    <div class="film-page-modal-inner">
      <button type="button" class="film-page-modal-close" id="film-page-modal-close" aria-label="Close">×</button>
      <h3 class="film-page-modal-title">Choose a cinema</h3>
      <div id="film-page-modal-list" class="film-page-modal-list"></div>
    </div>
  </div>
  <svg xmlns="http://www.w3.org/2000/svg" style="position:absolute;width:0;height:0;">
    <defs>
      <symbol id="imdb-logo" viewBox="0 0 64 32"><rect width="64" height="32" rx="2" fill="#F5C518"/><text x="32" y="21" text-anchor="middle" font-family="Arial,sans-serif" font-size="14" font-weight="bold" fill="#000">imdb</text></symbol>
      <symbol id="rt-logo" viewBox="0 0 32 32"><circle cx="16" cy="17" r="11" fill="#E50914"/><path d="M16 6 L18 4 L20 6 L18 8 L16 6" fill="#00B140"/><ellipse cx="16" cy="7" rx="4" ry="2.5" fill="#00B140"/></symbol>
      <symbol id="trakt-logo" viewBox="0 0 32 32"><rect x="6" y="8" width="20" height="16" rx="3" fill="none" stroke="#ED1C24" stroke-width="2"/><path d="M14 12 L14 20 M18 12 L18 20 M14 16 L18 16" stroke="#ED1C24" stroke-width="1.5" fill="none"/></symbol>
      <symbol id="icon-audio-desc" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 18v-6a9 9 0 0 1 18 0v6"/><path d="M21 19a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3zM3 19a2 2 0 0 0 2 2h1a2 2 0 0 0 2-2v-3a2 2 0 0 0-2-2H3z"/></symbol>
      <symbol id="icon-wheelchair" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="1"/><path d="M19 13v3h-2"/><path d="M6 21a3 3 0 0 1 0-6 2 2 0 0 1 2 2v4"/><path d="M6 21a5 5 0 0 0 5-5v-3h2"/><circle cx="18" cy="16" r="4"/><path d="M14 10h4v4"/></symbol>
      <symbol id="icon-2d" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="14" rx="1"/><path d="M7 12h4M7 16h6"/></symbol>
      <symbol id="icon-3d" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 8v8M4 8l8-4 8 4-8 4zM4 8l8 4M20 8l-8 4M12 12v8"/></symbol>
      <symbol id="icon-subtitles" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="6" width="20" height="12" rx="1"/><path d="M6 12h.01M10 12h.01M14 12h.01M18 12h.01M6 16h12"/></symbol>
      <symbol id="icon-silver-screen" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15 9 22 9 17 14 18 22 12 18 6 22 7 14 2 9 9 9"/></symbol>
      <symbol id="icon-event-cinema" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/></symbol>
      <symbol id="icon-strobe" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></symbol>
      <symbol id="icon-parent-baby" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4"/><circle cx="17" cy="11" r="2.5"/><path d="M17 13.5v4M15 18h4"/></symbol>
      <symbol id="icon-autism-friendly" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v8M8 12h8"/></symbol>
      <symbol id="icon-kids-club" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="8" r="3"/><path d="M5 21v-2a4 4 0 0 1 4-4h0"/><circle cx="15" cy="10" r="2"/><path d="M15 21v-2a2 2 0 0 0-2-2h0"/></symbol>
    </defs>
  </svg>
  <div class="page">
    <header>
      <h1>What's on at Merlin Cinemas Cornwall</h1>
      <p>Ratings, trailers &amp; links to IMDb, RT and Trakt</p>
    </header>
    <div class="tabs">{tabs_html}</div>
    <main id="films">{cards_html}</main>
    <footer>
      <p class="footer-disclaimer">An open source fan-made project. Not affiliated with Merlin Cinemas.</p>
      <div class="footer-links">
        <a href="https://www.merlincinemas.co.uk/">Merlin Cinemas</a>
      </div>
      <p class="footer-attribution">
        Posters and ratings via <a href="https://www.themoviedb.org/" target="_blank" rel="noopener">TMDb</a>. This product uses the TMDB API but is not endorsed or certified by TMDB.
      </p>
    </footer>
  </div>
  <script>
    document.querySelectorAll('.tab').forEach(function(btn) {{
      btn.addEventListener('click', function() {{
        document.querySelectorAll('.tab').forEach(function(b) {{ b.classList.remove('active'); }});
        btn.classList.add('active');
        var date = btn.getAttribute('data-date');
        var isAll = date === 'all';
        var sectionVisibility = {{ now: false, coming: false }};
        document.querySelectorAll('.film-card').forEach(function(card) {{
          var dates = (card.getAttribute('data-dates') || '').split(',');
          var show = isAll || dates.indexOf(date) !== -1;
          card.style.display = show ? 'block' : 'none';
          if (show) {{
            var status = card.getAttribute('data-status') || '';
            if (status === 'now') sectionVisibility.now = true;
            if (status === 'coming-soon') sectionVisibility.coming = true;
          }}
        }});
        document.querySelectorAll('.film-section').forEach(function(section) {{
          var sectionType = section.getAttribute('data-section') || '';
          var showSection = sectionType === 'now' ? sectionVisibility.now : sectionVisibility.coming;
          section.style.display = showSection ? 'grid' : 'none';
        }});
      }});
    }});
    document.querySelectorAll('.cast-more-btn').forEach(function(btn) {{
      btn.addEventListener('click', function() {{
        var rest = btn.previousElementSibling;
        if (rest && rest.classList.contains('cast-rest')) {{
          var on = rest.hasAttribute('hidden');
          if (on) {{ rest.removeAttribute('hidden'); btn.textContent = 'Less'; }}
          else {{ rest.setAttribute('hidden', ''); btn.textContent = 'More'; }}
        }}
      }});
    }});
    document.querySelectorAll('.showtimes-more-btn').forEach(function(btn) {{
      btn.addEventListener('click', function() {{
        var targetId = btn.getAttribute('data-target');
        var target = targetId ? document.getElementById(targetId) : null;
        if (!target) return;
        var isHidden = target.hasAttribute('hidden');
        if (isHidden) {{
          target.removeAttribute('hidden');
          btn.textContent = btn.getAttribute('data-less-label') || 'Show fewer showings';
        }} else {{
          target.setAttribute('hidden', '');
          btn.textContent = btn.getAttribute('data-more-label') || 'Show more showings';
        }}
      }});
    }});
    (function() {{
      var lb = document.getElementById('trailer-lightbox');
      var iframe = document.getElementById('trailer-lightbox-iframe');
      var backdrop = document.getElementById('trailer-lightbox-backdrop');
      var closeBtn = document.getElementById('trailer-lightbox-close');
      var fallbackLink = document.getElementById('trailer-lightbox-fallback');
      function closeLightbox() {{
        lb.classList.remove('is-open');
        lb.setAttribute('aria-hidden', 'true');
        iframe.src = '';
        if (fallbackLink) fallbackLink.href = '#';
      }}
      function openLightbox(embedUrl, watchUrl) {{
        iframe.src = embedUrl;
        if (fallbackLink && watchUrl) fallbackLink.href = watchUrl;
        lb.classList.add('is-open');
        lb.setAttribute('aria-hidden', 'false');
      }}
      document.querySelectorAll('.trailer-lightbox-trigger').forEach(function(btn) {{
        btn.addEventListener('click', function() {{
          var embedUrl = this.getAttribute('data-embed');
          var watchUrl = this.getAttribute('data-watch') || '';
          if (embedUrl) openLightbox(embedUrl, watchUrl);
        }});
      }});
      if (backdrop) backdrop.addEventListener('click', closeLightbox);
      if (closeBtn) closeBtn.addEventListener('click', closeLightbox);
      document.addEventListener('keydown', function(e) {{
        if (e.key === 'Escape' && lb.classList.contains('is-open')) closeLightbox();
      }});
    }})();
    (function() {{
      var modal = document.getElementById('film-page-modal');
      var modalList = document.getElementById('film-page-modal-list');
      var modalBackdrop = document.getElementById('film-page-modal-backdrop');
      var modalClose = document.getElementById('film-page-modal-close');
      function closeCinemaModal() {{
        modal.classList.remove('is-open');
        modal.setAttribute('aria-hidden', 'true');
        modalList.innerHTML = '';
      }}
      document.querySelectorAll('.film-page-trigger').forEach(function(btn) {{
        btn.addEventListener('click', function() {{
          var optionsId = this.getAttribute('data-options-id');
          var options = document.getElementById(optionsId);
          if (!options) return;
          modalList.innerHTML = options.innerHTML || '';
          modal.classList.add('is-open');
          modal.setAttribute('aria-hidden', 'false');
        }});
      }});
      if (modalBackdrop) modalBackdrop.addEventListener('click', closeCinemaModal);
      if (modalClose) modalClose.addEventListener('click', closeCinemaModal);
      document.addEventListener('keydown', function(e) {{
        if (e.key === 'Escape' && modal.classList.contains('is-open')) closeCinemaModal();
      }});
    }})();
  </script>
</body>
</html>
"""
    return html


def main() -> None:
    scrape_date = datetime.now(timezone.utc)
    previous_data: Optional[Dict[str, Any]] = None
    if Path(DATA_FILE).exists():
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                previous_data = json.load(f)
        except Exception as e:
            logger.warning("Could not read previous data file %s: %s", DATA_FILE, e)

    detail_cache = load_merlin_detail_cache()
    data, detail_cache_updated, scrape_diagnostics, failed_cinemas, active_cinema_slugs = scrape_whats_on(
        scrape_date, detail_cache=detail_cache
    )
    if detail_cache_updated:
        save_merlin_detail_cache(detail_cache)
    restored_cinemas = apply_previous_cinema_fallback(data, failed_cinemas, previous_data)
    if restored_cinemas:
        logger.warning("Restored previous cinema data after scrape failure for: %s", ", ".join(sorted(restored_cinemas)))

    failure_state = load_cinema_failure_state()
    failure_state, failure_state_changed = update_cinema_failure_state(
        failure_state, failed_cinemas, scrape_date, active_cinema_slugs
    )
    if failure_state_changed:
        save_cinema_failure_state(failure_state)

    excluded_raw = os.environ.get("HEALTH_EXCLUDED_CINEMAS", "")
    excluded_cinemas = {c.strip().lower() for c in excluded_raw.split(",") if c.strip()}
    max_consecutive_failures = _env_int(
        "MAX_CONSECUTIVE_CINEMA_FAILURES",
        "WTW_MAX_CONSECUTIVE_CINEMA_FAILURES",
        default=MAX_CONSECUTIVE_CINEMA_FAILURES_DEFAULT,
    )
    repeat_failures = [
        slug for slug, _err in failed_cinemas.items()
        if slug.lower() not in excluded_cinemas
        and int((failure_state.get("cinemas", {}).get(slug, {}) or {}).get("consecutive_failures") or 0) >= max_consecutive_failures
    ]
    if repeat_failures:
        details = []
        for slug in sorted(repeat_failures):
            row = (failure_state.get("cinemas", {}).get(slug, {}) or {})
            details.append(f"{slug} (consecutive_failures={row.get('consecutive_failures')}, last_error={row.get('last_error')})")
        raise RuntimeError(
            "Consecutive cinema scrape failures reached threshold: "
            + "; ".join(details)
        )

    if failed_cinemas:
        logger.warning(
            "Transient cinema scrape failures tolerated this run: %s",
            ", ".join(sorted(failed_cinemas.keys())),
        )

    validate_scrape_health(data, scrape_diagnostics, scrape_date)

    fingerprint = compute_fingerprint(data)
    prev_fingerprint = ""
    if Path(FINGERPRINT_FILE).exists():
        prev_fingerprint = Path(FINGERPRINT_FILE).read_text(encoding="utf-8").strip()
    unchanged = (
        fingerprint == prev_fingerprint
        and Path(DATA_FILE).exists()
        and Path(SITE_DIR, "index.html").exists()
    )
    force_rebuild = os.environ.get("FORCE_REBUILD", "").strip().lower() in {"1", "true", "yes"}
    if unchanged and not force_rebuild and not failure_state_changed:
        logger.info("Fingerprint unchanged; skipping TMDb enrichment and HTML rebuild.")
        return

    all_films = [film for cinema in (data.get("cinemas") or {}).values() for film in (cinema.get("films") or [])]
    films_by_tmdb_key: Dict[str, List[Dict[str, Any]]] = {}
    for film in all_films:
        key = _tmdb_cache_key(film)
        films_by_tmdb_key.setdefault(key, []).append(film)

    enrichment_fields = (
        "poster_url", "trailer_url", "runtime_min", "vote_average", "genres",
        "imdb_id", "overview", "director", "writer", "cast",
    )

    api_key = os.environ.get("TMDB_API_KEY")
    tmdb_cache = load_tmdb_cache()
    if api_key:
        for group in films_by_tmdb_key.values():
            film = group[0]
            film["film_slug"] = film.get("film_slug") or slug_from_film_url(film.get("film_url", ""))
            enrich_film_tmdb(film, api_key, tmdb_cache)
            for other in group[1:]:
                for key in enrichment_fields:
                    if other.get(key) in (None, "", []) and film.get(key) not in (None, "", []):
                        other[key] = film.get(key)
        save_tmdb_cache(tmdb_cache)
    else:
        logger.info("TMDB_API_KEY not set; skipping TMDb enrichment")
        # Preserve poster and other TMDb fields from last run so posters don't disappear
        if previous_data:
            try:
                old_all_films = [
                    f for c in (previous_data.get("cinemas") or {}).values() for f in (c.get("films") or [])
                ]
                old_films = {f.get("film_url"): f for f in old_all_films if f.get("film_url")}
                old_films_by_key = {
                    _tmdb_cache_key(f): f
                    for f in old_all_films
                }
                for film in all_films:
                    old = old_films_by_key.get(_tmdb_cache_key(film)) or old_films.get(film.get("film_url"))
                    if old:
                        for key in enrichment_fields:
                            if film.get(key) in (None, "", []) and old.get(key):
                                film[key] = old[key]
            except Exception as e:
                logger.warning("Could not merge previous TMDb data: %s", e)

    # Ensure search_title and default enrichment keys
    for film in all_films:
        film.setdefault("search_title", extract_search_title(film.get("title", "")))
        film.setdefault("poster_url", film.get("poster_url") or "")
        film.setdefault("trailer_url", "")
        film.setdefault("runtime_min", film.get("runtime_min"))
        film.setdefault("vote_average", None)
        film.setdefault("genres", [])
        film.setdefault("imdb_id", "")
        film.setdefault("overview", "")
        film.setdefault("director", "")
        film.setdefault("writer", "")

    # Download TMDb posters once per unique film key, then fan-out to duplicates.
    poster_targets: List[Tuple[str, str, str]] = []
    for key, group in films_by_tmdb_key.items():
        film = group[0]
        poster_url = film.get("poster_url") or ""
        if not poster_url.startswith("http"):
            continue
        slug = film.get("film_slug") or key
        poster_targets.append((key, poster_url, slug))

    poster_locals: Dict[str, str] = {}
    if poster_targets:
        poster_workers = max(1, min(8, len(poster_targets)))
        with ThreadPoolExecutor(max_workers=poster_workers) as executor:
            future_map = {
                executor.submit(_download_poster, poster_url, slug): key
                for key, poster_url, slug in poster_targets
            }
            for future in as_completed(future_map):
                key = future_map[future]
                local = future.result()
                if local:
                    poster_locals[key] = local

    for key, local in poster_locals.items():
        for other in films_by_tmdb_key.get(key, []):
            other["poster_url"] = local
    _ensure_placeholder_poster()

    missing_posters = [f.get("title", "") for f in all_films if not (f.get("poster_url") or "").strip()]
    if missing_posters:
        logger.warning("Missing posters for %d film(s): %s", len(missing_posters), ", ".join(missing_posters))
    fail_threshold_raw = os.environ.get("POSTER_MISSING_FAIL_THRESHOLD", "").strip()
    if fail_threshold_raw:
        try:
            fail_threshold = int(fail_threshold_raw)
            if fail_threshold >= 0 and len(missing_posters) > fail_threshold:
                raise RuntimeError(
                    f"Poster quality gate failed: {len(missing_posters)} missing poster(s) exceeds threshold {fail_threshold}"
                )
        except ValueError:
            logger.warning("Invalid POSTER_MISSING_FAIL_THRESHOLD value: %s", fail_threshold_raw)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", DATA_FILE)

    html = build_html(data)
    Path(SITE_DIR).mkdir(parents=True, exist_ok=True)
    Path(SITE_DIR, "index.html").write_text(html, encoding="utf-8")
    logger.info("Wrote %s/index.html", SITE_DIR)
    Path(FINGERPRINT_FILE).write_text(fingerprint, encoding="utf-8")
    if fingerprint == prev_fingerprint:
        logger.info("Fingerprint unchanged; nothing new to commit.")
    else:
        logger.info("Fingerprint updated; commit and push to publish.")


if __name__ == "__main__":
    main()
