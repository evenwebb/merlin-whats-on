# Merlin Cinemas Cornwall What's On

Scrapes Merlin Cinemas Cornwall listings, enriches films with TMDb metadata, and publishes a static HTML site in `docs/` for GitHub Pages.

## Production Status

Current setup is production-ready with:

- Multi-cinema scrape (Bodmin, Helston, Falmouth, Redruth, St Ives, Penzance, Ritz)
- TMDb enrichment via environment secret
- Change detection and fast no-change runs
- Health checks with configurable thresholds
- Per-cinema failure tolerance with consecutive-failure escalation
- Detailed GitHub issue creation on workflow failures (with dedupe by signature)

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 whats_on_scraper.py
```

Outputs:

- `whats_on_data.json`
- `docs/index.html`
- `docs/posters/*`
- `.tmdb_cache.json`
- `.merlin_detail_cache.json`
- `.whats_on_fingerprint`
- `.cinema_failure_state.json`

## Runtime Behavior

- Scrapes all configured cinemas in parallel.
- Uses `.whats_on_fingerprint` to detect meaningful listing changes.
- If unchanged (and `FORCE_REBUILD` not set), skips enrichment/render for fast runs.
- If changed, updates JSON/HTML/assets and caches.

## Configuration

### Core environment variables

| Variable | Default | Purpose |
|---|---:|---|
| `TMDB_API_KEY` | unset | Enables TMDb enrichment (required in GitHub Actions workflow). |
| `FORCE_REBUILD` | `false` | Force full rebuild even when fingerprint unchanged. |
| `POSTER_MISSING_FAIL_THRESHOLD` | unset locally | Optional quality gate for missing posters. |
| `TMDB_DELAY_SEC` | `0` | Optional pacing between TMDb calls. |
| `TMDB_EMPTY_POSTER_REFETCH_DAYS` | `1` | Retry cached TMDb poster misses after this many days (`0` = every run). |
| `WTW_ENABLED_CINEMAS` | `all` | Scrape subset (`bodmin,helston,...`) or `all`. |
| `WTW_INITIAL_SHOWINGS_VISIBLE` | `40` | Initial per-film showings shown before “Show more”. |

### Health checks

| Variable | Default | Purpose |
|---|---:|---|
| `HEALTHCHECK_ENFORCE` | `true` in Actions, `false` local | Fail run when health checks fail. |
| `WTW_FAIL_ON_MARKUP_DRIFT` | `false` | Hard-fail when selector returns zero film nodes. |
| `WTW_MIN_TOTAL_FILMS` | `10` | Minimum total films (alias supported for health gate). |
| `WTW_MIN_TOTAL_SHOWTIMES` | `30` | Minimum total showtimes (alias supported for health gate). |
| `WTW_MIN_FILMS_PER_CINEMA` | `0` | Minimum films required per non-excluded cinema. |
| `HEALTH_MIN_TOTAL_FILMS` | `10` | Minimum total films across non-excluded cinemas. |
| `HEALTH_MIN_TOTAL_SHOWTIMES` | `30` | Minimum total showtimes across non-excluded cinemas. |
| `HEALTH_MIN_CINEMAS_WITH_FILMS` | `4` | Minimum non-excluded cinemas with at least 1 film. |
| `HEALTH_MIN_NOW_SHOWING_FILMS` | `1` | Minimum films qualifying as “Now Showing”. |
| `HEALTH_MAX_MARKUP_SUSPECT_CINEMAS` | `1` | Max cinemas allowed with parser/markup warnings. |
| `HEALTH_EXCLUDED_CINEMAS` | `st-ives` (workflow default) | Comma-separated cinema slugs excluded from health gating. |

### Per-cinema failure handling

| Variable | Default | Purpose |
|---|---:|---|
| `MAX_CONSECUTIVE_CINEMA_FAILURES` | `2` | Escalation threshold before failing run for repeated single-cinema outages. |

Behavior:

- One-off single cinema failures are tolerated.
- Last good data is reused for failed cinema (if available).
- Consecutive failures tracked in `.cinema_failure_state.json`.
- Run fails only when a cinema reaches threshold (excluding health-excluded cinemas).

## GitHub Actions

Workflow: `.github/workflows/whats_on_html.yml`

Features:

- Scheduled daily run + manual trigger
- Optional whole-job retries (`SCRAPER_RUN_ATTEMPTS`; workflow defaults to **1** attempt to save CI minutes)
- Secrets-based TMDb key only (`secrets.TMDB_API_KEY`)
- Health/env context capture and scraper logs on failure
- Failure artifact upload (`scraper-failure-logs`)
- Auto issue create/update on failure (optional via `CREATE_FAILURE_ISSUE`)
- Failure signature dedupe (prevents repeated issue noise for same error)
- Reopen matching closed issue on recurrence
- Success streak tracking and auto-close after 2 successful runs

### Required repository secret

- `TMDB_API_KEY`

### Optional repository secret

- `CREATE_FAILURE_ISSUE` (`true` to enable issue creation)
- `SCRAPER_RUN_ATTEMPTS` (workflow defaults to **1**; set to `2` if you want one retry after failure)

### Recommended repository variables

- `TMDB_EMPTY_POSTER_REFETCH_DAYS` (e.g. `1` daily, `0` aggressive)
- `POSTER_MISSING_FAIL_THRESHOLD`
- `WTW_FAIL_ON_MARKUP_DRIFT`
- `WTW_MIN_TOTAL_FILMS`
- `WTW_MIN_TOTAL_SHOWTIMES`
- `WTW_MIN_FILMS_PER_CINEMA`
- `WTW_INITIAL_SHOWINGS_VISIBLE`
- `WTW_ENABLED_CINEMAS`
- `HEALTH_MIN_TOTAL_FILMS`
- `HEALTH_MIN_TOTAL_SHOWTIMES`
- `HEALTH_MIN_CINEMAS_WITH_FILMS`
- `HEALTH_MIN_NOW_SHOWING_FILMS`
- `HEALTH_MAX_MARKUP_SUSPECT_CINEMAS`
- `HEALTH_EXCLUDED_CINEMAS`
- `MAX_CONSECUTIVE_CINEMA_FAILURES`

## GitHub Pages

Set in GitHub:

1. **Settings -> Pages**
2. Source: **Deploy from a branch**
3. Branch: `main`, folder: `/docs`

## Troubleshooting

- If one cinema fails temporarily, check `.cinema_failure_state.json` and workflow logs.
- If parser warnings increase, review health summary and likely markup changes on Merlin pages.
- If TMDb enrichment is missing in CI, verify `TMDB_API_KEY` secret exists.
- If action fails, inspect generated issue and attached `scraper-failure-logs` artifact.

## License

[GPL-3.0](LICENSE)
