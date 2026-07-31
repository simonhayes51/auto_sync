"""
Config-driven tuning for the production-crawler redesign (rate limiter,
circuit breaker, backoff, adaptive batch sizing, failure-TTL caching).
Every knob is an env var with a documented default - no hardcoded
constants in http_client.py/scrape_queue.py/the *_worker.py files -
matching the existing HISTORY_CONCURRENCY-style env-var convention
already used in bin_sales_history_sync.py, no new config framework.
"""
import os


def _float_env(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _int_env(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


# --- Rate limiter (global token bucket, shared across all worker
# processes via crawler_rate_state - see backend/migrations/037_scrape_queue.sql) ---
RATE_LIMIT_REQUESTS_PER_SEC = _float_env("RATE_LIMIT_REQUESTS_PER_SEC", 3.0)
RATE_LIMIT_BURST_CAPACITY = _float_env("RATE_LIMIT_BURST_CAPACITY", 3.0)

# --- Circuit breaker (global, per explicit decision - trips stop every
# worker, not just the one that hit the threshold) ---
CIRCUIT_BREAKER_429_THRESHOLD = _int_env("CIRCUIT_BREAKER_429_THRESHOLD", 20)
CIRCUIT_BREAKER_403_THRESHOLD = _int_env("CIRCUIT_BREAKER_403_THRESHOLD", 5)
CIRCUIT_BREAKER_COOLDOWN_MINUTES = _int_env("CIRCUIT_BREAKER_COOLDOWN_MINUTES", 45)

# --- Backoff with jitter ---
BACKOFF_BASE_SECONDS = _float_env("BACKOFF_BASE_SECONDS", 1.0)
BACKOFF_MAX_SECONDS = _float_env("BACKOFF_MAX_SECONDS", 60.0)
BACKOFF_JITTER_FRACTION = _float_env("BACKOFF_JITTER_FRACTION", 0.25)
MAX_RETRIES = _int_env("MAX_RETRIES", 3)

# --- Adaptive batch sizing ---
BATCH_SIZE_MIN = _int_env("BATCH_SIZE_MIN", 100)
BATCH_SIZE_MAX = _int_env("BATCH_SIZE_MAX", 2000)
BATCH_SIZE_DEFAULT = _int_env("BATCH_SIZE_DEFAULT", 500)
BATCH_SIZE_SHRINK_FACTOR = _float_env("BATCH_SIZE_SHRINK_FACTOR", 0.5)
BATCH_SIZE_GROW_FACTOR = _float_env("BATCH_SIZE_GROW_FACTOR", 1.25)
BATCH_SIZE_ERROR_RATE_THRESHOLD = _float_env("BATCH_SIZE_ERROR_RATE_THRESHOLD", 0.05)
BATCH_SIZE_HEALTHY_RUNS_TO_GROW = _int_env("BATCH_SIZE_HEALTHY_RUNS_TO_GROW", 3)

# --- Failure-TTL cache (404 / no-market-page / missing-sales-link) ---
FAILURE_CACHE_TTL_MINUTES = _int_env("FAILURE_CACHE_TTL_MINUTES", 180)

# --- HTTP ---
HTTP_TIMEOUT_SECONDS = _float_env("HTTP_TIMEOUT_SECONDS", 15.0)
HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "Mozilla/5.0 (compatible; SBCSolver/1.5)")
