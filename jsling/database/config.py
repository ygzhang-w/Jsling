"""Configuration constants for Jsling."""

import json
from pathlib import Path

# Jsling home directory
JSLING_HOME: Path = Path.home() / ".jsling"
DB_PATH: Path = JSLING_HOME / "jsling.db"

# Default configuration values for database initialization
SYNC_INTERVAL: int = 10
STREAM_SYNC_PATTERNS: str = json.dumps([
    {"type": "suffix", "pattern": ".log"},
])
RSYNC_SYNC_PATTERNS: str = json.dumps([
    {"type": "suffix", "pattern": ".npy"},
])
RSYNC_OPTIONS: str = "-avz --progress"
STATUS_POLL_INITIAL_DELAY: int = 5
CLEANUP_COMPLETED_DAYS: int = 1
CLEANUP_KEEP_LOCAL: bool = False
JOB_SYNC_TIMEOUT: int = 30
SSH_CONNECT_TIMEOUT: int = 10
DEFAULT_RSYNC_MODE: str = "periodic"
RSYNC_INTERVAL: int = 60
STREAM_RETRY_COUNT: int = 3
STREAM_RETRY_DELAY: int = 5
WORKER_STATUS_POLL_INTERVAL: int = 30

# Submission worker configuration
SUBMISSION_POLL_INTERVAL: int = 2  # Interval to check for queued jobs (seconds)
SUBMISSION_MAX_CONCURRENT: int = 5  # Max concurrent job submissions
SUBMISSION_RETRY_COUNT: int = 3  # Number of retries for failed submissions
SUBMISSION_RETRY_DELAY: int = 10  # Delay between retries (seconds)
