"""Database schema initialization and default configurations."""

from datetime import datetime

from sqlalchemy.orm import Session

from jsling.database.models import Config
from jsling.database import config

# Mapping of config keys to their default values and types
_CONFIG_DEFAULTS = {
    "sync_interval": (lambda: str(config.SYNC_INTERVAL), type(config.SYNC_INTERVAL).__name__, "Job status polling interval in seconds (-1 to disable)"),
    "stream_sync_patterns": (lambda: config.STREAM_SYNC_PATTERNS, type(config.STREAM_SYNC_PATTERNS).__name__, "File patterns for streaming synchronization"),
    "rsync_sync_patterns": (lambda: config.RSYNC_SYNC_PATTERNS, type(config.RSYNC_SYNC_PATTERNS).__name__, "File patterns for rsync batch synchronization"),
    "rsync_options": (lambda: config.RSYNC_OPTIONS, type(config.RSYNC_OPTIONS).__name__, "Additional rsync command options"),
    "status_poll_initial_delay": (lambda: str(config.STATUS_POLL_INITIAL_DELAY), type(config.STATUS_POLL_INITIAL_DELAY).__name__, "Initial delay before first status poll in seconds"),
    "cleanup_completed_days": (lambda: str(config.CLEANUP_COMPLETED_DAYS), type(config.CLEANUP_COMPLETED_DAYS).__name__, "Days to keep completed jobs before cleanup"),
    "cleanup_keep_local": (lambda: str(config.CLEANUP_KEEP_LOCAL).lower(), type(config.CLEANUP_KEEP_LOCAL).__name__, "Whether to keep local workdir when cleaning up jobs"),
    "job_sync_timeout": (lambda: str(config.JOB_SYNC_TIMEOUT), type(config.JOB_SYNC_TIMEOUT).__name__, "Job status sync timeout in seconds"),
    "ssh_connect_timeout": (lambda: str(config.SSH_CONNECT_TIMEOUT), type(config.SSH_CONNECT_TIMEOUT).__name__, "SSH connection timeout in seconds"),
    "default_rsync_mode": (lambda: config.DEFAULT_RSYNC_MODE, type(config.DEFAULT_RSYNC_MODE).__name__, "Default rsync mode (periodic or final_only)"),
    "rsync_interval": (lambda: str(config.RSYNC_INTERVAL), type(config.RSYNC_INTERVAL).__name__, "Batch sync interval in seconds (only for periodic mode)"),
    "stream_retry_count": (lambda: str(config.STREAM_RETRY_COUNT), type(config.STREAM_RETRY_COUNT).__name__, "Number of retries for streaming sync"),
    "stream_retry_delay": (lambda: str(config.STREAM_RETRY_DELAY), type(config.STREAM_RETRY_DELAY).__name__, "Retry delay for streaming sync in seconds"),
    "worker_status_poll_interval": (lambda: str(config.WORKER_STATUS_POLL_INTERVAL), type(config.WORKER_STATUS_POLL_INTERVAL).__name__, "Worker status polling interval in seconds"),
}

def init_default_configs(session: Session) -> None:
    """Initialize default configurations
    
    Args:
        session: Database session
    """

    # cleanup all the existing configs
    session.query(Config).delete()

    # Add default configs
    for key, (value_fn, config_type, description) in _CONFIG_DEFAULTS.items():
        session.add(Config(
            config_key=key,
            config_value=value_fn(),
            config_type=config_type,
            description=description,
            updated_at=datetime.now()
        ))
    
    session.commit()


def reset_config_to_default(session: Session, key: str) -> bool:
    """Reset a configuration to its default value.
    
    Args:
        session: Database session
        key: Configuration key
        
    Returns:
        True if reset successful, False if key not found in defaults
    """
    if key not in _CONFIG_DEFAULTS:
        return False
    
    value_fn, config_type, description = _CONFIG_DEFAULTS[key]
    
    config_item = session.query(Config).filter(Config.config_key == key).first()
    
    if config_item:
        config_item.config_value = value_fn()
        config_item.config_type = config_type
        config_item.description = description
        config_item.updated_at = datetime.now()
    else:
        config_item = Config(
            config_key=key,
            config_value=value_fn(),
            config_type=config_type,
            description=description,
            updated_at=datetime.now()
        )
        session.add(config_item)
    
    session.commit()
    return True
