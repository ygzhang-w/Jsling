"""SQLAlchemy database models for Jsling."""

from datetime import datetime
from typing import Optional, TYPE_CHECKING

from sqlalchemy import Boolean, Integer, String, Text, DateTime, Index, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

if TYPE_CHECKING:
    from jsling.database.models import Worker


class Base(DeclarativeBase):
    """Base class for all database models."""
    pass


class Worker(Base):
    """Worker configuration table - manages remote cluster connections and resources."""
    
    __tablename__ = "workers"
    
    # Primary key
    worker_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    
    # Worker identification
    worker_name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    
    # SSH connection info
    host: Mapped[str] = mapped_column(String(255), nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=22, nullable=False)
    username: Mapped[str] = mapped_column(String(100), nullable=False)
    auth_method: Mapped[str] = mapped_column(String(20), nullable=False)  # 'key' or 'password'
    auth_credential: Mapped[str] = mapped_column(Text, nullable=False)  # encrypted
    
    # Remote configuration
    remote_workdir: Mapped[str] = mapped_column(String(500), nullable=False)
    queue_name: Mapped[str] = mapped_column(String(100), nullable=False)
    
    # Resource configuration
    worker_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 'cpu' or 'gpu'
    ntasks_per_node: Mapped[int] = mapped_column(Integer, nullable=False)
    gres: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    gpus_per_task: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    
    # Indexes
    __table_args__ = (
        Index('idx_worker_type', 'worker_type'),
        Index('idx_worker_active', 'is_active'),
    )
    
    def __repr__(self) -> str:
        return f"<Worker(id={self.worker_id}, name={self.worker_name}, type={self.worker_type})>"


class Job(Base):
    """Job record table - tracks all submitted jobs and their status."""
    
    __tablename__ = "jobs"
    
    # Primary key
    job_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    
    # Job identification
    slurm_job_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    worker_id: Mapped[str] = mapped_column(String(100), nullable=False)
    
    # Work directories
    local_workdir: Mapped[str] = mapped_column(String(500), nullable=False)
    remote_workdir: Mapped[str] = mapped_column(String(500), nullable=False)
    script_path: Mapped[str] = mapped_column(String(500), nullable=False)  # DEPRECATED: kept for DB compatibility, use command field instead
    command: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationship to Worker
    worker: Mapped["Worker"] = relationship("Worker", foreign_keys=[worker_id], primaryjoin="Job.worker_id == Worker.worker_id", lazy="joined")
    
    # Job status
    job_status: Mapped[str] = mapped_column(String(20), nullable=False)  # pending/running/completed/failed/cancelled
    sync_mode: Mapped[str] = mapped_column(String(20), nullable=False)  # DEPRECATED: kept for DB compatibility
    
    # Timestamps
    submit_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    start_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_sync_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Synchronization
    sentinel_file: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    
    # Extended fields for job execution tracking
    exit_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    uploaded_files: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    
    # Cleanup management
    marked_for_deletion: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    
    # File synchronization tracking
    sync_pid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sync_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # running/stopped/error
    rsync_mode: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # periodic/final_only
    synced_files: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    last_rsync_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # File sync configuration
    sync_rules: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON object for sync rules
    rsync_interval: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # rsync interval in seconds
    
    # Indexes
    __table_args__ = (
        Index('idx_job_status', 'job_status'),
        Index('idx_job_worker', 'worker_id'),
        Index('idx_job_submit_time', 'submit_time'),
    )
    
    def __repr__(self) -> str:
        return f"<Job(id={self.job_id}, slurm_id={self.slurm_job_id}, status={self.job_status})>"


class Config(Base):
    """System configuration table - stores global configuration parameters."""
    
    __tablename__ = "configs"
    
    # Primary key
    config_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    
    # Configuration value
    config_value: Mapped[str] = mapped_column(Text, nullable=False)
    config_type: Mapped[str] = mapped_column(String(20), nullable=False)  # string/int/bool/path/json
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Timestamp
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    def __repr__(self) -> str:
        return f"<Config(key={self.config_key}, type={self.config_type})>"


class WorkerLiveStatus(Base):
    """Worker live status table - caches real-time Slurm queue status from daemon polling."""
    
    __tablename__ = "worker_live_status"
    
    # Primary key (same as worker_id for 1:1 relationship)
    worker_id: Mapped[str] = mapped_column(String(100), ForeignKey("workers.worker_id"), primary_key=True)
    
    # Queue availability state (from sinfo %a: up/down)
    queue_state: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    
    # CPU slot statistics (from sinfo %C: alloc/idle/other/total)
    cpu_alloc: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cpu_idle: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cpu_other: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cpu_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # GPU slot statistics (from sinfo %G: gres_type:alloc/total)
    gpu_alloc: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gpu_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Node statistics (from sinfo %D and node state breakdown)
    nodes_available: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    nodes_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Timestamps
    last_updated: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    
    # Relationship to Worker
    worker: Mapped["Worker"] = relationship("Worker", foreign_keys=[worker_id])
    
    def __repr__(self) -> str:
        return f"<WorkerLiveStatus(worker_id={self.worker_id}, nodes={self.nodes_available}/{self.nodes_total})>"
