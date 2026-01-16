"""Worker manager for cluster registration and management."""

import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from sqlalchemy.orm import Session

from jsling.database.models import Worker as WorkerModel
from jsling.connections.worker import Worker
from jsling.connections.ssh_client import SSHClient
from jsling.utils.yaml_parser import parse_worker_config, WorkerConfig
from jsling.utils.encryption import encrypt_credential
from jsling.utils.validators import validate_worker_id


class WorkerManager:
    """Manager for Worker registration and operations."""
    
    def __init__(self, session: Session):
        """Initialize worker manager.
        
        Args:
            session: Database session
        """
        self.session = session
    
    def _generate_worker_id(self, worker_name: str) -> str:
        """Generate unique worker ID.
        
        Args:
            worker_name: Worker name
            
        Returns:
            Unique worker ID
        """
        # Use sanitized name + short UUID
        base = worker_name.lower().replace(" ", "_")[:30]
        suffix = str(uuid.uuid4())[:8]
        return f"{base}_{suffix}"
    
    def add_from_yaml(self, yaml_path: str | Path) -> WorkerModel:
        """Register worker from YAML configuration file.
        
        Args:
            yaml_path: Path to YAML configuration file
            
        Returns:
            Created Worker model
            
        Raises:
            ValueError: If validation fails
            FileNotFoundError: If YAML file not found
        """
        # Parse and validate YAML
        config = parse_worker_config(yaml_path)
        
        # Check if worker name already exists
        existing = self.session.query(WorkerModel).filter(
            WorkerModel.worker_name == config.worker_name
        ).first()
        
        if existing:
            raise ValueError(f"Worker with name '{config.worker_name}' already exists")
        
        # Generate worker ID
        worker_id = self._generate_worker_id(config.worker_name)
        validate_worker_id(worker_id)
        
        # Encrypt credential
        encrypted_cred = encrypt_credential(config.auth_credential)
        
        # Create Worker model
        worker_model = WorkerModel(
            worker_id=worker_id,
            worker_name=config.worker_name,
            host=config.host,
            port=config.port,
            username=config.username,
            auth_method=config.auth_method,
            auth_credential=encrypted_cred,
            remote_workdir=config.remote_workdir,
            queue_name=config.queue_name,
            worker_type=config.worker_type,
            ntasks_per_node=config.ntasks_per_node,
            gres=config.gres,
            gpus_per_task=config.gpus_per_task,
            is_active=True,
            created_at=datetime.now()
        )
        
        # Validate worker (SSH connection + queue check)
        worker = Worker(worker_model)
        success, message = worker.validate()
        
        if not success:
            worker.close()
            raise ValueError(f"Worker validation failed: {message}")
        
        worker.close()
        
        # Save to database
        self.session.add(worker_model)
        self.session.commit()
        
        return worker_model
    
    def get(self, worker_id: str) -> Optional[WorkerModel]:
        """Get worker by ID.
        
        Args:
            worker_id: Worker ID
            
        Returns:
            Worker model or None
        """
        return self.session.query(WorkerModel).filter(
            WorkerModel.worker_id == worker_id
        ).first()
    
    def get_by_name(self, worker_name: str) -> Optional[WorkerModel]:
        """Get worker by name.
        
        Args:
            worker_name: Worker name
            
        Returns:
            Worker model or None
        """
        return self.session.query(WorkerModel).filter(
            WorkerModel.worker_name == worker_name
        ).first()
    
    def list_all(self, active_only: bool = False) -> List[WorkerModel]:
        """List all workers.
        
        Args:
            active_only: Only return active workers
            
        Returns:
            List of Worker models
        """
        query = self.session.query(WorkerModel)
        
        if active_only:
            query = query.filter(WorkerModel.is_active == True)
        
        return query.all()
    
    def list_by_type(self, worker_type: str, active_only: bool = True) -> List[WorkerModel]:
        """List workers by type.
        
        Args:
            worker_type: Worker type (cpu/gpu)
            active_only: Only return active workers
            
        Returns:
            List of Worker models
        """
        query = self.session.query(WorkerModel).filter(
            WorkerModel.worker_type == worker_type
        )
        
        if active_only:
            query = query.filter(WorkerModel.is_active == True)
        
        return query.all()
    
    def list_workers(
        self, 
        worker_type: Optional[str] = None, 
        include_inactive: bool = False
    ) -> List[WorkerModel]:
        """List workers with optional filtering.
        
        Args:
            worker_type: Optional worker type filter (cpu/gpu)
            include_inactive: Include inactive workers
            
        Returns:
            List of Worker models
        """
        query = self.session.query(WorkerModel)
        
        if worker_type:
            query = query.filter(WorkerModel.worker_type == worker_type)
        
        if not include_inactive:
            query = query.filter(WorkerModel.is_active == True)
        
        return query.all()
    
    def update(self, worker_id: str, yaml_path: str | Path) -> WorkerModel:
        """Update worker from YAML configuration.
        
        Args:
            worker_id: Worker ID
            yaml_path: Path to YAML configuration file
            
        Returns:
            Updated Worker model
            
        Raises:
            ValueError: If worker not found or validation fails
        """
        worker_model = self.get(worker_id)
        
        if not worker_model:
            raise ValueError(f"Worker not found: {worker_id}")
        
        # Parse new configuration
        config = parse_worker_config(yaml_path)
        
        # Update fields
        worker_model.worker_name = config.worker_name
        worker_model.host = config.host
        worker_model.port = config.port
        worker_model.username = config.username
        worker_model.auth_method = config.auth_method
        worker_model.auth_credential = encrypt_credential(config.auth_credential)
        worker_model.remote_workdir = config.remote_workdir
        worker_model.queue_name = config.queue_name
        worker_model.worker_type = config.worker_type
        worker_model.ntasks_per_node = config.ntasks_per_node
        worker_model.gres = config.gres
        worker_model.gpus_per_task = config.gpus_per_task
        
        # Validate updated worker
        worker = Worker(worker_model)
        success, message = worker.validate()
        worker.close()
        
        if not success:
            self.session.rollback()
            raise ValueError(f"Worker validation failed: {message}")
        
        self.session.commit()
        return worker_model
    
    def remove(self, worker_id: str) -> bool:
        """Remove worker.
        
        Args:
            worker_id: Worker ID
            
        Returns:
            True if removed, False if not found
        """
        worker_model = self.get(worker_id)
        
        if not worker_model:
            return False
        
        self.session.delete(worker_model)
        self.session.commit()
        return True
    
    def enable(self, worker_id: str) -> bool:
        """Enable worker.
        
        Args:
            worker_id: Worker ID
            
        Returns:
            True if enabled, False if not found
        """
        worker_model = self.get(worker_id)
        
        if not worker_model:
            return False
        
        worker_model.is_active = True
        self.session.commit()
        return True
    
    def disable(self, worker_id: str) -> bool:
        """Disable worker.
        
        Args:
            worker_id: Worker ID
            
        Returns:
            True if disabled, False if not found
        """
        worker_model = self.get(worker_id)
        
        if not worker_model:
            return False
        
        worker_model.is_active = False
        self.session.commit()
        return True
    
    def test_worker(self, worker_id: str) -> tuple[bool, str]:
        """Test worker connection and configuration.
        
        Args:
            worker_id: Worker ID
            
        Returns:
            Tuple of (success, message)
        """
        worker_model = self.get(worker_id)
        
        if not worker_model:
            return False, f"Worker not found: {worker_id}"
        
        worker = Worker(worker_model)
        try:
            success, message = worker.validate()
            return success, message
        finally:
            worker.close()
