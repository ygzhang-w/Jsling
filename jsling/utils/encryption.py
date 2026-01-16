"""Password encryption and decryption utilities using Fernet symmetric encryption."""

import base64
import hashlib
import os
from pathlib import Path

from cryptography.fernet import Fernet

from jsling.database.config import JSLING_HOME


# Encryption key file location
KEY_FILE = JSLING_HOME / ".key"


def _generate_key() -> bytes:
    """Generate a new encryption key."""
    return Fernet.generate_key()


def _get_or_create_key() -> bytes:
    """Get existing encryption key or create a new one.
    
    Returns:
        Encryption key as bytes
    """
    if KEY_FILE.exists():
        with open(KEY_FILE, 'rb') as f:
            return f.read()
    else:
        # Ensure ~/.jsling directory exists
        KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Generate new key
        key = _generate_key()
        
        # Save key with restricted permissions
        with open(KEY_FILE, 'wb') as f:
            f.write(key)
        
        # Set file permissions to 600 (owner read/write only)
        os.chmod(KEY_FILE, 0o600)
        
        return key


def encrypt_credential(credential: str) -> str:
    """Encrypt a credential (password or key path).
    
    Args:
        credential: Plain text credential
        
    Returns:
        Base64-encoded encrypted credential
    """
    key = _get_or_create_key()
    f = Fernet(key)
    
    # Encrypt
    encrypted = f.encrypt(credential.encode('utf-8'))
    
    # Return as base64 string for storage
    return base64.b64encode(encrypted).decode('utf-8')


def decrypt_credential(encrypted_credential: str) -> str:
    """Decrypt a credential.
    
    Args:
        encrypted_credential: Base64-encoded encrypted credential
        
    Returns:
        Plain text credential
        
    Raises:
        cryptography.fernet.InvalidToken: If decryption fails
    """
    key = _get_or_create_key()
    f = Fernet(key)
    
    # Decode from base64
    encrypted = base64.b64decode(encrypted_credential.encode('utf-8'))
    
    # Decrypt
    decrypted = f.decrypt(encrypted)
    
    return decrypted.decode('utf-8')


def hash_credential(credential: str) -> str:
    """Create a hash of credential for validation (not reversible).
    
    Args:
        credential: Plain text credential
        
    Returns:
        SHA256 hash as hex string
    """
    return hashlib.sha256(credential.encode('utf-8')).hexdigest()
