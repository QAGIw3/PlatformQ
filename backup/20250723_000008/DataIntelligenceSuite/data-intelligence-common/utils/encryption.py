"""
Encryption utilities.

Provides encryption, hashing, and security utilities.
"""

import os
import base64
import hashlib
import hmac
import secrets
from typing import Optional, Union, Tuple, Dict, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import bcrypt
import jwt
from datetime import datetime, timedelta

from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class HashAlgorithm:
    """Hash algorithms"""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA384 = "sha384"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE2S = "blake2s"


class EncryptionError(Exception):
    """Encryption error"""
    pass


class SymmetricEncryption:
    """Symmetric encryption utilities using Fernet"""
    
    @staticmethod
    def generate_key() -> bytes:
        """Generate encryption key"""
        return Fernet.generate_key()
        
    @staticmethod
    def encrypt(data: Union[str, bytes], key: Union[str, bytes]) -> bytes:
        """Encrypt data"""
        if isinstance(data, str):
            data = data.encode('utf-8')
        if isinstance(key, str):
            key = key.encode('utf-8')
            
        try:
            f = Fernet(key)
            return f.encrypt(data)
        except Exception as e:
            raise EncryptionError(f"Encryption failed: {e}")
            
    @staticmethod
    def decrypt(encrypted_data: Union[str, bytes], key: Union[str, bytes]) -> bytes:
        """Decrypt data"""
        if isinstance(encrypted_data, str):
            encrypted_data = encrypted_data.encode('utf-8')
        if isinstance(key, str):
            key = key.encode('utf-8')
            
        try:
            f = Fernet(key)
            return f.decrypt(encrypted_data)
        except Exception as e:
            raise EncryptionError(f"Decryption failed: {e}")
            
    @staticmethod
    def encrypt_string(data: str, key: Union[str, bytes]) -> str:
        """Encrypt string and return base64 encoded result"""
        encrypted = SymmetricEncryption.encrypt(data, key)
        return base64.urlsafe_b64encode(encrypted).decode('utf-8')
        
    @staticmethod
    def decrypt_string(encrypted_data: str, key: Union[str, bytes]) -> str:
        """Decrypt base64 encoded string"""
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
        decrypted = SymmetricEncryption.decrypt(encrypted_bytes, key)
        return decrypted.decode('utf-8')


class AsymmetricEncryption:
    """Asymmetric encryption utilities using RSA"""
    
    @staticmethod
    def generate_key_pair(key_size: int = 2048) -> Tuple[bytes, bytes]:
        """Generate RSA key pair"""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
            backend=default_backend()
        )
        
        # Serialize private key
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Serialize public key
        public_key = private_key.public_key()
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        return private_pem, public_pem
        
    @staticmethod
    def encrypt(data: Union[str, bytes], public_key: bytes) -> bytes:
        """Encrypt data with public key"""
        if isinstance(data, str):
            data = data.encode('utf-8')
            
        try:
            public_key_obj = serialization.load_pem_public_key(
                public_key,
                backend=default_backend()
            )
            
            encrypted = public_key_obj.encrypt(
                data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            
            return encrypted
        except Exception as e:
            raise EncryptionError(f"RSA encryption failed: {e}")
            
    @staticmethod
    def decrypt(encrypted_data: bytes, private_key: bytes) -> bytes:
        """Decrypt data with private key"""
        try:
            private_key_obj = serialization.load_pem_private_key(
                private_key,
                password=None,
                backend=default_backend()
            )
            
            decrypted = private_key_obj.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            
            return decrypted
        except Exception as e:
            raise EncryptionError(f"RSA decryption failed: {e}")


class AESEncryption:
    """AES encryption utilities"""
    
    @staticmethod
    def generate_key(key_size: int = 256) -> bytes:
        """Generate AES key"""
        return os.urandom(key_size // 8)
        
    @staticmethod
    def encrypt(
        data: Union[str, bytes],
        key: bytes,
        mode: str = "GCM"
    ) -> Tuple[bytes, bytes, Optional[bytes]]:
        """
        Encrypt data using AES.
        
        Returns: (ciphertext, nonce/iv, tag)
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
            
        try:
            if mode == "GCM":
                # Generate nonce
                nonce = os.urandom(12)
                
                # Create cipher
                cipher = Cipher(
                    algorithms.AES(key),
                    modes.GCM(nonce),
                    backend=default_backend()
                )
                
                encryptor = cipher.encryptor()
                ciphertext = encryptor.update(data) + encryptor.finalize()
                
                return ciphertext, nonce, encryptor.tag
                
            elif mode == "CBC":
                # Generate IV
                iv = os.urandom(16)
                
                # Pad data to 16-byte boundary
                padded_data = AESEncryption._pad(data)
                
                # Create cipher
                cipher = Cipher(
                    algorithms.AES(key),
                    modes.CBC(iv),
                    backend=default_backend()
                )
                
                encryptor = cipher.encryptor()
                ciphertext = encryptor.update(padded_data) + encryptor.finalize()
                
                return ciphertext, iv, None
                
            else:
                raise ValueError(f"Unsupported mode: {mode}")
                
        except Exception as e:
            raise EncryptionError(f"AES encryption failed: {e}")
            
    @staticmethod
    def decrypt(
        ciphertext: bytes,
        key: bytes,
        nonce_or_iv: bytes,
        tag: Optional[bytes] = None,
        mode: str = "GCM"
    ) -> bytes:
        """Decrypt AES encrypted data"""
        try:
            if mode == "GCM":
                cipher = Cipher(
                    algorithms.AES(key),
                    modes.GCM(nonce_or_iv, tag),
                    backend=default_backend()
                )
                
                decryptor = cipher.decryptor()
                plaintext = decryptor.update(ciphertext) + decryptor.finalize()
                
                return plaintext
                
            elif mode == "CBC":
                cipher = Cipher(
                    algorithms.AES(key),
                    modes.CBC(nonce_or_iv),
                    backend=default_backend()
                )
                
                decryptor = cipher.decryptor()
                padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
                
                # Remove padding
                return AESEncryption._unpad(padded_plaintext)
                
            else:
                raise ValueError(f"Unsupported mode: {mode}")
                
        except Exception as e:
            raise EncryptionError(f"AES decryption failed: {e}")
            
    @staticmethod
    def _pad(data: bytes, block_size: int = 16) -> bytes:
        """PKCS7 padding"""
        padding_length = block_size - (len(data) % block_size)
        padding = bytes([padding_length] * padding_length)
        return data + padding
        
    @staticmethod
    def _unpad(data: bytes) -> bytes:
        """Remove PKCS7 padding"""
        padding_length = data[-1]
        return data[:-padding_length]


class HashUtils:
    """Hashing utilities"""
    
    @staticmethod
    def hash_data(
        data: Union[str, bytes],
        algorithm: str = HashAlgorithm.SHA256
    ) -> str:
        """Hash data using specified algorithm"""
        if isinstance(data, str):
            data = data.encode('utf-8')
            
        if algorithm == HashAlgorithm.MD5:
            return hashlib.md5(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA1:
            return hashlib.sha1(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA384:
            return hashlib.sha384(data).hexdigest()
        elif algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512(data).hexdigest()
        elif algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b(data).hexdigest()
        elif algorithm == HashAlgorithm.BLAKE2S:
            return hashlib.blake2s(data).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
            
    @staticmethod
    def hash_file(
        file_path: str,
        algorithm: str = HashAlgorithm.SHA256,
        chunk_size: int = 8192
    ) -> str:
        """Hash file contents"""
        hash_obj = hashlib.new(algorithm)
        
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hash_obj.update(chunk)
                
        return hash_obj.hexdigest()
        
    @staticmethod
    def hmac_hash(
        data: Union[str, bytes],
        key: Union[str, bytes],
        algorithm: str = HashAlgorithm.SHA256
    ) -> str:
        """Generate HMAC hash"""
        if isinstance(data, str):
            data = data.encode('utf-8')
        if isinstance(key, str):
            key = key.encode('utf-8')
            
        return hmac.new(key, data, getattr(hashlib, algorithm)).hexdigest()
        
    @staticmethod
    def verify_hmac(
        data: Union[str, bytes],
        key: Union[str, bytes],
        expected_hash: str,
        algorithm: str = HashAlgorithm.SHA256
    ) -> bool:
        """Verify HMAC hash"""
        actual_hash = HashUtils.hmac_hash(data, key, algorithm)
        return hmac.compare_digest(actual_hash, expected_hash)


class PasswordUtils:
    """Password hashing and validation"""
    
    @staticmethod
    def hash_password(password: str, rounds: int = 12) -> str:
        """Hash password using bcrypt"""
        salt = bcrypt.gensalt(rounds=rounds)
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
        
    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """Verify password against hash"""
        try:
            return bcrypt.checkpw(
                password.encode('utf-8'),
                hashed.encode('utf-8')
            )
        except Exception:
            return False
            
    @staticmethod
    def generate_password(
        length: int = 16,
        include_symbols: bool = True
    ) -> str:
        """Generate secure random password"""
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        if include_symbols:
            alphabet += "!@#$%^&*()_+-=[]{}|;:,.<>?"
            
        return ''.join(secrets.choice(alphabet) for _ in range(length))
        
    @staticmethod
    def check_password_strength(password: str) -> Dict[str, Any]:
        """Check password strength"""
        strength = {
            "length": len(password),
            "has_uppercase": any(c.isupper() for c in password),
            "has_lowercase": any(c.islower() for c in password),
            "has_digits": any(c.isdigit() for c in password),
            "has_symbols": any(not c.isalnum() for c in password),
            "score": 0
        }
        
        # Calculate score
        if strength["length"] >= 8:
            strength["score"] += 1
        if strength["length"] >= 12:
            strength["score"] += 1
        if strength["has_uppercase"]:
            strength["score"] += 1
        if strength["has_lowercase"]:
            strength["score"] += 1
        if strength["has_digits"]:
            strength["score"] += 1
        if strength["has_symbols"]:
            strength["score"] += 1
            
        # Determine strength level
        if strength["score"] >= 5:
            strength["level"] = "strong"
        elif strength["score"] >= 3:
            strength["level"] = "medium"
        else:
            strength["level"] = "weak"
            
        return strength


class TokenUtils:
    """JWT token utilities"""
    
    @staticmethod
    def generate_token(
        payload: Dict[str, Any],
        secret: str,
        algorithm: str = "HS256",
        expires_in: Optional[timedelta] = None
    ) -> str:
        """Generate JWT token"""
        if expires_in:
            payload["exp"] = datetime.utcnow() + expires_in
            
        if "iat" not in payload:
            payload["iat"] = datetime.utcnow()
            
        return jwt.encode(payload, secret, algorithm=algorithm)
        
    @staticmethod
    def decode_token(
        token: str,
        secret: str,
        algorithms: list = ["HS256"],
        verify: bool = True
    ) -> Dict[str, Any]:
        """Decode and verify JWT token"""
        try:
            return jwt.decode(
                token,
                secret,
                algorithms=algorithms,
                options={"verify_signature": verify}
            )
        except jwt.ExpiredSignatureError:
            raise EncryptionError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise EncryptionError(f"Invalid token: {e}")
            
    @staticmethod
    def generate_api_key(prefix: str = "pk", length: int = 32) -> str:
        """Generate API key"""
        key = secrets.token_urlsafe(length)
        return f"{prefix}_{key}"
        
    @staticmethod
    def generate_secret(length: int = 32) -> str:
        """Generate secret key"""
        return secrets.token_hex(length)


class KeyDerivation:
    """Key derivation utilities"""
    
    @staticmethod
    def derive_key(
        password: Union[str, bytes],
        salt: Optional[bytes] = None,
        iterations: int = 100000,
        key_length: int = 32
    ) -> Tuple[bytes, bytes]:
        """Derive encryption key from password"""
        if isinstance(password, str):
            password = password.encode('utf-8')
            
        if salt is None:
            salt = os.urandom(16)
            
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=key_length,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        
        key = kdf.derive(password)
        return key, salt
        
    @staticmethod
    def generate_fernet_key_from_password(
        password: str,
        salt: Optional[bytes] = None
    ) -> Tuple[bytes, bytes]:
        """Generate Fernet-compatible key from password"""
        key, salt = KeyDerivation.derive_key(password, salt, key_length=32)
        
        # Fernet requires base64-encoded 32-byte key
        fernet_key = base64.urlsafe_b64encode(key)
        
        return fernet_key, salt


# Convenience functions

def encrypt(data: Union[str, bytes], key: Union[str, bytes]) -> bytes:
    """Encrypt data using symmetric encryption"""
    return SymmetricEncryption.encrypt(data, key)


def decrypt(encrypted_data: Union[str, bytes], key: Union[str, bytes]) -> bytes:
    """Decrypt data using symmetric encryption"""
    return SymmetricEncryption.decrypt(encrypted_data, key)


def hash_password(password: str) -> str:
    """Hash password securely"""
    return PasswordUtils.hash_password(password)


def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash"""
    return PasswordUtils.verify_password(password, hashed)


def generate_token(payload: Dict[str, Any], secret: str, **kwargs) -> str:
    """Generate JWT token"""
    return TokenUtils.generate_token(payload, secret, **kwargs)


def hash_data(data: Union[str, bytes], algorithm: str = HashAlgorithm.SHA256) -> str:
    """Hash data"""
    return HashUtils.hash_data(data, algorithm) 