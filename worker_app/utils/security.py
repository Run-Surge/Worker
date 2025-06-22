import uuid
import socket
import hashlib
import platform
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict
from jose import JWTError, jwt

#TODO: get it from /auth/public-key
secret_key = '27985f8dbbaca184422fbc28a3d6443505edb2ec575d589cd52036ad1f1510dc'

class SecurityManager:
    def __init__(self):
        self.algorithm = 'HS256'

    def verify_token(self, token: str):
        try:
            payload = jwt.decode(token, secret_key, algorithms=[self.algorithm])
            if payload.get("type") != "access":
                raise Exception("Invalid token")
            
            return payload  
        except jwt.ExpiredSignatureError:
            raise Exception("Token has expired")
        except jwt.JWTError as e:
            print(f"JWTError: {e}")
            raise Exception("Invalid token")
        
    @staticmethod
    def get_machine_fingerprint():
    # Gather various pieces of system information
        info_string = ""
        try:
            info_string += platform.platform() # OS, release, version, etc.
        except Exception: pass
        try:
            info_string += platform.node() # Network name of the system
        except Exception: pass
        try:
            info_string += platform.processor() # Processor name
        except Exception: pass
        try:
            # Using uuid.getnode() for MAC address (can be inconsistent)
            # You might replace this with get_system_uuid() for better uniqueness
            info_string += str(uuid.getnode())
        except Exception: pass
        try:
            info_string += str(socket.gethostname()) # Hostname
        except Exception: pass
        try:
            info_string += str(socket.getfqdn()) # Fully qualified domain name
        except Exception: pass

        # Hash the combined string
        return hashlib.sha256(info_string.encode()).hexdigest()


# Create a singleton instance
security_manager = SecurityManager()
