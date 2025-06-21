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


# Create a singleton instance
security_manager = SecurityManager()
