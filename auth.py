"""
This contains various api authentication functions meant to be used as dependencies on endpoints
"""
from typing import Annotated
from fastapi import Header, HTTPException
import os

admin_token_str = os.environ.get("ADMIN_TOKEN", None)


def admin_token(token: Annotated[str, Header()] = None):
    if token != admin_token_str:
        raise HTTPException(status_code=400, detail="Invalid token")
    return token
