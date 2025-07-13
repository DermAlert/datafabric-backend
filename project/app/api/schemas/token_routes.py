from pydantic import BaseModel, EmailStr
from fastapi import Form
from typing import List, Optional
from datetime import date

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class TokenRefresh(BaseModel):
    refresh_token: str

class RoleOut(BaseModel):
    id: int
    name: str
    nivel_acesso: int

    model_config = {
        "from_attributes": True
    }

class UserOut(BaseModel):
    id: int
    nome_usuario: Optional[str] = None
    email: EmailStr
    cpf: str
    fl_ativo: bool
    roles: List[RoleOut] = []

    model_config = {
        "from_attributes": True
    }