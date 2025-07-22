from sqlalchemy.orm import Session
from . import db
from pydantic import BaseModel
from typing import Optional, List
import datetime

# --- Pydantic Schemas ---

class WasmModuleBase(BaseModel):
    id: str
    description: Optional[str] = None

class WasmModuleCreate(WasmModuleBase):
    pass

class WasmModule(WasmModuleBase):
    filepath: str
    created_at: datetime

    class Config:
        orm_mode = True

# --- CRUD Functions ---

def create_wasm_module(db_session: Session, module: WasmModuleCreate, filepath: str) -> WasmModule:
    db_module = db.WasmModule(
        id=module.id,
        description=module.description,
        filepath=filepath
    )
    db_session.add(db_module)
    db_session.commit()
    db_session.refresh(db_module)
    return db_module

def get_wasm_module(db_session: Session, module_id: str) -> Optional[db.WasmModule]:
    return db_session.query(db.WasmModule).filter(db.WasmModule.id == module_id).first()

def get_wasm_modules(db_session: Session, skip: int = 0, limit: int = 100) -> List[db.WasmModule]:
    return db_session.query(db.WasmModule).offset(skip).limit(limit).all()

def delete_wasm_module(db_session: Session, module_id: str) -> Optional[db.WasmModule]:
    db_module = get_wasm_module(db_session, module_id)
    if db_module:
        db_session.delete(db_module)
        db_session.commit()
    return db_module 