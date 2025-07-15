
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import List, Optional
import os
import shutil
from ... import db, wasm_module_crud
from ...db import get_db

router = APIRouter()

WASM_MODULES_DIR = "/app/wasm_modules"

@router.post("/wasm-modules", response_model=wasm_module_crud.WasmModule)
def create_wasm_module(
    module_id: str = Form(...),
    description: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db_session: Session = Depends(get_db)
):
    """
    Uploads a new WASM module, saves it to the filesystem, and registers its
    metadata in the database.
    """
    if wasm_module_crud.get_wasm_module(db_session, module_id):
        raise HTTPException(status_code=400, detail=f"WASM module with id '{module_id}' already exists.")

    # Ensure the storage directory exists
    os.makedirs(WASM_MODULES_DIR, exist_ok=True)
    
    filepath = os.path.join(WASM_MODULES_DIR, f"{module_id}.wasm")
    
    try:
        with open(filepath, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    finally:
        file.file.close()
        
    module_data = wasm_module_crud.WasmModuleCreate(id=module_id, description=description)
    return wasm_module_crud.create_wasm_module(db_session, module=module_data, filepath=filepath)

@router.get("/wasm-modules", response_model=List[wasm_module_crud.WasmModule])
def read_wasm_modules(
    skip: int = 0,
    limit: int = 100,
    db_session: Session = Depends(get_db)
):
    """
    Retrieves a list of all registered WASM modules.
    """
    return wasm_module_crud.get_wasm_modules(db_session, skip=skip, limit=limit)

@router.get("/wasm-modules/{module_id}", response_model=wasm_module_crud.WasmModule)
def read_wasm_module(
    module_id: str,
    db_session: Session = Depends(get_db)
):
    """
    Retrieves metadata for a single WASM module.
    """
    db_module = wasm_module_crud.get_wasm_module(db_session, module_id)
    if not db_module:
        raise HTTPException(status_code=404, detail="WASM module not found.")
    return db_module

@router.delete("/wasm-modules/{module_id}", response_model=wasm_module_crud.WasmModule)
def delete_wasm_module(
    module_id: str,
    db_session: Session = Depends(get_db)
):
    """
    Deletes a WASM module from the filesystem and the database.
    """
    db_module = wasm_module_crud.get_wasm_module(db_session, module_id)
    if not db_module:
        raise HTTPException(status_code=404, detail="WASM module not found.")
        
    # Delete the file
    if os.path.exists(db_module.filepath):
        os.remove(db_module.filepath)
        
    # Delete the database record
    return wasm_module_crud.delete_wasm_module(db_session, module_id) 