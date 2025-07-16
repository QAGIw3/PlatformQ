from cassandra.cluster import Session
from datetime import datetime

def store_nonce(db: Session, nonce: str):
    query = "INSERT INTO siwe_nonces (nonce, created_at) VALUES (%s, %s)"
    prepared_query = db.prepare(query)
    db.execute(prepared_query, [nonce, datetime.now()])

def use_nonce(db: Session, nonce: str) -> bool:
    query = "SELECT nonce FROM siwe_nonces WHERE nonce = %s"
    prepared_query = db.prepare(query)
    row = db.execute(prepared_query, [nonce]).one()
    if not row: return False
    delete_query = "DELETE FROM siwe_nonces WHERE nonce = %s"
    prepared_delete = db.prepare(delete_query)
    db.execute(prepared_delete, [nonce])
    return True 