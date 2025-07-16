CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  username TEXT,
  email TEXT,
  hashed_password TEXT,
  full_name TEXT,
  disabled BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  role TEXT,
  bio TEXT,
  profile_picture_url TEXT,
  did TEXT,
  wallet_address TEXT,
  storage_backend TEXT,
  storage_config TEXT
);

CREATE INDEX IF NOT EXISTS ON users(username);
CREATE INDEX IF NOT EXISTS ON users(email);

CREATE TABLE IF NOT EXISTS tokens (
    token TEXT PRIMARY KEY,
    user_id UUID,
    expires_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ON tokens(user_id);

CREATE TABLE IF NOT EXISTS siwe_nonces (
    nonce TEXT PRIMARY KEY,
    created_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ON users(wallet_address); 