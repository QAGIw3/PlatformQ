from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
import os
from dotenv import load_dotenv
import sys

# --- CUSTOM PATH SETUP ---
# Add the service's root directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# ---

# This is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line needs to be placed before any calls to logger().
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# --- CUSTOM SETUP FOR PLATFORMQ ---

# Load .env file from the project root for local development
# In production, these vars would be set by the container environment.
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)

# Provide a default value to ensure the env var is always a string
db_url = os.getenv('POSTGRES_DATABASE_URL', 'postgresql://user:password@localhost/platformq_assets')
config.set_main_option('POSTGRES_DATABASE_URL', db_url)

# Import the Base from our shared library and the models from our service
# This ensures that the 'target_metadata' below has our models registered
from app.postgres_db import Base
from app.db import models

# This is the target metadata for 'autogenerate' support
target_metadata = Base.metadata

# --- END CUSTOM SETUP ---


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
