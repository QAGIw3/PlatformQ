from ..postgres_db import get_db_session

# This file now acts as a pass-through for dependencies.
# This is a good practice as it allows you to easily add
# more complex dependencies later (e.g., security, rate limiting)
# without changing the endpoint signatures. 