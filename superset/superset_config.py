"""
Mounted read-only into the container at /app/pythonpath/superset_config.py
(the image's PYTHONPATH), which is where Superset looks for config on boot -
confirmed empty/absent in the base image, so this is required, not optional.

SECRET_KEY must come from the environment, not be hardcoded here: Superset
uses it to sign session cookies, so a value baked into a file that ends up
in version control defeats the point of having one.
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Superset's OWN metadata (dashboards, charts, users) - a separate Postgres
# instance from RisingWave, which holds the actual T24 data being charted.
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset"
)
