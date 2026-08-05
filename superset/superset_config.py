"""
Mounted at /app/pythonpath/superset_config.py, the image's actual PYTHONPATH
(ships empty by default, so this file is required, not optional).
"""

import os

# From the environment, not hardcoded: signs session cookies, so a value
# checked into version control would defeat the point of having one.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Superset's own metadata (dashboards, charts, users) - separate from
# RisingWave, which holds the actual T24 data being charted.
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@superset-db:5432/superset"
