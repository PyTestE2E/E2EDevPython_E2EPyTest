# conftest.py

import pytest
from sqlalchemy import create_engine


from Configuration.Config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
)


@pytest.fixture(scope="session")
def mysql_conn_stage():
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )
    yield engine
    engine.dispose()

