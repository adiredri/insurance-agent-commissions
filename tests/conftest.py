import pytest

from commissions_pipeline.utils.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("pytest")
    yield session
    session.stop()
