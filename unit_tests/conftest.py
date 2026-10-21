import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    session = DatabricksSession.builder.profile("RETAILQ_DEV").serverless().getOrCreate()
    yield session
    session.stop()
