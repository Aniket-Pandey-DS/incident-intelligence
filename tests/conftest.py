import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("IncidentIntelligence-Tests")
        .master("local[2]")
        .getOrCreate()
    )

    yield session
    session.stop()
