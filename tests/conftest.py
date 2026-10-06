from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from ETL.spark import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One Spark session shared by all tests."""
    session = get_spark()
    yield session
    session.stop()
