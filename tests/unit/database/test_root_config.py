"""Root database configuration and orchestration/geospatial boundaries."""

import pytest

from database.config import DatabaseConfig, resolve_test_database_config
from database.verification import CLIMATE_TABLES, GEOSPATIAL_TABLES


def test_application_config_never_uses_test_prefix():
    env = {
        "TEST_POSTGRES_HOST": "localhost",
        "TEST_POSTGRES_PORT": "5432",
        "TEST_POSTGRES_DATABASE": "weather_test",
        "TEST_POSTGRES_USER": "tester",
        "TEST_POSTGRES_PASSWORD": "secret",
    }
    with pytest.raises(ValueError, match="POSTGRES_HOST"):
        DatabaseConfig.from_env(env)


def test_test_config_never_falls_back_to_application():
    env = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DATABASE": "weather",
        "POSTGRES_USER": "developer",
        "POSTGRES_PASSWORD": "secret",
    }
    with pytest.raises(ValueError, match="TEST_POSTGRES_HOST"):
        resolve_test_database_config(env)


def test_test_config_requires_explicit_test_identity():
    env = {
        "TEST_POSTGRES_HOST": "localhost",
        "TEST_POSTGRES_PORT": "5432",
        "TEST_POSTGRES_DATABASE": "weather",
        "TEST_POSTGRES_USER": "tester",
        "TEST_POSTGRES_PASSWORD": "secret",
    }
    with pytest.raises(ValueError, match="test database"):
        resolve_test_database_config(env)


def test_url_handles_reserved_password_characters():
    config = DatabaseConfig("localhost", 5432, "weather", "dev", "p@ss:/#word")
    assert config.sqlalchemy_url.password == "p@ss:/#word"
    assert "%40" in config.url


def test_domains_have_exactly_five_and_three_disjoint_tables():
    assert len(CLIMATE_TABLES) == 5
    assert len(GEOSPATIAL_TABLES) == 3
    assert not (CLIMATE_TABLES & GEOSPATIAL_TABLES)
