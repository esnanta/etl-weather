from pipeline.database import (
    DatabaseConfig,
    create_database_engine,
    create_observation_table,
    create_schema,
)


def main() -> None:

    config = DatabaseConfig()

    engine = create_database_engine(
        config,
    )

    create_schema(
        engine,
        config,
    )

    create_observation_table(
        engine,
        config,
    )


if __name__ == "__main__":
    main()