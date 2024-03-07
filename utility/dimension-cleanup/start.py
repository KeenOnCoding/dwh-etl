from commons.db.query_runner import QueryRunner
from commons.db.sqlalchemyutils import create_engine
from configurations.configurator import Configurator
from main import DimensionCleanupRunner


def get_runner():
    config = Configurator().get_configurator()

    engine = create_engine(config)

    query_runner = QueryRunner(config, engine)

    return DimensionCleanupRunner(config, query_runner)


def main():
    runner = get_runner()
    runner.run()


if __name__ == "__main__":
    main()
