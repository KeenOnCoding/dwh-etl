from commons.db.query_runner import QueryRunner
from commons.db.sqlalchemyutils import create_engine
from commons.configurations.configurator import Configurator
from processor.work_experience_processor.datamanager import DataManager
from processor.work_experience_processor.datasource import DataSource
from processor.work_experience_processor.main import WorkExperienceRunner


def get_runner():
    config = Configurator().get_configurator()
    engine = create_engine(config)
    query_runner = QueryRunner(config, engine)
    datamanager = DataManager(query_runner)
    datasource = DataSource(query_runner)
    return WorkExperienceRunner(config, engine, query_runner, datamanager, datasource)


def main():
    runner = get_runner()
    runner.run()


if __name__ == "__main__":
    main()
