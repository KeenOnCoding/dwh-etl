from abc import ABC, abstractmethod
from commons.utils.validators import require_schema_presence


class AbstractDatabaseSink(ABC):

    @abstractmethod
    def save(self, data, table_name):
        pass


class SparkDBSink(AbstractDatabaseSink):

    def __init__(self, config, connection_properties, spark):
        self.config = config
        self.url = connection_properties.url
        self.properties = connection_properties.properties
        self.spark = spark

    def save(self, data, table_name=None):
        if table_name is None:
            raise ValueError('Table name should be set!')

        table_name = require_schema_presence(table_name)

        data.write.jdbc(
            url=self.url,
            properties=self.properties,
            table=table_name,
            mode="append"
        )
