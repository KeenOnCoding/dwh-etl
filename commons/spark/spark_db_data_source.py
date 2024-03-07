from commons.utils.validators import require_schema_presence


class SparkDBDataSource:

    def __init__(self, config, connection_properties, spark):
        self.config = config
        self.url = connection_properties.url
        self.properties = connection_properties.properties
        self.spark = spark

    def load(self, table_name, predicates):
        table_name = require_schema_presence(table_name)

        return self.spark.read.jdbc(
            url=self.url,
            table=table_name,
            properties=self.properties,
            predicates=predicates
        )
