from pyspark.sql import DataFrame


class SparkSpreadsheetsDataSource:

    def __init__(self, spark_session, data) -> None:
        self.spark_session = spark_session
        self.data = data

    def get_dataframe(self) -> DataFrame:
        return self.spark_session.createDataFrame(
            data=self.data, schema=self.data.pop(0))
