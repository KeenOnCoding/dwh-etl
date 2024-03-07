from pyspark.sql.types import StructType
import pyspark.sql.functions as F


def create_empty_dataframe(spark):
    schema = StructType()
    df = spark.createDataFrame([], schema)
    return df


def get_df_data_size(df, nested_field):
    if nested_field:
        response_size_df = df.select(F.size(F.col(nested_field)))
        response_size = response_size_df.collect()[0][0]
    else:
        response_size = df.count()
    return response_size


def df_string(df, lines=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return (df._jdf.showString(lines, 20, vertical))
    else:
        return (df._jdf.showString(lines, int(truncate), vertical))
