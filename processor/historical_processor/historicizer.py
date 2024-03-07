from typing import Dict, Tuple, List

from commons.spark.spark_db_data_source import SparkDBDataSource
from commons.spark.spark_db_sink import SparkDBSink
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as f


def _generate_historical_filter(id_to_maximum_version: Dict = None):
    def pandas_historical_filter(key, pdf):
        # spark passes tuple here of one element
        key = key[0]
        pdf = pdf.sort_values(by='version') \
            .reset_index(drop=True)

        # if we have don't have record with the same id in the history table
        # let record with the minimum version from the staging be the first
        # in the history
        if id_to_maximum_version is None:
            prev_version = pdf.loc[pdf['version'].idxmin()][
                'hashsum'].strip()
        else:
            # else - take the hashsum of the latest version of the record
            prev_version = id_to_maximum_version[key]['hashsum'].strip()

        mask = [False] * len(pdf)

        # if there are records that are not in the history table, the first
        # record will clash with itself on the first iteration
        # so we eliminate the case by passing boolean flag and setting it to
        # False after the first iteration
        pass_first = id_to_maximum_version is None

        for index, row in pdf.iterrows():
            current_hashsum = row['hashsum'].strip()
            if current_hashsum != prev_version or pass_first:
                prev_version = current_hashsum
                pass_first = False
                mask[index] = True

        return pdf[mask]

    return pandas_historical_filter


class Historicizer:
    __slots__ = ('required_fields', 'history_table', 'staging_table',
                 'history_field_mapping', 'staging_field_mapping', 'df_reader',
                 'df_appender')

    def __init__(self, *, df_reader: SparkDBDataSource,
                 df_appender: SparkDBSink, history_table_name: str,
                 staging_table_name: str,
                 history_field_mapping: Dict[str, str] = None,
                 staging_field_mapping: Dict[str, str] = None):
        self.history_table = history_table_name
        self.staging_table = staging_table_name
        self.df_reader = df_reader
        self.df_appender = df_appender
        self.history_field_mapping = history_field_mapping
        self.staging_field_mapping = staging_field_mapping

        self.required_fields = {
            'history': {'version': 'version',
                        'hashsum': 'hashsum',
                        'id': 'id'},
            'staging': {'version': 'version',
                        'hashsum': 'hashsum',
                        'id': 'id'},
        }

        self.__override_required_fields()

    def __override_required_fields(self) -> None:
        if self.history_field_mapping is None \
                and self.staging_field_mapping is None:
            return

        for field in self.required_fields['history']:
            try:
                history_substitution_value = self.history_field_mapping[field]
                self.required_fields['history'][field] = history_substitution_value
            except KeyError:
                pass

        for field in self.required_fields['staging']:
            try:
                staging_substitution_value = self.staging_field_mapping[field]
                self.required_fields['staging'][field] = staging_substitution_value
            except KeyError:
                pass

    def historicize(self) -> Tuple[int, int]:
        history_df: DataFrame = self.df_reader.load(self.history_table,
                                                    predicates=None)
        history_df = self.__rename_columns(history_df, mapping='history')

        window = Window.partitionBy('id')
        history_unique_ids_to_max_version = history_df \
            .withColumn('maxVersion', f.max('version').over(window)) \
            .where(f.col('version') == f.col('maxVersion')) \
            .drop('maxVersion')

        max_history_version = history_unique_ids_to_max_version \
            .agg(f.max('version').alias('version')) \
            .first()[0]

        version_predicate = self.__construct_version_predicate(
            max_history_version)

        staging_df: DataFrame = self.df_reader.load(self.staging_table,
                                                    version_predicate) \
            .persist()

        amount_of_records_to_analyze = staging_df.count()

        # if no data to analyze - return
        if amount_of_records_to_analyze == 0:
            return 0, 0

        staging_df_prepared = self.__rename_columns(staging_df,
                                                    mapping='staging') \
            .repartition('id') \
            .persist()

        unique_staging_ids = staging_df_prepared.select('id') \
            .dropDuplicates(['id']) \
            .persist()

        ids_not_in_history = unique_staging_ids.alias('S') \
            .join(
            history_unique_ids_to_max_version.select('id').alias('H').hint(
                'broadcast'),
            f.col('S.id') == f.col('H.id'),
            'left') \
            .where(f.col('H.id').isNull()) \
            .select(f.col('S.id')) \
            .persist()

        new_staging_df_in_history = staging_df_prepared.alias('S') \
            .join(history_unique_ids_to_max_version.alias('H')
                  .hint('broadcast'),
                  (f.col('S.id') == f.col('H.id')) &
                  (f.col('S.version') > f.col('H.version')),
                  'inner') \
            .selectExpr('S.*')

        new_staging_df_not_in_history = staging_df_prepared.alias('S') \
            .join(ids_not_in_history.alias('H').hint('broadcast'),
                  'id', 'inner') \
            .select(f.col('S.*'))

        df_schema = new_staging_df_in_history.schema

        id_to_maximum_version = \
            self.__unpack_df_rows(
                history_unique_ids_to_max_version.select('id', 'version', 'hashsum').collect())

        new_filtered_records_in_history = new_staging_df_in_history \
            .groupby('id') \
            .applyInPandas(_generate_historical_filter(id_to_maximum_version),
                           df_schema) \
            .persist()

        new_filtered_records_not_in_history = new_staging_df_not_in_history \
            .groupby('id') \
            .applyInPandas(_generate_historical_filter(), df_schema) \
            .persist()

        amount_new_not_in_history = new_filtered_records_not_in_history.count()
        amount_new_in_history = new_filtered_records_in_history.count()

        new_historical_records = new_filtered_records_in_history \
            .union(new_filtered_records_not_in_history)

        output = self.__rename_columns(new_historical_records,
                                       mapping='history', forward=False)

        try:
            self.df_appender.save(output, self.history_table)
        except Exception:
            print('An error occur when trying to compute the result. '
                  'Please, consider checking connection properties, '
                  'table and schema names, and fields.')
            raise

        return amount_new_in_history, amount_new_not_in_history

    def __unpack_df_rows(self, df_rows):
        output_mapping = dict()
        for row in df_rows:
            row_dict = row.asDict()
            current_id = row_dict['id']
            output_mapping[current_id] = {
                'version': row_dict['version'],
                'hashsum': row_dict['hashsum']
            }
        return output_mapping

    def __rename_columns(self, df: DataFrame, mapping: str,
                         forward: bool = True) -> DataFrame:
        """
        Renames required DF fields to unify theirs usage.

        For example, if we have the following mapping for required field:
        { id : employee_id, hashsum : hash_sum }, and forward is set to True
        the input dataframe with columns [employee_id, hash_sum] will have its
        columns renamed to [id, hashsum].
        If forward parameter is set to False, the dataframes parameters will be
        renamed to the initial ones (id -> employee_id, hashsum -> hash_sum).

        :param df: df where columns will be renamed
        :param mapping: key of mapping to use (history or staging)
        :param forward: boolean parameter which defines the rename order
        :return: df with renamed columns
        """
        try:
            rename_mapping = self.required_fields[mapping].copy()
        except KeyError:
            print(
                "No such mapping. There are two available options: 'history' "
                "and 'staging'.")
            raise

        if forward:
            rename_mapping = {v: k for k, v in rename_mapping.items()}

        for old_name, new_name in rename_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)

        return df

    def __construct_version_predicate(self, max_history_version) -> List:
        if max_history_version is None:
            max_history_version = 0
        # extract canonical table name from staging, because predicate
        # is applied only for staging table
        return [f'{self.required_fields["staging"]["version"]} '
                f'> {max_history_version}']
