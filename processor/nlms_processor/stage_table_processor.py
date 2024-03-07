"""
This class is the second stage.
Data from nlms_report_generator are transferred here.
At the output, we have a dataframe ready to be loaded into DWH.
"""
from functools import reduce
import time
import calendar
import pyspark
from pyspark import SparkFiles
from pyspark.sql.functions import lit
from commons.logging.simple_logger import logger

base_cols = ['Student ID', 'Email', 'Username', 'Grade', 'Homework', 'Enrollment Track',
        'Verification Status', 'Certificate Eligible', 'Certificate Delivered',
        'Certificate Type', 'Enrollment Status', 'Course ID', 'Course Name',
        'Enrollment Date', 'Course Passing Date', 'Homework (Avg)']

cols = ['student_id', 'email', 'username', 'grade', 'homework', 'enrollment_track',
        'verification_status', 'certificate_eligible', 'certificate_delivered',
        'certificate_type', 'enrollment_status', 'course_id', 'course_name',
        'enrollment_date', 'course_passing_date', 'homework_avg', 'batch_id']


class StageTableProcessor:
    """
    In this class, data is extracted from reports,
    dataframes are created for each of them,
    and then they are combined into single dataframe,
    which is ready to be loaded into DWH.
    """
    def __init__(self, spark_session, nlms_reports):
        self.spark_session = spark_session
        self.nlms_reports = nlms_reports

    def create_stage_table(self):
        """
        In this method, a data frame is created
        with a structure similar to the table stage in DWH.
        """
        dataframe = self.create_single_dataframe_from_all_reports(
            spark=self.spark_session,
            reports_url=self.nlms_reports)
        logger.info("Created dataframe from all reports")

        dataframe = dataframe.select(*base_cols)
        logger.info(f"Selected columns for stage table: {base_cols}")

        timestamp = self.get_timestamp()
        logger.info(f"TIMESTAMP: {timestamp}")

        dataframe = dataframe.withColumn('batch_id', lit(timestamp)).toDF(*cols)
        logger.info(f"Added 'batch_id' column and replaced columns names {base_cols} by {cols}")

        return dataframe

    def create_single_dataframe_from_all_reports(self,
                                                 spark,
                                                 reports_url) -> pyspark.sql.dataframe.DataFrame:
        """
        List of urls of all reports is input,
        they are processed to the list of PySpark dataframes,
        then all dataframes are combined into one
        by calling the union_all_dataframes_by_names() method.
        """
        all_dfs = []

        for report_url in reports_url:
            filename = report_url.split("/")
            spark.sparkContext.addPyFile(report_url)
            dataframe = spark.read\
                .csv("file://" + SparkFiles.get(f"{filename[-1]}"),
                     header=True)
            all_dfs.append(dataframe)

        all_reports_final_dataframe = self.union_all_dataframes_by_names(dfs=all_dfs)

        return all_reports_final_dataframe

    @staticmethod
    def union_all_dataframes_by_names(dfs: list):
        """
        this method takes as input a list of dataframes
        and union them into one.
        If cols_to_drop parameter is specified -
        method drops columns from the list/
        """
        final_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

        return final_df

    @staticmethod
    def get_timestamp() -> int:
        """
        It returns the corresponding Unix timestamp value,
        assuming an epoch of 1970-01-01.
        Hence, no extra adjustment in the DateTime object is made.
        """
        current_gmt = time.gmtime()
        time_stamp = calendar.timegm(current_gmt)

        return time_stamp
