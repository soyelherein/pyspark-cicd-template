# Copyright 2020 soyel.alam@ucdconnect.ie
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This module setup the testing environment and make available the pytest fixtures
"""
from ddl import schema
from dependencies import job_submitter
from pandas.testing import assert_frame_equal
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from typing import Dict, List, Union
from unittest.mock import MagicMock, create_autospec, PropertyMock
import json
import os
import pytest
import unittest

__owner__ = 'soyel.alam@ucdconnect.ie'
__docformat__ = 'Sphinhx'


class SparkETLTests(unittest.TestCase):
    """
    This class does the testing setup for the spark code.\
    Object of this class is available throughout testing session.\
    Prepares gold standard data as mapped in the testbed.json file\
    Create necessary dataframes and tables, apply any schema defined in schema.py.\
    Reads the job configuration defined config_file \
    and append any testing specific or command line configurations if specified in the testbed.json file.\
    Defines mock objects to introspect the calling mechanism of a few spark libraries.\
    Defines helper function to comapare the gold standard data with processes output.

    :param config_file: Relative path of the config file, defaults to 'configs/config.json'
    :type config_file: str, optional
    :param extra_file_options: Extra file options for the testing datafiles, defaults to {}
    :type extra_file_options: Dict, optional
    :ivar dataframes: Dictionary to hold dataframes for the testing, defaults to {}
    :type dataframes: Dict
    :ivar mock_df: Mock object with spec of spark Dataframe to introspect the calling behaviour
    :type mock_df: MagicMock
    :ivar mock_spark: Mock object with spec of SparkSession to introspect the calling behaviour
    :type mock_spark: MagicMock

    """

    def __init__(self, config_file: str = 'configs/config.json', extra_file_options: Dict = {}):
        self.__root_dir = str(Path(__file__).parent.parent)
        self.file_options: Dict = {'format': 'csv',
                                   'sep': ',',
                                   'ignoreLeadingWhiteSpace': True,
                                   'ignoreTailingWhiteSpace': True,
                                   'header': True,
                                   'inferSchema': True}
        self.file_options.update(extra_file_options)
        self.config_file = os.path.join(self.__root_dir, config_file)
        self.dataframes: Dict = {}
        self.mock_df: MagicMock = create_autospec(DataFrame)
        self.mock_spark: MagicMock = create_autospec(SparkSession)
        # .parent.parent as conftest.py is now placed 2 levels below root directory
        self.setUp()
        super().__init__()

    def setUp(self):
        """Start Spark, read configs, create the Dataframes and mocks
        """
        self.spark, self.logger = job_submitter.create_spark_session('test_pipeline')
        self.config: Dict = job_submitter.load_config_file(self.config_file)
        self.setup_testbed()
        self.setup_mocks()

    def tearDown(self):
        """Stop Spark
        """
        self.teardown_testbed()
        self.spark.stop()

    def setup_testbed(self):
        """Creates the Dataframes and tables from the test files as mapped in tests/testbed.json, \
        store those in instance variable named dataframes. \
        It also enriches the test specific job configurations as per the testbed.json

        tests/test_data/page_views.csv
        email,pages
        james@example.com,home
        james@example.com,about
        patricia@example.com,home

        ddl/schema.py
        page_views = StructType(
        [StructField('email', StringType(), True),
        StructField('pages', StringType(), True)])

        testbed.json
        {
        "data": {
        "page_views": { "file": "tests/test_data/page_views.csv" , "schema": "page_views"}
        }
        }
        """

        try:
            with open(os.path.join(self.__root_dir, 'tests/testbed.json')) as f:
                test_bed_conf: Dict = json.load(f)
                data_dict: Dict = test_bed_conf.get('data')
                self.logger.info('loading test data from testbed')
                for df, meta in data_dict.items():
                    dataframe: DataFrame = self.spark.read.load(meta.get('file'),
                                                                schema=getattr(schema, meta.get('schema'), None),
                                                                **self.file_options)
                    self.dataframes[df] = dataframe
                    if len(df.split('.')) > 1:
                        self.spark.sql(f'create database if not exists {df.split(".")[0]}')
                    dataframe.write.saveAsTable(df, format='hive', mode='overwrite')
                    self.logger.info(f'loaded{df} from {meta.get("file")}')

                conf: Dict = test_bed_conf.get('config')
                self.config.update(conf)
                self.logger.info(f'loaded test config {self.config}')

        except FileNotFoundError:
            self.logger.info('No test data to cook')

    def teardown_testbed(self):
        """Removes the local tables created as part of testing
        """

        try:
            with open('tests/testbed.json') as f:
                test_bed_conf: Dict = json.load(f)
                data_dict: Dict = test_bed_conf.get('data')
                self.logger.info('loading test data from testbed')
                for df, meta in data_dict.items():
                    dataframe: DataFrame = self.spark.read.load(meta.get('file'),
                                                                schema=getattr(schema, meta.get('schema'), None),
                                                                **self.file_options)
                    self.dataframes[df] = dataframe
                    if len(df.split('.')) > 1:
                        self.spark.sql(f'drop database if not exists {df.split(".")[0]}')
                    self.spark.sql(f'drop table if exists {df}')

        except FileNotFoundError:
            self.logger.info('No test data to cook')

    def setup_mocks(self):
        """Mocking spark and dataframes to introspect the calling behaviour for unittesting
        """
        mock_read = create_autospec(DataFrameReader)
        mock_write = create_autospec(DataFrameWriter)
        type(self.mock_spark).read = PropertyMock(return_value=mock_read)
        type(self.mock_df).write = PropertyMock(return_value=mock_write)

    @classmethod
    def comapare_dataframes(cls, df1: DataFrame, df2: DataFrame, excluded_keys: Union[List, str, None] = []) -> bool:
        """
        Compares 2 DataFrames for exact match\
        internally it use pandas.testing.assert_frame_equal


        :param df1: processed data
        :type df1: DataFrame
        :param df2: gold standard expected data
        :type df2: DataFrame
        :return: True
        :param excluded_keys: columns to be excluded from comparision, optional
        :type excluded_keys: Union[List, str, None]
        :rtype: Boolean
        :raises: AssertionError Dataframe mismatch
        """
        excluded_keys = excluded_keys if type(excluded_keys) == list else [excluded_keys]
        df1 = df1.drop(*excluded_keys)
        df2 = df2.drop(*excluded_keys)
        sort_columns = [cols[0] for cols in df1.dtypes]
        df1_sorted = df1.toPandas().sort_values(by=sort_columns, ignore_index=True)
        df2_sorted = df2.toPandas().sort_values(by=sort_columns, ignore_index=True)
        assert_frame_equal(df1_sorted, df2_sorted)
        return True


@pytest.fixture(scope='session')
def testbed():
    """Defines pyspark fixture"""
    testbed = SparkETLTests()
    yield testbed
    testbed.tearDownClass()
