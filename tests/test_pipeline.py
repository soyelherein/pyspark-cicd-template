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
This module contains unit tests, shallow integration tests using mock and integration tests for
jobs.pipeline.py.
"""
from jobs import pipeline
from tests.conftest import SparkETLTests
from unittest.mock import patch
from pyspark.sql import DataFrame
from ddl import schema


def test_pipeline_transform_with_sample(testbed: SparkETLTests):
    """Test pipeline.transform method using small chunks of input data and expected output data\
    to make sure the function is behaving as expected.
    .. seealso:: :class:`SparkETLTests`

    """
    # Given - getting the input dataframes
    inc_df: DataFrame = testbed.dataframes['page_views']
    prev_df: DataFrame = testbed.dataframes['soyel_db.user_pageviews']
    # getting the expected dataframe
    expected_data: DataFrame = testbed.dataframes['expected_output_user_pageviews']
    # When - actual data
    transformed_data: DataFrame = pipeline.transform(inc_df=inc_df,
                                                     prev_df=prev_df,
                                                     config=testbed.config,
                                                     logger=testbed.logger)
    # Then - comparing the actual and expected data
    testbed.comapare_dataframes(df1=transformed_data, df2=expected_data)


def test_pipeline_extract_mock_calls(testbed: SparkETLTests):
    """Test pipeline.extract method using the mocked spark session and introspect the calling pattern\
    to make sure spark methods were called with intended arguments
    .. seealso:: :class:`SparkETLTests`

    """
    # When - calling the extract method with mocked spark and test config
    pipeline.extract(spark=testbed.mock_spark,
                     config=testbed.config,
                     logger=testbed.config)
    # Then - introspecting the spark method call
    testbed.mock_spark.read.load.assert_called_once_with(
        path='/user/soyel/pyspark-cicd-template/input/page_views',
        format='csv',
        header=True,
        schema=schema.page_views)
    testbed.mock_spark.read.table.assert_called_once_with(tableName='soyel_db.user_pageviews')
    testbed.mock_spark.reset_mock()


def test_pipeline_load_mock_calls(testbed: SparkETLTests):
    """Test pipeline.load method using the mocked spark session and introspect the calling pattern\
    to make sure spark methods were called with intended arguments
    .. seealso:: :class:`SparkETLTests`

    """
    # When - calling the extract method with mocked spark and test config
    pipeline.load(df=testbed.mock_df, config=testbed.config, logger=testbed.config)
    # Then - introspecting the spark method call
    testbed.mock_df.write.save.assert_called_once_with(
        path='/user/soyel/pyspark-cicd-template/output/user_pageviews',
        mode='overwrite')
    testbed.mock_df.reset_mock()


def test_run_integration(testbed: SparkETLTests):
    """Test pipeline.run method to make sure the integration is working fine\
    It avoids reading and writing operations by mocking the load and extract method
    .. seealso:: :class:`SparkETLTests`

    """
    # Given
    with patch('jobs.pipeline.load') as mock_load:
        with patch('jobs.pipeline.extract') as mock_extract:
            mock_load.return_value = True
            mock_extract.return_value = (testbed.dataframes['page_views'],
                                         testbed.dataframes['soyel_db.user_pageviews'])
            # When
            status = pipeline.run(spark=testbed.spark,
                                  config=testbed.config,
                                  logger=testbed.logger)
            # Then
            testbed.assertTrue(status)
