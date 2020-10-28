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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, coalesce
from typing import Dict, Tuple
from ddl import schema


def extract(spark: SparkSession, config: Dict, logger) -> Tuple[DataFrame, DataFrame]:
    """Read incremental file and historical data and return as DataFrames

    :param spark: Spark session object.
    :type spark: SparkSession
    :param config: job configuration
    :type config: Dict
    :param logger: Py4j Logger
    :type logger: Py4j.Logger
    :return: Spark DataFrames.
    :rtype: DataFrame
    """
    inc_df: DataFrame = spark.read.load(path=config['page_views_path'],
                                        format='csv',
                                        header=True,
                                        schema=schema.page_views)
    prev_df: DataFrame = spark.read.table(tableName=config['user_pageviews_tab'])

    return inc_df, prev_df


def transform(inc_df: DataFrame, prev_df: DataFrame, config: Dict, logger) -> DataFrame:
    """Transform the data for final loading.

    :param inc_df: Incremental DataFrame.
    :type inc_df: DataFrame
    :param prev_df: Final DataFrame.
    :type prev_df: DataFrame
    :param config: job configuration
    :type config: Dict
    :param logger: Py4j Logger
    :type logger: Py4j.Logger
    :return: Transformed DataFrame.
    :rtype: DataFrame
    """

    # calculating the metrics
    inc_df: DataFrame = (inc_df.groupBy('email').count().
                         select(['email',
                                 col('count').alias('page_view'),
                                 lit(config['process_date']).alias('last_active')
                                 ])
                         )

    # merging the data with historical records
    df_transformed: DataFrame = (inc_df.join(prev_df,
                                             inc_df.email == prev_df.email,
                                             'full').
                                 select([coalesce(prev_df.email, inc_df.email).
                                        alias('email'),
                                         (coalesce(prev_df.page_view, lit(0))
                                          +
                                          coalesce(inc_df.page_view, lit(0))).
                                        alias('page_view'),
                                         coalesce(prev_df.created_date,
                                                  inc_df.last_active).cast('date').
                                        alias('created_date'),
                                         coalesce(inc_df.last_active,
                                                  prev_df.last_active).cast('date').
                                        alias('last_active')
                                         ])
                                 )

    return df_transformed


def load(df: DataFrame, config: Dict, logger) -> bool:
    """Write data in final destination

    :param df: DataFrame to save.
    :type df: DataFrame
    :param config: job configuration
    :type config: Dict
    :param logger: Py4j Logger
    :type logger: Py4j.Logger
    :return: True
    :rtype: bool
    """
    df.write.save(path=config['output_path'], mode='overwrite')
    return True


def run(spark: SparkSession, config: Dict, logger) -> bool:
    """
    Entry point to the pipeline

    :param spark: SparkSession object
    :type spark: SparkSession
    :param config: job configurations and command lines
    :param logger: Log4j Logger
    :type logger: Log4j.Logger
    :type config: Dict
    :return: True
    :rtype: bool
    """

    logger.warn('pipeline is starting')

    # execute the pipeline
    inc_data, prev_data = extract(spark=spark, config=config, logger=logger)
    transformed_data = transform(inc_df=inc_data,
                                 prev_df=prev_data,
                                 config=config,
                                 logger=logger)
    load(df=transformed_data, config=config, logger=logger)

    logger.warn('pipeline is complete')
    return True
