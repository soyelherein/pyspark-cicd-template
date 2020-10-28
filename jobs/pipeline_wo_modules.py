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
from pyspark.sql.functions import col, lit, coalesce, current_date
from pyspark.sql.types import *

"""
Incremental file: input/page_views
        email,pages
        james@example.com,home
        james@example.com,about
        patricia@example.com,home
Final Table::
        +-----------------+---------+------------+-----------+
        |email            |page_view|created_date|last_active|
        +-----------------+---------+------------+-----------+
        |james@example.com|10       |2020-01-01  |2020-07-04 |
        |mary@example.com |100      |2020-02-04  |2020-02-04 |
        |john@example.com |1        |2020-03-04  |2020-06-04 |
        +-----------------+---------+------------+-----------+
"""

spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()

# Configs and variables
page_views = StructType(
    [
        StructField('email', StringType(), True),
        StructField('pages', StringType(), True)
    ]
)

page_views_path = '/user/soyel/pyspark-cicd-template/input/page_views'
user_pageviews_tab = 'soyel_db.user_pageviews'
output_path = '/user/soyel/pyspark-cicd-template/output/user_pageviews'

# Extract
inc_df: DataFrame = spark.read.csv(path=page_views_path,
                                   header=True,
                                   schema=page_views)
prev_df: DataFrame = spark.read.table(tableName=user_pageviews_tab)

# Transform
inc_df: DataFrame = (inc_df.groupBy('email').count().
                     select(['email',
                             col('count').alias('page_view'),
                             current_date().alias('last_active')
                             ])
                     )

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

# Load
df_transformed.write.save(path=output_path, mode='overwrite')

spark.stop()
