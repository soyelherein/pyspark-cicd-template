from pyspark.sql.types import *

page_views = StructType(
    [StructField('email', StringType(), True),
     StructField('pages', StringType(), True)])

user_pageviews = StructType(
    [
        StructField('email', StringType(), True),
        StructField('page_view', LongType(), True),
        StructField('created_date', DateType(), True),
        StructField('last_active', DateType(), True)
    ]
)



