from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, coalesce, current_date
from typing import Tuple


def extract(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    inc_df: DataFrame = spark.read.load(path=config['page_views'])
    prev_df: DataFrame = spark.read.table(tableName=config['user_pageviews'])
    return inc_df, prev_df


def transform(inc_df: DataFrame, prev_df: DataFrame) -> DataFrame:
    # calculating the metrics
    inc_df: DataFrame = inc_df.groupBy('email').count(). \
        select(['email',
                col('count').alias('page_view'),
                lit(config['process_date']).alias('last_active')
                ])

    # merging the data with historical records
    df_transformed: DataFrame = inc_df.join(prev_df, inc_df.email == prev_df.email, 'full'). \
        select([coalesce(prev_df.email, inc_df.email).alias('email'),
                (coalesce(prev_df.page_view, lit(0)) + coalesce(inc_df.page_view, lit(0))).alias('page_view'),
                coalesce(prev_df.created_date, inc_df.last_active).cast('date').alias('created_date'),
                coalesce(inc_df.last_active, prev_df.last_active).cast('date').alias('last_active')
                ])

    return df_transformed


def load(df: DataFrame, config: Dict, logger) -> bool:
    df.write.save(path=config['output_path'], mode='overwrite')
    return True


def run() -> bool:
    spark: SparkSession = SparkSession.builder.enableHiveSupport().getOrCreate()
    # execute the pipeline
    inc_data, prev_data = extract(spark=spark)
    transformed_data = transform(inc_df=inc_data, prev_df=prev_data)
    load(df=transformed_data)
    spark.stop()
    return True


if __name__ == '__main__':
    run()
