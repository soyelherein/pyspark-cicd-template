### Environment agnostic PySpark data-pipeline testing and CICD

### ****Motivation**

One major challenge in data pipeline implementation is how to reliably test the pipelines. As the outcome of the code is tightly coupled with data and the environment.
This consequently blocks the developer to follow test-driven development, Identify early bugs by writing good unit testing, and releasing the code via CICD with confidence.

One way to overcome this challenge is to use immutable data to run and test the pipeline. So that the result of ETL functions can be matched against known outputs.
Obviously, this requires a good knowledge of the application and how good is the data matches the business requirements.
It also needs some setups to enable the developer to focus on building the application instead of spending time on the environment preparation.
This blog-post focuses on this second aspect and aims at providing a model of self-contained data pipelines with CICD implementation.

### **Introduction**

The idea is to incrementally develop and test the pipeline based on locally stored data in immutable files. We use [Apache Spark](https://spark.apache.org/) and its Python(PySpark) APIs for developing data pipelines and [pytest](https://docs.pytest.org/en/stable/) to test it.

Required spark environment, DataFrames, and tables will be made available during testing using a pyspark based [conftest](https://docs.pytest.org/en/2.7.3/plugins.html?highlight=re) file based on the configuration stored in a JSON file named test_bed.json.

We will structure decouple and structure our pipeline into modules. Then focus on the heavy lifting of testing environment setup. Once we have the test cases ready, It would be plugged into Jenkins based CICD.

This blog is very detailed and meant to be followed along with the code in https://github.com/soyelherein/pyspark-cicd-project

For demonstration purposes, let’s consider we have a single file of pipeline that consumes file containing “pageviews” data and merges it into a final target table. The code tightly coupled with the environment in terms of files and data. It is a single standalone file taking care of everything starting from starting and stopping SparkSession.

```python
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

page_views_path = '/user/stabsumalam/pyspark-cicd-template/input/page_views'
user_pageviews_tab = 'stabsumalam_db.user_pageviews'
output_path = '/user/stabsumalam/pyspark-cicd-template/output/user_pageviews'

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
```



The application can be submitted using Spark. 

```shell
$SPARK_HOME/bin/spark-submit pipeline_wo_modules.py
```

If you look closely, there are five major sections in the pipeline creation of spark session, static configuration variables, Extract, Transform, Load.

Let’s now deep dive into structuring the project. The overall structure would look like below, each part will be explained in the later section:

```shell
root/
 |-- configs/
 |   |-- config.json
 |-- dependencies/
 |   |-- job_submitter.py
 |-- ddl/
 |   |-- schema.py
 |-- jobs/
 |   |-- pipeline.py
 |-- tests/
 |   |-- test_data/
 |   |-- | -- employees/
 |   |-- | -- employees_report/
 |   |-- conftest.py
 |   |-- test_bed.json
 |   |-- test_pipeline.py
 |  Dockerfile
 |  Jenkinsfile
 |  Makefile
 |  Pipfile
```

#### Decouple Spark Environment

#### Decouple Spark Environment

It is not practical to test and debug Spark jobs by sending them to a cluster using spark-submit and examining stack traces for clues on what went wrong.
Fortunately, we can use [Pypi Pyspark](https://pypi.org/project/pyspark/) along with [pipenv](https://docs.pipenv.org) to manage an isolated environment for our pipeline.

**pipenv** — [pipenv](https://pypi.org/project/pipenv/) helps us managing project dependencies and Python environments (i.e. virtual environments). All development and production dependencies are described in the Pipfile. Once you have pipenv and compatible python available. Pyspark, pytest, and any additional dependencies can be installed with this simple command.

```
pip install pipenv --dev
```

**dependencies.job_submitter** — Since a data application can have numerous upstream and downstream pipelines, It makes sense to take out the spark environment management and other common tasks into a shared entry point. So, an application can focus only on the business logic.

This submitter module takes the job name as an argument and executes the functionality defined in it. With this submitter module, the command is changed like below:

```shell
$SPARK_HOME/bin/spark-submit \
 --py-files dependencies/job_submitter.py, jobs/pipeline_wo_modules.py \
dependencies/job_submitter.py --job pipeline_wo_modules
```

It has the capability to parse static configuration from JSON files and pass any dynamic argument to the job as a dictionary. The pipeline itself has to expose a run method(discussed in the Decouple Application section) that is the entry point for the ETL.

It is entrusted with starting and stopping spark sessions, parsing the configuration files containing static variables, and any dynamic command-line arguments then executing the requested pipeline. Please head back to the [Github](https://github.com/soyelherein/pyspark-cicd-project) repo for the implementation.

#### Decouple Application

**Decouple Application

**jobs** — We design our functions to have Extract and Load functions to handle the IO operations, we will test those using mocks to avoid side effects. Transform functions are designed to be side effect free that takes DataFrames input and returns DataFrames output which can be compared against the locally stored data. additionally, we will have an entry point method named to run for our pipeline that does the integration of the ETL. Developers are encouraged to have different pipeline files inside the jobs directory focusing on different business logic instead of having a single big file.

> Extract — Reads the incremental file and historical data from the table and return 2 Dataframes

> Transform — Calculates the metrics based on incremental and historical DataFrames and return a final DataFrame

> Load — Writes the data into the final output path

> Run — Does the integration between ETL process. It is exposed to the job submitter module. It accepts the spark session, job configurations, and a logger object to execute the pipeline.

**configs** **and ddl** — We will take out the static configurations and place them in a JSON file (configs/config.json) so that it can be overwritten as per the test config.

```json
{
  "page_views_path": "/user/stabsumalam/pyspark-cicd-template/input/page_views",
  "user_pageviews_tab": "stabsumalam_db.user_pageviews",
  "output_path" : "/user/stabsumalam/pyspark-cicd-template/output/user_pageviews"
}
```

As explained in the job_submitter module, this config along with any dynamic parameters to the job are made available to the pipeline methods as dictionary.

We will also take out the schema from the code in the ddl/schema.py file, this will be helpful to create the test data in the form of DataFrames and Tables using a helper method during testing.

```python
def extract(spark: SparkSession, config: Dict, logger) -> Tuple[DataFrame, DataFrame]:
    """Read incremental file and historical data and return as DataFrames
    """
    inc_df: DataFrame = spark.read.load(path=config['page_views_path'],
                                        format='csv',
                                        header=True,
                                        schema=schema.page_views)
    prev_df: DataFrame = spark.read.table(tableName=config['user_pageviews_tab'])

    return inc_df, prev_df


def transform(inc_df: DataFrame, prev_df: DataFrame, config: Dict, logger) -> DataFrame:
    """Transform the data for final loading.
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
    """
    df.write.save(path=config['output_path'], mode='overwrite')
    return True


def run(spark: SparkSession, config: Dict, logger) -> bool:
    """
    Entry point to the pipeline
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
```

Given that we have structured our ETL jobs in testable modules. We are all set to focus on the tests.

#### Testbed

**conftest** — We have used [pytest](https://docs.pytest.org/en/stable/) style tests for our pipeline along with leveraging a few features (i.e. mock, patch) form [unittest](https://docs.python.org/3/library/unittest.html). This file does the heavy lifting of setting up the jobs for tests i.e providing test sparkSession and mocks creating the tables and DataFrames locally from the CSV files. The mapping is defined in the testbed.json file. 

```json
{
    "data": {
        "page_views": {
            "file": "tests/test_data/page_views.csv",
            "schema": "page_views"
        },
        "stabsumalam_db.user_pageviews": {
            "file": "tests/test_data/user_pageviews.csv",
            "schema": "user_pageviews"
        },
        "expected_output_user_pageviews": {
            "file": "tests/test_data/expected_user_pageviews.csv",
            "schema": "user_pageviews"
        }
    },
    "config": {
        "process_date": "2020-07-21"
    }
}
```

This config is pretty self-explanatory. We have defined the DataFrame and table details under the “data” key. If the job accepts any dynamic parameter as job-args(i.e. process_date), That override should be part of the “config” key. It would be sent as a dictionary argument to the job. setup_testbed(Please refer to the [Github](https://github.com/soyelherein/pyspark-cicd-project) for implementation details) helper method is responsible for producing the DataFrame and tables once the test_bed.json file is configured. The file format can be configured as per the need in the conftest, default is as shown below.

```json
self.file_options: Dict = {'format': 'csv',
                                   'sep': ',',
                                   'ignoreLeadingWhiteSpace': True,
                                   'ignoreTailingWhiteSpace': True,
                                   'header': True,
                                   'inferSchema': True}
```

For read and write operations we encourage to use the generic methods like “[read.load](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)” and “[write](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)”, instead of “read.csv” or “read.orc” so that our mocks can be more generic. Anyway, this file must be changed as per your need.

**test_pipeline —**We have created a session-level [pytest fixture](https://docs.pytest.org/en/stable/fixture.html) containing all the hard works done in the conftest in an object. As you see in the later section we will perform the entire testing using it’s member attributes. 

Now let’s test our **transform** method that takes the incremental and historical DataFrames as input and produces the final DataFrame.

```python
def test_pipeline_transform_with_sample(testbed: SparkETLTests):
    """Test pipeline.transform method using small chunks of input \
    against expected output\
    to make sure the function is behaving as expected.
    """
    # Given - getting the input dataframes
    inc_df: DataFrame = testbed.dataframes['page_views']
    prev_df: DataFrame = testbed.dataframes['stabsumalam_db.user_pageviews']
    # getting the expected dataframe
    expected_data: DataFrame = testbed.dataframes['expected_output_user_pageviews']
    # When - actual data
    transformed_data: DataFrame = pipeline.transform(inc_df=inc_df,
                                                     prev_df=prev_df,
                                                     config=testbed.config,
                                                     logger=testbed.logger)
    # Then - comparing the actual and expected data
    testbed.comapare_dataframes(df1=transformed_data, df2=expected_data)
```

Since the I/O operations are already been separated out we can introspect the calling behavior of **extract and load** using mocks. These mocks are setup in the conftest file.

```python
def test_pipeline_extract_mock_calls(testbed: SparkETLTests):
    """Test pipeline.extract method using the mocked spark session \
    and introspect the calling pattern\
    to make sure spark methods were called with intended arguments
    .. seealso:: :class:`SparkETLTests`

    """
    # calling the extract method with mocked spark and test config
    pipeline.extract(spark=testbed.mock_spark,
                     config=testbed.config,
                     logger=testbed.config)
    # introspecting the spark method call
    testbed.mock_spark.read.load.assert_called_once_with(
        path='/user/stabsumalam/pyspark-cicd-template/input/page_views',
        format='csv',
        header=True,
        schema=schema.page_views)
    testbed.mock_spark.read.table.assert_called_once_with(
        tableName='stabsumalam_db.user_pageviews')
    testbed.mock_spark.reset_mock()
```

Since we have already tested individual methods we can make use of patching to do the **integration** test by patching the outcomes of different functions and avoiding side-effects of writing into the disk.

```python
def test_run_integration(testbed: SparkETLTests):
    """Test pipeline.run method to make sure the integration is working fine\
    It avoids reading and writing operations by mocking the load and extract method
    .. seealso:: :class:`SparkETLTests`

    """
    # Given
    with patch('jobs.pipeline.load') as mock_load:
        with patch('jobs.pipeline.extract') as mock_extract:
            mock_load.return_value = True
            mock_extract.return_value = (
                testbed.dataframes['page_views'],
                testbed.dataframes['stabsumalam_db.user_pageviews'])
            # When
            status = pipeline.run(spark=testbed.spark,
                                  config=testbed.config,
                                  logger=testbed.logger)
            # Then
            testbed.assertTrue(status)
```

These tests can be run from IDE or by simply running `pytest` command.

![img](https://cdn-images-1.medium.com/max/1600/1*kv4Tt1RM3gz6pRwq1aCczg.png)

In a complex production scenario, related pipeline methods can be connected in terms of inputs and expected outputs which is immutable. A fair understanding of application and segregation of different subject area can provide a valuable regression like confidence for CICD integration.

#### CICD

**Dockerfile —** Contains the dockerized container with the virtual environment setup for the Jenkins agent.

**Makefile —**  This Makefile utility zips all the code, dependencies, and config in the packages.zip file so that Jenkins can create the artifact, and CD process can upload it into a repository. The final code can be submitted as below

```
$SPARK_HOME/bin/spark-submit \
--py-files packages.zip \
--files configs/config.json \
dependencies/job_submitter.py --job pipeline --conf-file configs/config.json
```

**Jenkinsfile —** It defines the CICD process. where the Jenkins agent runs the docker container defined in the Dockerfile. in the prepare step followed by running the test. Once the test is successful in the prepare artifact step it uses the makefile to create a zipped artifact. The final step is to publish the artifact which is the deployment step.

All you need to have a Jenkins setup where you define a pipeline project and point to the Jenkins file.

![img](https://cdn-images-1.medium.com/max/1600/1*f6xGehlszZW__F3o9IF30w.png)

#### References

[**Best Practices for PySpark ETL Projects**
*I have often lent heavily on Apache Spark and the SparkSQL APIs for operationalising any type of batch data-processing…*alexioannides.com](https://alexioannides.com/2019/07/28/best-practices-for-pyspark-etl-projects/)

[**Best Practices Writing Production-Grade PySpark Jobs**
*How to Structure Your PySpark Job Repository and Code*developerzen.com](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f)

[**Data’s Inferno: 7 Circles of Data Testing Hell with Airflow**
*Why data testing is hard, but you should really do it!*medium.com](https://medium.com/wbaa/datas-inferno-7-circles-of-data-testing-hell-with-airflow-cef4adff58d8)https://medium.com/wbaa/datas-inferno-7-circles-of-data-testing-hell-with-airflow-cef4adff58d8)
