Introduction
============
There are a few good Blog about modularising, packaging and structuring the data pipelines for Spark jobs.
However, the testing part is often neglected or covered from very top level.
Data application testing is different and a pipeline change in the logic is always prone to breaking the logic somewhere else.

This project discusses on a template for data pipeline project using Apache Spark and its Python(`PySpark`) APIs with special focus on data-pipeline testing.
But, before we deep dive, We need to touch upon how we can structure our code to use of the approach outlined here.
This project covers the following topics:

* Structuring ETL codes into testable modules.
* Setting up configurations and test data(Testbed).
* Boilerplate pytest style testcases for PySpark jobs.
* Packaging and submitting jobs in the cluster.


Initial Pyspark code
*********************

Let's start with a poorly constructed  Pyspark pipeline. We will apply the structure in it one step at a time. 
We will go over the codes so that at the end, all the pieces will make sense how this approach can help us to build a TDD data-pipeline.

Let's consider, we have a pipeline that consume files containing pageviews data and merge it into a final table.

.. literalinclude:: ../../jobs/pipeline_wo_modules.py
  :lines: 18-

The application can be submitted on spark

.. code-block::

	$SPARK_HOME/bin/spark-submit pipeline_wo_modules.py

Let's now look into modularising the application.

Handling static configurations
******************************
If we look closely to the above code, the file paths and other static configurations are tightly coupled with the code.
For local execution we want to execute the code in isolation and we will avoid the side effects that can occur from I/O.

Let's decouple the static configurations as a JSON file `configs/config.json`.

.. literalinclude:: ../../configs/config.json


Handling spark environments
***************************
It is not practical to test and debug Spark jobs by sending them to a cluster  using spark-submit and examining stack traces for clues on what went wrong.
Fortunately we have `Pypi Pyspark <https://pypi.org/project/pyspark/>`_ locally on `pipenv <https://docs.pipenv.org>`_

Our pipeline should only focus on the business transformations. Let's take out the auxiliary heavy lifting to a separate module.
This module can be reused for all other pipelines that follow a common structure as suggested in this project.

:mod:`dependencies.job_submitter` takes care of the following

* Handles the creation of spark environment.
* Passes static job configuration parameters from `configs/config.json` to the job.
* Parses command line arguments to accept dynamic inputs and pass it to the job.
* Dynamically loads the requested job module and runs it.

The job itself has to expose a `run` method.

.. literalinclude:: ../../dependencies/job_submitter.py
  :lines: 22-

Modularize the code
*******************
Regardless of the complexity of a data-pipeline, this often reduces to defining a series of Extract, Transform and Load (ETL) jobs.

So, 1st step to test the application is to modularize to address the below.

* Segregate the logic into testable modules.
* Separating out the side effects of reading and writing the data.

Below is our pipeline structure:

* :meth:`jobs.pipeline.extract` - deals with reading the input data and return the DataFrames.
* :meth:`jobs.pipeline.transform` - deals with defining the business logic and produce the final DataFrame.
* :meth:`jobs.pipeline.load` - deals with saving the final data into the final destination.
* :meth:`jobs.pipeline.run` - acts as the entry point for the pipeline and collaborate between different parts of the pipeline.
* We have taken out the schema for the DataFrames in `ddl/schema.py`

There is a really good blog  by `Dr. Alex loannides <https://alexioannides.com/2019/07/28/best-practices-for-pyspark-etl-projects/>`_ 
and `Eran Campf <https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f#.wg3iv4kie>`_ about structuring ETL projects.
Here We have a single module pipeline here with just singleton Extract, Transform and Load methods.
Our overall project structure would look like.

.. code-block:: console

	root/
	 |-- configs/
	 |   |-- config.json
	 |-- dependencies/
	 |   |-- job_submitter.py
	 |-- ddl/
	 |	 |-- schema.py
	 |-- jobs/
	 |   |-- pipeline.py
	 |-- tests/
	 |   |-- test_data/
	 |   |-- | -- employees/
	 |   |-- | -- employees_report/
	 |   |-- conftest.py
	 |   |-- test_bed.json
	 |   |-- test_pipeline.py
	 |   Makefile
	 |   Pipfile
	 |   Pipfile.lock


Our final code looks like:

.. literalinclude:: ../../jobs/pipeline.py
  :lines: 19-

Testing setup
*************
Given that we have structured our ETL jobs in testable modules.
We can feed it a small slice of ‘real-world’ production data that has been persisted locally(`tests/test_data`) and check it against expected results.
We will be using `pytest <https://docs.pytest.org/en/stable/>`_ style tests for our pipeline,
under the hood we will also leverage few features (i.e. mock) form `unittest <https://docs.python.org/3/library/unittest.html>`_

Let's look into the different functionality of our :mod:`tests.conftest`

The 1st function of it is to start a SparkSession locally for testing.

.. literalinclude:: ../../tests/conftest.py
  :lines: 72-83

We have an utility method :meth:`tests.conftest.SparkETLTests.setup_testbed` that reads the `Testbed` configurations to create the Dataframes in order to test out transform function.

.. literalinclude:: ../../tests/conftest.py
  :lines: 85-130

Let's now have a look into our testing code for the Transform method.

.. literalinclude:: ../../tests/test_pipeline.py
  :pyobject: test_pipeline_transform

As you can see we have made available the a pytest fixture named `testbed`. This object stores the DataFrames and configs for testing in it's member variables.
We are passing the DataFrames created out of the test files and matching the output DataFrame using another helper function :meth:`tests.conftest.SparkETLTests.comapare_dataframes`

.. literalinclude:: ../../tests/conftest.py
  :pyobject: SparkETLTests.comapare_dataframes

Since the I/O operations are already been separated out we can introspect their calling behaviour using mocks.
These mocks are setup in :meth:`tests.conftest.SparkETLTests.setup_mocks`

.. literalinclude:: ../../tests/conftest.py
  :pyobject: SparkETLTests.setup_mocks

And the code is tested using below block

.. literalinclude:: ../../tests/test_pipeline.py
   :lines: 41-65

I have used the generic `read <https://spark.apache.org/docs/2.3.0/api/python/_modules/pyspark/sql/readwriter.html#DataFrameReader.load>`_ and `write <https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/readwriter.html#DataFrameWriter.save>`_ module of spark for these mocks to work.

Now, let's look into the integration testing, We are now able to test out pipeline by mocking the return value of the I/O operations.

.. literalinclude:: ../../tests/test_pipeline.py
  :pyobject: test_run_integration

The idea is to use immutable test files for performing the whole validation. Methods can be connected in terms of input and expected output, across different upstream and downstream modules.
A proper regression can be leveraged by using this approach of immutable test data and plugged into a CICD deployment.

Running the example locally
***************************
We use `pipenv <https://docs.pipenv.org>`_ for managing project dependencies and Python environments (i.e. virtual environments).
All development and production dependencies are described in the `Pipfile`

.. code-block:: console

	pip install pipenv

Additionally, you can have `pyenv <https://github.com/pyenv/pyenv>`_ to have the desired python enviroment.

To execute the example unit test for this project run

.. code-block:: console

	pipenv run python -m unittest tests/test_*.py

Deploying into production
*************************

The project has a build-in Makefile utility to create zipped dependency and configs and bundle them together

.. code-block:: console

	make clean
	make build

Now you can run the pipeline using below command

.. code-block:: console

	$SPARK_HOME/bin/spark-submit \
	--py-files packages.zip \
	--files configs/config.json \
	dependencies/job_submitter.py --job pipeline --conf-file configs/config.json