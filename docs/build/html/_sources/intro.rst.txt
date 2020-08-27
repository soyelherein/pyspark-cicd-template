Introduction
============
This blog discusses about developing, testing and deploying data pipelines solely on our local workstation.
As a data engineer, on daily basis we face the challenge of not being able to test the functionality of our code.
Unless performing sort of UAT on actual production like data in some UAT/DEV/PreProd environment.
This project discusses on a template for data pipelines using Apache Spark and its Python(`PySpark`) APIs.
We will mainly focus on a testing framework and CICD using a small sample of production like data stored in csv files.
I call this approach as Testbed, which can be further leveraged to a small regression to have the overall confidence on the application before it is deployed.


But, before we deep dive, We need to touch upon the modularising of our application to testable units and avoid side effects.
This project covers the following topics:

* Structuring ETL application.
* Testbed Setup.
* Unittesting.
* CICD.


Structuring ETL application
***************************

Let's consider, we have a pipeline that consume files containing pageviews data and merge it into a final table.

.. literalinclude:: ../../jobs/pipeline_wo_modules.py
  :lines: 18-

The application can be submitted on spark

.. code-block::

	$SPARK_HOME/bin/spark-submit pipeline_wo_modules.py

Ok, you have already pointed out many flaws in this code, so let's address those. 
We will briefly cover this part, the idea here is taken from well documented blog  by `Dr. Alex loannides <https://alexioannides.com/2019/07/28/best-practices-for-pyspark-etl-projects/>`_ .
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
	 |	 Dockerfile
	 |	 Jenkinsfile
	 |   Makefile
	 |   Pipfile
	 |   Pipfile.lock

Handling static configurations
------------------------------

First flaw you noticed is the file paths and other static configurations are tightly coupled with the code.
Let's decouple the static configurations in a JSON file `configs/config.json`.

.. literalinclude:: ../../configs/config.json

For isolated testing we will now be able to override few of these in later section.

Handling spark environments
---------------------------
It is not practical to test and debug Spark jobs by sending them to a cluster  using spark-submit and examining stack traces for clues on what went wrong.
Our pipeline should only focus on the business transformations.
Fortunately we can use `Pypi Pyspark <https://pypi.org/project/pyspark/>`_ along with `pipenv <https://docs.pipenv.org>`_ to manage an isolated environment.
More on this on `running the example locally` section 
Let's take out the heavy lifting to a separate module.
This module can be reused for all other pipelines that follow this common structure.

:mod:`dependencies.job_submitter` takes care of the following

Besides handling the spark session, In parses the `configs/config.json` and any dynamic command line arguments then execute the requested job.
The job itself has to expose a `run` method that is covered in the below section.

.. literalinclude:: ../../dependencies/job_submitter.py
  :lines: 22-

Modularize the code
-------------------
Regardless of the complexity of a data-pipeline, this often reduces to defining a series of Extract, Transform and Load (ETL) jobs.
Now let's modularise the code in such a way that Transform is free from side effects(here IO). IO bound Extract and Load can be tested using mocks.

Below is our job structure:

* :meth:`jobs.pipeline.extract` - deals with reading the input data and return the DataFrames.
* :meth:`jobs.pipeline.transform` - deals with defining the business logic and produce the final DataFrame.
* :meth:`jobs.pipeline.load` - deals with saving the final data into the final destination.
* :meth:`jobs.pipeline.run` - acts as the entry point for the pipeline and collaborate between different parts of the pipeline.

* We have taken out the schema for the DataFrames in `ddl/schema.py` so that we can leverage the structure to construct the test data.

Our final code looks like:

.. literalinclude:: ../../jobs/pipeline.py
  :lines: 19-

Testbed Setup
*************
Given that we have structured our ETL jobs in testable modules.
We can now test the IO bound Extract and Load using mock.
In our idempotent Transform function we will feed a small slice(maybe few hundreds would be enough to test the code) of ‘real-world’ production data, locally stored in csv. Then check it against expected results.
We will be using `pytest <https://docs.pytest.org/en/stable/>`_ style tests for our pipeline,
under the hood we will also leverage few features (i.e. mock) form `unittest <https://docs.python.org/3/library/unittest.html>`_

All the setup and helper functions ate in :mod:`tests.conftest` so that the developer can focus on the testing and need not worry about creating dataframes/test spark session/mocks etc etc.
Let's look into the different functionality

The 1st function of it is to start a SparkSession locally for testing.

.. literalinclude:: ../../tests/conftest.py
  :lines: 72-83

Since the I/O operations are already been separated out we can introspect their calling behaviour using mocks.
These mocks are setup in :meth:`tests.conftest.SparkETLTests.setup_mocks`

.. literalinclude:: ../../tests/conftest.py
  :pyobject: SparkETLTests.setup_mocks

And the code is tested using below block

.. literalinclude:: ../../tests/test_pipeline.py
   :lines: 41-65

As you notice we have made available the a pytest fixture named `testbed` which stores all the required attributes.I have used the generic `read <https://spark.apache.org/docs/2.3.0/api/python/_modules/pyspark/sql/readwriter.html#DataFrameReader.load>`_ and `write <https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/readwriter.html#DataFrameWriter.save>`_ module of spark for these mocks to work.

Now for Transform method we need two dataframes as input/
It has an utility method :meth:`tests.conftest.SparkETLTests.setup_testbed` that reads the `Testbed` configurations to create these dataframes as well as the expected dataframe to compare.

.. literalinclude:: ../../tests/conftest.py
  :lines: 85-130

The actual code needs to be written to evaluate the behaviour of our transform method.

.. literalinclude:: ../../tests/test_pipeline.py
  :pyobject: test_pipeline_transform

For DataFrame comparision we have another helper function :meth:`tests.conftest.SparkETLTests.comapare_dataframes`

.. literalinclude:: ../../tests/conftest.py
  :pyobject: SparkETLTests.comapare_dataframes


Now, let's look into the integration testing, We are now able to test out pipeline by mocking the return value of the I/O operations.

.. literalinclude:: ../../tests/test_pipeline.py
  :pyobject: test_run_integration

The idea is to use immutable test files for performing the whole validation. Methods can be connected in terms of input and expected output, across different upstream and downstream modules.
A proper regression can be leveraged by using this approach of immutable test data. In this case I demonstrated a single pipeline, However we can have many pipelines inside jobs folder all connected in terms of input and expected outputs.
Which we will now implement a CICD on this pipeline in later section.

Running the example locally
---------------------------
We use `pipenv <https://docs.pipenv.org>`_ for managing project dependencies and Python environments (i.e. virtual environments).
All development and production dependencies are described in the `Pipfile`

.. code-block:: console

	pip install pipenv

Additionally, you can have `pyenv <https://github.com/pyenv/pyenv>`_ to have the desired python enviroment.

To execute the example unit test for this project run

.. code-block:: console

	pipenv run python -m unittest tests/test_*.py

Building Artifact
------------------

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

CICD
****
This section is quite straightforward, We will use a docker based agent for CICD. the image will be build from the DockerFile present inside the project folder.

.. literalinclude:: ../../Dockerfile

The testing step is same as simple as running `pytest`. Once the testing is successful we are uploading our artifact zip to S3.

.. literalinclude:: ../../Jenkinsfile

That's all for this blog, Thank you.