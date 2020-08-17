# Copyright 2020 soyel.alam@ucdconnect.ie
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from typing import Dict, Tuple, Any
import json
from pyspark.sql import SparkSession
import importlib


def create_spark_session(job_name: str):
    """Create spark session to run the job

    :param job_name: job name
    :type job_name: str
    :return: spark and logger
    :rtype: Tuple[SparkSession,Log4j]
    """
    spark: SparkSession = SparkSession.builder.appName(job_name).enableHiveSupport().getOrCreate()
    app_id: str = spark.conf.get('spark.app.id')
    log4j = spark._jvm.org.apache.log4j
    message_prefix = '<' + job_name + ' ' + app_id + '>'
    logger = log4j.LogManager.getLogger(message_prefix)
    return spark, logger


def load_config_file(file_name: str) -> Dict:
    """
    Reads the configs/config.json file and parse as a dictionary

    :param file_name: name of the config file
    :return: config dictionary
    """
    try:
        with open(f'{file_name}') as f:
            conf: Dict = json.load(f)
        return conf

    except FileNotFoundError:
        raise FileNotFoundError(f'{file_name} Not found')


def parse_job_args(job_args: str) -> Dict:
    """
    Reads the additional job_args and parse as a dictionary

    :param job_args: extra job_args i.e. k1=v1 k2=v2
    :return: config dictionary
    """
    return {a.split('=')[0]: a.split('=')[1] for a in job_args}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Job submitter',
                                     usage='--job job_name, --conf-file config_file_name, --job-args k1=v1 k2=v2')
    parser.add_argument('--job', help='job name', dest='job_name', required=True)
    parser.add_argument('--conf-file', help='Config file path', required=False)
    parser.add_argument('--job-args',
                        help='Additional job arguments, It would be made part of config dict',
                        required=False,
                        nargs='*')
    args = parser.parse_args()
    job_name = args.job_name
    spark, logger = create_spark_session(job_name)
    config_file = args.conf_file if args.conf_file else 'configs/config.json'
    config_dict: Dict = load_config_file(config_file)
    if args.job_args:
        job_args = parse_job_args(args.job_args)
        config_dict.update(job_args)
    logger.warn(f'calling job {args.job_name}  with {config_dict}')
    job = importlib.import_module(f'jobs.{job_name}')
    job.run(spark=spark, config=config_dict, logger=logger)
    spark.stop()
