"""
utils
~~~~~~~
This module contains utils defined for reading spark configuration
from Spark Conf file and returning the object back to spark main application code.
"""


import configparser

from pyspark import SparkConf


def get_spark_app_config() -> object:
    """

    :rtype: object
    """
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config_file_path = r"C:\Users\Siddharth\PycharmProjects\IHS_Markit_CaseStudy\spark.conf"
    config.read(config_file_path)

    for (key, value) in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, value)
    return spark_conf
