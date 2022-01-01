from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, broadcast, count, desc, regexp_replace
from utils.utils import get_spark_app_config
from utils.logger import logger
from utils import schemas
from jproperties import Properties
import pandas as pd
import sys
import os
import re as r

os.environ['SPARK_HOME'] = "E://spark-3.0.3-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "C://Program Files//Java//jdk1.8.0_311"
os.environ['HADOOP_HOME'] = "C://Hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class CaseAnalysis:
    def run(self):
        conf = get_spark_app_config()

        '''Intializing spark application with configurations
        read from spark.conf file.'''

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        logger.info("Starting Spark Application")

        conf_out = spark.sparkContext.getConf()
        logger.info(conf_out.toDebugString())

        '''Assigning property values from
        reading from requirement.properties file'''

        configs = Properties()
        properties_file_path = r"C:\Users\Siddharth\PycharmProjects\IHS_Markit_CaseStudy\requirement.properties"
        with open(properties_file_path, 'rb') as read_prop:
            configs.load(read_prop)
        repartition_value = int(configs.get("partition_value").data)
        input_dir = configs.get("INPUT_FILE_DIR").data
        output_dir = configs.get("OUTPUT_FILE_DIR").data

        '''reading 4 different xlxs/csv files into corressponding
        dataframes and performing transformations.'''

        file1 = input_dir +'/' +'File1.xlsx'
        pdDF1 = pd.read_excel(file1)

        schema=schemas.schema

        logger.info("File1.xlsx is loading into dataframe")

        df1 = spark.createDataFrame(pdDF1,schema)

        df1_filtered_parts = df1.filter(col("Part Number").isNotNull())
        #df1.show(10,truncate=False)
        logger.info('File1.xlsx has been loaded with count' )
        logger.info(df1.count())

        file2 = input_dir + '/' + 'File2.xlsx'
        pdDF2 = pd.read_excel(file2)

        logger.info("File2.xlsx is loading into dataframe")

        df2 = spark.createDataFrame(pdDF2,schema)

        logger.info("File2.xlsx has been loaded with count")
        logger.info(df2.count())

        country_schema = schemas.country_schema
        country_file = input_dir + '/' + 'country.csv'
        country_df = spark.read.format("csv").option("header","true").schema(country_schema)\
        .load(country_file)
        #country_df.show(truncate=False)


        status_schema = schemas.status_schema
        status_file = input_dir + '/' + 'status.csv'
        status_df = spark.read.format("csv").option("header","true").option("delimiter", "~").schema(status_schema)\
        .load(status_file)

        df2_filtered_parts = df2.filter(col("Part Number").isNotNull())

        #df2.show(10,truncate=False)

        merge_df = df1_filtered_parts.union(df2_filtered_parts)

        logger.info("Merging dataframes with clean data")

        merge_clean_df = merge_df.withColumn("Status", regexp_replace(merge_df.Status, "[^A-Z_]", ""))

        '''merge_clean_df.filter(col("Part Number").isin(["AS7C164A-15JCNTR", "AS7C31025B-12TJINTR",
                                               "AS7C1026C-15JIN" , "AS7C32098A-10TIN"]))\
                                                .show(truncate=False)'''


        # merge_df.show(500,truncate=False)
        # print(merge_df.count())

        logger.info("Dropping duplicates on basis of part numbers from merged dataframe")

        merge_df_unique_parts = merge_clean_df.drop_duplicates(subset=["Part Number"])
        #merge_df_unique_parts.show(500,truncate=False)
        #print(merge_df_unique_parts.count())

        status_join_expr = merge_df_unique_parts["Status"] == status_df["Status"]
        country_join_expr = merge_df_unique_parts["COUNTRY OF ORIGIN"] == country_df["Country"]
        join_type = "inner"

        logger.info("Joining the dataframes country, status and merged dataframe with clean data and unique parts")

        '''Joining the dataframes created above and broadcasting dataframes
        status_df and country_df since they are considerably small files as compared to
        merge_df_unique_parts. The code will be scalable when data volume increases.'''

        total_details_df = merge_df_unique_parts.join(broadcast(status_df),status_join_expr, join_type)\
            .join(broadcast(country_df),country_join_expr, join_type)\
            .select(merge_df_unique_parts["Part Number"], merge_df_unique_parts["Tolerance"], merge_df_unique_parts["MOQ"]
            , merge_df_unique_parts["Box Qty"] ,merge_df_unique_parts["Level"] ,merge_df_unique_parts["Dry Pack"]
            , country_df["Country Name"], status_df["Full Status"])

        if total_details_df:
            try:
                logger.info("Printing the output to console")
                total_details_df.show(500,truncate=False)
            except Exception as err:
                logger.error("%s, Error: %s", str(__name__), str(err))

            logger.info("Writing the joined dataframe with country and full status details to parquet file format under output directory.")
            output_file = output_dir + '/' + 'output.parquet'
            try:
                total_details_df.select(col("Part Number").alias("Part_Number"), col("Tolerance"), col("MOQ"),
                            col("Box Qty").alias("Box_Qty"),col("Level"),
                            col("Dry Pack").alias("Dry_Pack"),col("Country Name").alias("Country_Name"),
                            col("Full Status").alias("Full_Status") )\
                            .repartition(repartition_value).write.mode("overwrite")\
                            .parquet(output_file)
                logger.info("writing to location!")

            except Exception as err:
                logger.error("%s, Error: %s", str(__name__), str(err))

            DIR_PATH = output_file

            if os.path.exists(DIR_PATH):
                files = os.listdir(DIR_PATH)
                if [f for f in files if f.endswith(".parquet")]:
                    logger.info("total_details_df files have been written successfully under output folder.")
                else:
                    logger.error("total_details_df files are not written. Please investigate! ")
            else:
                logger.info("Wrong Path given!")
        else:
            logger.error("Please investigate joining conditions.")


        status_parts_df = total_details_df.groupBy(col("Full Status").alias("Full_Status")).agg(count("Part Number")
                                                              .alias("Part_Count")).orderBy(col("Part_Count"))
        if status_parts_df:
            try:
                logger.info("Printing the output to console")
                status_parts_df.show(truncate=False)
            except Exception as err:
                logger.error("%s, Error: %s", str(__name__), str(err))

            logger.info("Writing the status wise aggregation dataframe to parquet file format under output directory.")
            status_by_parts_file = output_dir + '/' + 'statusbycount.parquet'
            try:
                status_parts_df.repartition(repartition_value).write.mode("overwrite")\
                    .parquet(status_by_parts_file)
                logger.info("writing to location!")

            except Exception as err:
                logger.error("%s, Error: %s", str(__name__), str(err))

            DIR_PATH = status_by_parts_file

            if os.path.exists(DIR_PATH):
                files = os.listdir(DIR_PATH)
                if [f for f in files if f.endswith(".parquet")]:
                    logger.info("status_parts_df files have been written successfully under output folder.")
                else:
                    logger.error("status_parts_df files are not written. Please investigate! ")
            else:
                logger.info("Wrong Path given!")
        else:
            logger.error("Please investigate your aggregates.")


        country_parts_df = total_details_df.groupBy(col("Country Name").alias("Country_Name")).agg(count("Part Number")
                                                              .alias("Part_Count")).orderBy(col("Part_Count"))
        if country_parts_df:
            try:
                logger.info("Printing the output to console")
                country_parts_df.show(truncate=False)
            except Exception as err:
                logger.error("%s, Error: %s", str(__name__), str(err))

            logger.info("Writing the country wise aggregation dataframe to parquet file format under output directory.")
            country_by_parts_file = output_dir + '/' + 'countrybycount.parquet'

            try:
                country_parts_df.repartition(repartition_value).write.mode("overwrite")\
                .parquet(country_by_parts_file)
                logger.info("writing to location!")

            except Exception as err:
                logger.error("%s, Error: %s", str(__name__), str(err))

            DIR_PATH = country_by_parts_file

            if os.path.exists(DIR_PATH):
                files = os.listdir(DIR_PATH)
                if [f for f in files if f.endswith(".parquet")]:
                    logger.info("country_parts_df files have been written successfully under output folder.")
                else:
                    logger.error("country_parts_df files are not written. Please investigate! ")
            else:
                logger.info("Wrong Path given!")
        else:
            logger.error("Please investigate your aggregates.")


        logger.info("Ending Spark Application")
        spark.stop()

if __name__ == '__main__':
    CaseAnalysis().run()