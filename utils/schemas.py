"""
schema
~~~~~~~
This module contains schema defined for different dataframes
which are loaded through the spark application.
"""

from pyspark.sql.types import *
from pyspark.sql.types import StructType

schema = StructType([(StructField("Part Number", StringType())),
                     (StructField("Tolerance", StringType())),
                     (StructField("MOQ", FloatType())),
                     (StructField("Box Qty", FloatType())),
                     (StructField("Level", IntegerType())),
                     (StructField("Dry Pack", StringType())),
                     (StructField("COUNTRY OF ORIGIN", StringType())),
                     (StructField("Status", StringType()))])

country_schema = StructType([(StructField("Country", StringType())),
                             (StructField("Country Name", StringType()))
                             ])

status_schema: StructType = StructType([(StructField("Status", StringType())),
                                        (StructField("Full Status", StringType()))
                                        ])
