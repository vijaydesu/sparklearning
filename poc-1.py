import pyspark;
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from os.path import abspath
from pyspark.sql import Row
import os
# define the schema for the JSON data
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType

cwd = os.getcwd()
#Tools to access databricks tables - https://docs.databricks.com/en/dev-tools/index-sql.html
# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession.builder \
    .master("local[1]") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport()\
    .appName("SparkByExamples1.com") \
    .getOrCreate() 

print("Hello..."+cwd)



query_df3 = spark.sql("SELECT * FROM MultiOrders WHERE address.zipcode ='75068'")
query_df3.show(truncate=False)

query_df4 = spark.sql("SELECT shipping,count(*) FROM MultiOrders GROUP BY shipping")
query_df4.show(truncate=False)

query_df5 = spark.sql("SHOW TABLE EXTENDED LIKE 'MultiOrders'")
query_df5.show(truncate=False,vertical=True)

query_df6 = spark.sql("SHOW CREATE TABLE MultiOrders")
query_df6.show(truncate=False)

#----------

# Create a table from Parquet File
#spark.sql("CREATE OR REPLACE VIEW eOrders using json OPTIONS" + 
#      " (path 'resources/poc-order.json')")
#spark.sql("select * from eOrders").show()

# export SPARK_HOME=/usr/local/Cellar/apache-spark/3.5.0
# export PYTHONPATH=$SPARK_HOME/libexec/python:$SPARK_HOME/libexec/python/build:$PYTHONPATH