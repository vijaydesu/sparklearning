import pyspark;
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from os.path import abspath
from pyspark.sql import Row
import os
# define the schema for the JSON data
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,DateType

cwd = os.getcwd()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

spark = SparkSession.builder \
    .master("local[1]") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport()\
    .appName("SparkByExamples.com") \
    .getOrCreate() 

print("Hello..."+cwd)

data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
df = spark.createDataFrame(data)
df.show()

# Read JSON file into dataframe    
df = spark.read.json("resources/poc-order-multi.json")
df.printSchema()
df.show()

#Transform and write to Parquet File
df.select("orderId", "orderDate","total").write.mode('overwrite').save("orderpaq.parquet", format="parquet")
dfpaq = spark.read.parquet("orderpaq.parquet")
dfpaq.printSchema()
dfpaq.show()
#-------

#--- Temporary View IN MEMORY---
df.createOrReplaceTempView("eOrders")
#----

#--- PERMANENT TABLE - HIVE STORE--- 
df.write.mode('overwrite').saveAsTable("Orders")
#----------


query_df = spark.sql("SELECT * FROM eOrders WHERE shipping='USPS'")
query_df.show()

query_df1 = spark.sql("SELECT * FROM Orders WHERE shipping='FEDEX'")
query_df1.show()

# spark is an existing SparkSession
spark.sql("CREATE TABLE IF NOT EXISTS jsonorders (customerName STRING, orderId STRING) USING hive")
spark.sql("LOAD DATA LOCAL INPATH 'resources/poc-order-multi.json' INTO TABLE jsonorders")

query_df2 = spark.sql("SELECT * FROM jsonorders")
query_df2.show()

# Multiple JSON Files--
# --- https://medium.com/@uzzaman.ahmed/introduction-to-pyspark-json-api-read-and-write-with-parameters-3cca3490e448 ---




addressSchema = StructType([
    StructField("city", StringType(), True),
    StructField("line1", StringType(), True),
    StructField("line2", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True)
])

jsonSchema = StructType([
    StructField("customerName", StringType(), True),
    StructField("orderDate", DateType(), True),
    StructField("orderId", StringType(), True),
    StructField("shipping", StringType(), True),
    StructField("store", StringType(), True),
    StructField("tax", DoubleType(), True),
    StructField("total", DoubleType(), True),
    StructField("address", StructType(addressSchema), True)

])

# read the JSON file with parameters
dfMulti = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "corrupt_record") \
    .option("dateFormat", "MM/dd/yyyy") \
    .schema(jsonSchema) \
    .json("resources/multi")

# show the DataFrame
dfMulti.show()
dfMulti.printSchema()

#--- PERMANENT TABLE - HIVE STORE--- 
dfMulti.write.mode('overwrite').saveAsTable("MultiOrders")

dfMulti = dfMulti.select("orderId", "orderDate","total","address.city","address.zipcode")
dfMulti.show()

# --- SQL Reference - https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html
#--- https://spark.apache.org/docs/latest/sql-data-sources.html
query_df3 = spark.sql("SHOW TABLE EXTENDED LIKE 'MultiOrders'")
query_df3.show(truncate=False)
#---------

query_df3 = spark.sql("SELECT * FROM MultiOrders WHERE address.zipcode ='75068'")
query_df3.show(truncate=False)

query_df4 = spark.sql("SELECT shipping,count(*) FROM MultiOrders GROUP BY shipping")
query_df4.show(truncate=False)

#----------

# Create a table from Parquet File
#spark.sql("CREATE OR REPLACE VIEW eOrders using json OPTIONS" + 
#      " (path 'resources/poc-order.json')")
#spark.sql("select * from eOrders").show()

# export SPARK_HOME=/usr/local/Cellar/apache-spark/3.5.0
# export PYTHONPATH=$SPARK_HOME/libexec/python:$SPARK_HOME/libexec/python/build:$PYTHONPATH