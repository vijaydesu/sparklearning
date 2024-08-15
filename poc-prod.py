
import pyspark;
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from os.path import abspath
from pyspark.sql import Row
# Using col() function
from pyspark.sql.functions import col
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
#--Display Fields Max---
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
print("Hello..."+cwd)


# Read JSON file into dataframe    
df = spark.read.json("resources/snapshot/OrderData_newFormat.json")
#df.printSchema()
print("No. of records : "+ "{}".format(df.count()))

df1=df.drop("paymentInfo","payments","entries","visibleEntries","deliveryPointOfService","appliedProductPromotions")
#df.printSchema()

#df.show(1)
df2=df.select("code","net","totalPriceWithTax.value",col("totalPrice.value").alias("totalPrice"),"totalTax.value","subTotal.value","totalItems","user.uid","user.name","channelType","deliveryPointOfService.code","contactInfo.firstName","contactInfo.lastName","contactInfo.phoneNumber","customerMasterNumber","paymentType","status","guestCustomer","placedBy","cartId","carryOutType","promiseType","created")
df2.show(vertical=True)
#df.show(1,truncate=False,vertical=True)


