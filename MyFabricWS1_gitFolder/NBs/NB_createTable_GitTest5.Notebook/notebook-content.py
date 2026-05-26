# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5c3a5650-5485-4b19-beb2-3661c144c400",
# META       "default_lakehouse_name": "LakehouseFab",
# META       "default_lakehouse_workspace_id": "9e63bea0-77f2-4cd2-bc30-749d31851c0c",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c3a5650-5485-4b19-beb2-3661c144c400"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#git test Branch 5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dest_ws_id = "9e63bea0-77f2-4cd2-bc30-749d31851c0c"
lakehouse_id = "5c3a5650-5485-4b19-beb2-3661c144c400"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_to_lh = f"abfss://{dest_ws_id}@msit-onelake.dfs.fabric.microsoft.com/{lakehouse_id}/"
print(f"current lakehouse path: {path_to_lh}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rpt_dest = path_to_lh + "Tables/dbo/DimCopyProd"
print(f"current lakehouse path: {rpt_dest}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(f"""select * from dbo.DimProduct limit 3""")
#.createOrReplaceTempView("vwProd")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#rpt_dest = "abfss://9e63bea0-77f2-4cd2-bc30-749d31851c0c@msit-onelake.dfs.fabric.microsoft.com/5c3a5650-5485-4b19-beb2-3661c144c400/Tables/dbo/DimCopyProd"
#df = spark.sql(f"""select * from dbo.DimProduct limit 3""")
df.write.format("delta").mode("overwrite").save(rpt_dest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LakehouseFab.dbo.DimCustomer LIMIT 1000")
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
df_agg=df["Title", "CustomerKey"].groupBy("Title").count().where(F.col("Title").isin("Mr.", "Sr.")) \
.withColumnRenamed("count", "CustCount")
df_agg.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl = df.withColumn("CutNew", left(F.col("FirstName"), 2))
#.withColumn("CityCut", F.col("city").substr(1,3))
df_cl.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
df_agg=df["Title", "CustomerKey"].groupBy("Title").count().where((F.col("Title") == "Mr.") | (F.col("Title") ==  "Sr.")) \
.withColumnRenamed("count", "CustCount")
df_agg.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

#df_select = df_pdt.select("ProductID", "ProductName")
df_select2 = df_pdt["ProductID", "ProductName","Group", "ListPrice"].where((df_pdt["Group"]=="A") | (df_pdt["ListPrice"]>2000))
df_count = df_pdt["ProductID","Group"].groupBy("Group").count()
df_agg = df_pdt.groupBy("Group").agg(F.sum(F.col("ListPrice").cast("float")).alias("TotalPrice"))
display(df_agg.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pdt.write.format("delta").saveAsTable("ProductTst")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #opp mentenanc test practice

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# Sample schema
schema = StructType([
StructField("SchemaName", StringType(), True),
StructField("TableName", StringType(), True),
StructField("IsActive", BooleanType(), True),
StructField("WorkspaceName", StringType(), True),
StructField("LakehouseName", StringType(), True),
StructField("RefreshFrequency", StringType(), True),
StructField("NoOfDays", IntegerType(), True)
])

# Sample data
data = [
    ("dbo", "DimProduct", True,"MyFabricWS1", "LakehouseFab", "Daily", 1),
     ("dbo", "DimCustomer", True,"MyFabricWS1", "LakehouseFab", "Daily", 1)
]

df = spark.createDataFrame(data, schema)

# Write to table
df.write.mode("overwrite").saveAsTable("LakehouseFab.dbo.DimLHTblConfig")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from LakehouseFab.dbo.DimLHTblConfig

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, datetime
from delta.tables import DeltaTable
import os
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_datetime(d: date):
    return datetime.combine(d, datetime.min.time())


spark = SparkSession.builder.getOrCreate()

# Sample schema
schema2 = StructType([
StructField("SchemaName", StringType(), True),
StructField("TableName", StringType(), True),
StructField("WorkspaceName", StringType(), True),
StructField("LakehouseName", StringType(), True),
StructField("RecordCount", LongType(), True),
StructField("MaxRecordModifiedDate", TimestampType(), True),
StructField("RefreshFrequency", StringType(), True),
StructField("LastRefreshDate", TimestampType(), True),
StructField("NextRefreshDate", TimestampType(), True),
StructField("NoOfDays", IntegerType(), True),
StructField("IsStale", IntegerType(), True),
StructField("StaleNoOfDays", IntegerType(), True),
StructField("IsActive", BooleanType(), True),
StructField("SakeholderAlias", StringType(), True),
StructField("AlertMailSendFlag", IntegerType(), True),
StructField("RecordModifiedDate", TimestampType(), True),
StructField("PrevCount", IntegerType(), True),
StructField("CountVariancePerc", IntegerType(), True)
])

# Sample data
data = [
    ("dbo", "DimProduct", "MyFabricWS1", "LakehouseFab", 10,
     to_datetime(date.today()), "", to_datetime(date.today()), to_datetime(date.today()),
     0, 1, -1, True, "", 1, to_datetime(date.today()), 10, 0),

    ("dbo", "DimCustomer", "MyFabricWS1", "LakehouseFab", 5,
     to_datetime(date.today()), "", to_datetime(date.today()), to_datetime(date.today()),
     0, 1, -1, True, "", 1, to_datetime(date.today()), 10, 0)
]

df2 = spark.createDataFrame(data, schema2)

# Write to table
df2.write.mode("overwrite").saveAsTable("LakehouseFab.dbo.DimLHTblConfigExcp")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_df = spark.read.format("delta").load("abfss://MyFabricWS1@onelake.dfs.fabric.microsoft.com/LakehouseFab.Lakehouse/Tables/dbo/dimlhtblconfig")
xception_df = spark.read.format("delta").load("abfss://MyFabricWS1@onelake.dfs.fabric.microsoft.com/LakehouseFab.Lakehouse/Tables/dbo/dimlhtblconfigexcp")

result_df = config_df.join(xception_df, on=["TableName", "SchemaName"],
how="left_anti"
)
result_df.filter(F.col("IsActive")==True).createOrReplaceTempView("vw_LHTblConfig")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RESULT SCHEMA
result_schema = StructType([
StructField("WorkspaceName", StringType(), True),
StructField("LakehouseName", StringType(), True),
StructField("SchemaName", StringType(), True),
StructField("TableName", StringType(), True),
StructField("RecordCount", LongType(), True),
StructField("MaxRecordModifiedDate", TimestampType(), True),
StructField("RefreshFrequency", StringType(), True),
StructField("LastRefreshDate", TimestampType(), True),
StructField("NextRefreshDate", TimestampType(), True),
StructField("NoOfDays", IntegerType(), True),
StructField("IsStale", BooleanType(), True),
StructField("StaleNoOfDays", IntegerType(), True),
StructField("IsActive", BooleanType(), True),
StructField("StakeholderAlias", StringType(), True),
StructField("AlertMailSendFlag", BooleanType(), True)
]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def is_delta_table(spark,path_or_table):
    try:
        detail =spark.sql(f"DESCRIBE DETAIL {path_or_table}").collect()[0]
        if detail['format'].lower() == 'delta':
            return True
    except Exception:
            if os.path.exists(os.path.join(path_or_table,"_delta_log")):
                return True
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, to_date, date_trunc, countDistinct, current_timestamp, from_utc_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType, LongType
from py4j.java_gateway import java_import
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date, date_trunc, countDistinct, current_date, datediff, lag
from datetime import date
from pyspark.sql.window import Window

#-- FETCH TABLE LIST
tables = spark.sql(f"""
SELECT DISTINCT
WorkspaceName,
LakehouseName,
SchemaName,
TableName,
CASE WHEN SchemaName = 'dbo' THEN 'jojoba' ELSE NULL END AS StakeholderAlias
FROM vw_LHTableConfig
=** ).collect()

RESULT SCHEMA
result_schema = StructType([
StructField("WorkspaceName", StringType(), True),
StructField("LakehouseName", StringType(), True),
StructField("SchemaName", StringType(), True),
StructField("TableName", StringType(), True),
StructField("RecordCount", LongType(), True),
StructField("MaxRecordModifiedDate", TimestampType(), True),
StructField("RefreshFrequency", StringType(), True),
StructField("LastRefreshDate", TimestampType(), True),
StructField("NextRefreshDate", TimestampType(), True),
StructField("NoOfDays", IntegerType(), True),
StructField("IsStale", BooleanType(), True),
StructField("StaleNoOfDays", IntegerType(), True),
StructField("IsActive", BooleanType(), True),
StructField("StakeholderAlias", StringType(), True),
StructField("AlertMailSendFlag", BooleanType(), True)
]
)
results = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
