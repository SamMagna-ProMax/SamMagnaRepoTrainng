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

Git B4 test

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run Common_fn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_update_RefreshDateAuto

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_dbo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #load , threshold and custom truncate

# CELL ********************

destPath = bronze_lh + "Tables/ad/DimCopyProd_dist"
#spark.read.load(bronze_lh + "Tables/dbo/DimCopyProd").createOrReplaceTempView("vw_DimCopyProd")
#spark.read.format("delta").load(bronze_lh + "Tables/dbo/DimCopyProd").createOrReplaceTempView("vw_DimCopyProd")
spark.read.load(f"{bronze_lh}/Tables/{schema_dbo}/DimCopyProd").createOrReplaceTempView("vw_DimCopyProd2")
#spark.sql(f"select * from vw_DimCopyProd2 limit 1").createOrReplaceTempView("vw_DimCopyProd_LessData")
df = spark.sql(f"select * from vw_DimCopyProd2").withColumn("UpStreamRefreshData", lit(from_utc_timestamp(current_timestamp(),'PST'))).withColumn("SourceRefreshDetail", lit(from_utc_timestamp(current_timestamp(),'PST'))).createOrReplaceTempView("vw_DimCopyProd_LessData")
#check_threshold("vw_DimCopyProd2", destPath,10,"count")
#custom_truncate("vw_DimCopyProd2", destPath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

check_threshold("vw_DimCopyProd_LessData", destPath,1,"count")
custom_truncate("vw_DimCopyProd_LessData", destPath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create temp table or load on disk for performace optimization
df_temp_table = spark.sql("""select * from vw_DimCopyProd2""")

df_temp_table.persist(StorageLevel.DISK_ONLY)
df_temp_table.take(1)   #this is action needed to trigger persisting
df_temp_table.createOrReplaceTempView("Temp_view_DimCOpyProd2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql.types import TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_today = spark.sql("""select date_format(from_utc_timestamp(current_timestamp(),'PST'),'yyyyMMdd') as today""")
lastRefresh = df_today.collect()[0]["today"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LakehouseFab.ad.tblsourcerefresh LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#run below code at end of NB; so if the NB successfuly run it will update the Source refresh table
df_today = spark.sql("""select date_format(from_utc_timestamp(current_timestamp(),'PST'),'yyyyMMdd') as today""")
lastRefresh = df_today.collect()[0]["today"]

data_category = 'Product1'

#get the first part YYYYMMDD BEFORE DOT
last_lastRfresh_str = lastRefresh.split('.')[0]
print("last_lastRfresh_str: {last_lastRfresh_str}", last_lastRfresh_str)

#convert to datetime and set time
final_datetime = datetime.strptime(last_lastRfresh_str, "%Y%m%d").replace(hour=0, minute=0, second=0,microsecond=0)

print("final_datetime test: ", final_datetime)

#cast column
upstream_refresh_date = spark.createDataFrame([(final_datetime,)], ["UpStreamRefreshDetail"])

#convert to timestamptype
upstream_refresh_date = upstream_refresh_date.withColumn("UpStreamRefreshDetail", upstream_refresh_date["UpStreamRefreshDetail"].cast(TimestampType()))

#the 1st row from df
firstvalue = upstream_refresh_date.first()

if firstvalue:
    upstream_refresh_date_value = firstvalue['UpStreamRefreshDetail']

    print("firstvalue: ", firstvalue)

    #call source_refresh_detail function
    source_refresh_details(data_category, upstream_refresh_date_value)
else:
    print("no new data found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#run below code at end of NB; so if the NB successfuly run this query is pulling upstream source refresh for example from different LH may be direct shortcut
#get max log date as most refresh date

source_path = bronze_lh + "Table/dbo/ProductSourceDirectShortCut"
log_path = base_path + "/_delta_log"

#list delta log file which is JSON format only
logs =[f for fin mssparkutils.fs.ls(log_path) if f.name.endwith(".json")]

if logs:
    #get letest log by name
    latest_log = max(logs, key=lambada f:f.name)

    #get 1st comit information
    content = mssparkutils.fs.head(latest_log.path, 65536)

    latest_commit_time = None
    for line in content.splitlines():
        try:
            record = json.loads(line)
            if "comitInfo" in record:
                ts = record["comitInfo"].get("timestamp")
                if ts:
                    latest_comit_time = ts
                    break
        except json.JSONDecodeError:
            continue

if latest_commit_time:
    upstream.refresh_data_value = datetime.formattimesstamp)latest_comit_time / 10000)
    data_Category = 'Sales2'
    source_refresh_detial(data_category, upstream_refresh_date_value)
else:
    raise RuntimeError("No changed comit timestamp found in Detal,lets conne")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#copy table from one lakehouse to another like from Staging to Dev
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#Source Path
Stage_base ="abfss://9e63bea0-workspaceID-749d31851c0c@onelake.dfs.fabric.microsoft.com/76c05b7-LakehouseID-6c82b330/Tables/dbo/"
Dest_base = "abfss://9e63bea0-workspaceID-749d31851c0c@onelake.dfs.fabric.microsoft.com/76c05b7-LakehouseID-6c82b330/Tables/dbo/"

#Array for multiple table
tables = [
    "producttble",
    "salestble"
    #list tables
]

#copy each table using for loop
for t in tables:
    Stage_path = f"{Stage_base}"
    Dest_Path = f"{Dest_base}"
    print(f"Copying {t} from stage to dest....")

    df = spark.read.format("delta").load(Stage_path)
    df.write.format("delta").mode("overwrite").save(Dest_Path)
    print(f"successfully copied {t} to Dest")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#get latest modified date from Lakehouse log

latest_file = "abfss://9e63bea0-workspaceID-749d31851c0c@onelake.dfs.fabric.microsoft.com/76c05b7-LakehouseID-6c82b330/Tables/dbo/ProductTbl"
latest_time = None

for file in status:
    mod_time = datetime.fromtimestamp(file.getModificationTime() / 1000)
    if latest_time is None or mod_time > latest_time:
        latest_time = mod_time
        latest_file = file

print("Most recent source updated file:", latest_file.getPath().getName())
print("Last modified time: ", latest_time)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#get latest modified date from Lakehouse log using hadoop (option)

latest_file = "abfss://9e63bea0-workspaceID-749d31851c0c@onelake.dfs.fabric.microsoft.com/76c05b7-LakehouseID-6c82b330/Tables/dbo/ProductTbl"

fs = spark._jvm.org.apache.hadoop.fs.FilesSystem.get(spark._jsc.hadoopConfiguration())

status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(latest_file))

for file in status:
    mod_time = datetime.fromtimestamp(file.getModficationTime()/ 1000)

print(file.getPath().getName(), mod_time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#how to add filed parameter in sematic model suing sematic lik
%%pip install semantic-link-labs
import sempyfrom sempy_labs.tom import connect_sematic_model

datasetName = "TestModel"
workspaceName = "MyWS_dev"
with connect_seantic model(dataset = datasetName, readonly = False, workspace=workspaceName) as model:
    FieldParameterTable = 'FileParameter'
    FieldsIncluded = ["'pduct[productName]", "product[ProductColor]", "DicCustomer[Area]", "DimCustomer[CustID]"]
    model.add_field_parameter(table_name = FieldParameterTable, object = FieldsIncluded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Change column to Pascal Case
def to_pascal_case():
    return ''.join.(word.capitalize() for word in s.split('_'))

#example
input_string = "product_sales_table"
pascal_case = to_pascal_case(input_string)
print(pascal_case)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #Update record modified date from NB into rptSourceRefresh table

# CELL ********************

# MAGIC %%sql
# MAGIC select * from vw_DimCopyProd_LessData

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC df_pdt = spark.read.format("csv").option("header","true").load('Files/csv/ProductData_Fabric.csv')
# MAGIC display(df.limit(10))
# MAGIC #load csv file

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#load csv file and create table, if no header
from pyspark.sql.types import *

Pdt = StructType(
    [
        StructField("GroupP", StringType()),
        StructField("NameM", StringType())
    ]
)
df = spark.read.load('Files/csv/SampleData_Fabric.csv',
    format ='csv',
    schema = Pdt,
    header = False)
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#filter and add column
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

#Read csv file and load in table
#df_agg.write.mode("overwrite").parquet('Files/csv/Prodt_summary.parquet')
df_agg.write.partitionBy("Group").mode("overwrite").parquet('Files/csv/Pdt_sumry_partition')
df_pdt.createOrReplaceTempView("vw_Pdt_tbl")
df_pdt.write.format("delta").saveAsTable("ProductTst")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#select and display in chart
from matplotlib import pyplot as plt

data = spark.sql("select Category, count(*) as PrdCount from vw_Pdt_tbl group by Category").toPandas()

plt.clf()
fig = plt.figure(figsize = (12,8))

plt.bar(x=data['Category'], height=data["PrdCount"], color='orange')

plt.title('pdt count')
plt.xlabel('Category')
plt.ylabel('PrdCount')
plt.grid(color='#9515a6', linestyle = '--', linewidth = 2, axis ='y', alpha=0.7)
plt.xticks(rotation=70)
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create table from sample and insert data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

# Define schema
schema = StructType([
    StructField("WorkStream", StringType(), True),
    StructField("DataCategory", StringType(), True),
    StructField("SupportAlias", StringType(), True),
    StructField("logedBy", StringType(), True),
    StructField("IsActive", StringType(), True),
    StructField("UpStreamRefreshData", StringType(), True),
    StructField("SourceRefreshDetail", StringType(), True)
])

# Create data
data = [
    ("Sales",   "Sales1",   "supportAsk", "Abeb", "Y", "12/30/2025", "12/31/2025"),
    ("Sales",   "Sales2",   "supportAsk", "Cavd", "Y", "12/30/2025", "12/31/2025"),
    ("Product", "Product1", "pdtsupport", "Ceda", "Y", "12/30/2025", "12/31/2025"),
    ("QA",      "QA1",      "qaSupport",  "Bka",  "Y", "12/30/2025", "12/31/2025")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()

df.write.format("delta").saveAsTable("ad.tblSourceRefresh")
#df.write.format("delta").mode("overwrite").saveAsTable("YourTableName")
#df.write.format("delta").mode("append").saveAsTable("YourTableName")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
