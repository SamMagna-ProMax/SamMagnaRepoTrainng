# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run Common_fn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, upper


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tblSourceRefreshDetail = bronze_lh + "Tables/ad/tblsourcerefresh"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.master("local").appName("Usp_SourceRefreshDetails").getOrCreate()

def source_refresh_details(data_category, upstream_refresh_data):   
    try:
        #create temp df
        temp_df = spark.createDataFrame([(data_category, upstream_refresh_data)], ["DataCategory", "UpStreamRefreshData"])

       # print("test step: temp_df is done")

        #load source df existed data from source refresh table
        source_df=spark.read.format("delta").load(tblSourceRefreshDetail)

        #manage upstreamfresh logic - check flags
        update_df=source_df.filter(
            (lower(col("IsActive"))=='y') & (lower(col("DataCategory")) == lower(lit(data_category)))
        )

        update_df = update_df.withColumn(
            "UpStreamRefreshData",
            when(col("UpStreamRefreshData").isNull() | (col("UpStreamRefreshData") =='') | (col("UpStreamRefreshData") < lit(upstream_refresh_data)), lit(upstream_refresh_data))
            .otherwise(col("UpStreamRefreshData"))
        )

        #update data updated date and upstream refresh date
        update_df = update_df.withColumn(
            "SourceRefreshDetail",
            from_utc_timestamp(current_timestamp(), "PST")
        )

        #update by joining main df and applying change
        final_df = source_df.alias('src').join(update_df.alias('upd'), on=["DataCategory", "IsActive"], how="left_outer").select(
            #select all column from source_df except these need to update
            col('src.workStream'),
            col('src.DataCategory'),
            col('src.supportAlias'),
            col('src.LogedBy'),
            col('src.IsActive'),
            when(col('src.DataCategory') == data_category, col('upd.UpStreamRefreshData')).otherwise(col('src.UpStreamRefreshData')).alias('UpStreamRefreshData'),
            when(col('src.DataCategory') == data_category, col('upd.SourceRefreshDetail')).otherwise(col('src.SourceRefreshDetail')).alias('SourceRefreshDetail')
        )

       # print("test step: final_df is done")

        #else the data with the final df
        final_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(tblSourceRefreshDetail)

        #print("test step: final_df load is done")

        #print completed 
        print("sucessfuly updated")

    except Exception as e:
        print(f"Error happened: {str(e)}")
        raise
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#run below code at end of each NB; so if the NB successfuly run it will update the Source refresh table
#This is done based on assumption of one table "tblsourcerefresh"; whic has column: workStream, DataCategory, SupportAlias, LgedBy, IsActive, UpStreamRefreshData and SourceRefreshDetail
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
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************


#run below code at end of NB for source caming from different LH through shortcut; so if the NB successfuly run this query is pulling upstream source refresh for example from different LH may be direct shortcut
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

df = spark.read.load('Files/data/products.csv',    format='csv',    header=True)
display(df.limit(10).orderby(""))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC df = spark.read.format("csv").option("header", "true").load("Files/data/products.csv")
# MAGIC display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC df_select = df_pdt.select("ProductID", "ProductName")
# MAGIC df_select2 = df_pdt["ProductID", "ProductName","Group", "ListPrice"].where((df_pdt["Group"]=="A") | (df_pdt["ListPrice"]>2000))
# MAGIC display(df_select2.limit(5))


# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

#df_select = df_pdt.select("ProductID", "ProductName")
df_select2 = df_pdt["ProductID", "ProductName","Group", "ListPrice"].where((df_pdt["Group"]=="A") | (df_pdt["ListPrice"]>2000))
df_count = df_pdt["ProductID","GroupCount"].groupBy("Group").count()
df_agg = df_pdt.groupBy("Group").agg(F.sum(F.col("ListPrice").cast("float")).alias("TotalPrice"))
display(df_agg.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
