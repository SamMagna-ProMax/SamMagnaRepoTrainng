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

from pyspark.sql.functions import col, lit, udf
#from delta.table import DeltaTable
#from sempy.fabric as fabric
#from sepy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
import re
from pyspark.sql import SparkSession
#from pyspark.sql.types import StringType IntegerType, TimestampType
from pyspark.sql import DataFrame


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import io
import sempy.fabric as fabric
import re
import builtins
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql import dataframe
from pyspark.sql.functions import col, lit, udf, count
from delta.tables import DeltaTable
from pyspark.sql.functions import from_utc_timestamp, current_timestamp
from pyspark.sql.functions import (
        regexp_extract, concat_ws, lpad, col, to_timestamp, when
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#def get_lakhouse_path():
#    current_workspace_name = str(notebookutils.env.getworkspaceName()).lower()
#    print(f"current workspace is: {current_workspace_name})
#
#    #assign lakehouse identification and workspace identification from the environment
#    if "MyFabric" in current_workspace_name:
#        dest_workspace_id = "9e63bea0-77f2-4cd2-bc30-749d31851c0c" # MyFabricWS1elopment workspace
#        lakehouse_id = "5c3a5650-5485-4b19-beb2-3661c144c400" #MyFabricWS1elopment lakhouse id
#    elif "MyFabric" in current_workspace_name:
#         dest_workspace_id = "9e63bea0-77f2-4cd2-bc30-749d31851c0c" # production workspace
#        lakehouse_id = "5c3a5650-5485-4b19-beb2-3661c144c400" #production lakhouse id
#    else:
#        raise ValueError("X workspace name does not contain 'MyFabricWS1' or 'prod'. cannot identify the envit)
#
# #if the path is ok, create AVFSS path
#     path_to_lh = f"abfss://{dest_workspace_id)@msit-onelake.dfs.fabric.microsoft.com/{lakehouse_id}/"
#     print(f"Using Lakhouse Path: {path_to_lh}")
#
#    return path_to_lh


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_dbo = "dbo"
schema_adm = "ad"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_lh_paths():
    """ get current workspace return lh path """ 
    cur_ws = notebookutils.mssparkutils.env.getWorkspaceName().lower()
    print(f"current WS: {cur_ws}")

    #Define env
    env_config = {
        "WS1": {
            "workspace_id": "9e63bea0-77f2-4cd2-bc30-749d31851c0c",
            "lakehouses": {
                #list multiple lakehouse
                "LakehouseFab": "5c3a5650-5485-4b19-beb2-3661c144c400"
                #"Lakehouse2" : ""
            }
        }
        ,
        "processing": {
            "workspace_id": "test9e63bea0-77f2-4cd2-bc30-749d31851c0c",
            "lakehouses": {
                #list multiple lakehouse
                "LakehouseFab": "test5c3a5650-5485-4b19-beb2-3661c144c400"
                #"Lakehouse2" : ""
            }
        }
    }

    #determine env
    environment = None
    for env in env_config:
        if f"{env}" in cur_ws:
            environment = env
            break
            
    if not environment:
        raise ValueError("Workspace name MyFabricWS1, processing ...")

    #build lakehouse name
    lh_id = env_config[environment]["lakehouses"]
    if not lh_id:
        raise ValueError("Wrong LH name. correct as LakehouseFab,Lakehouse2... ")

    workspace_id = env_config[environment]["workspace_id"]
    lh_ids = env_config[environment]["lakehouses"]

    #build the path for each name
    #workspace_id = env_config[env]["workspace_id"]
    #lh_ids = env_config[env]["lakehouses"]

    paths = {
        name: f"abfss://{workspace_id}@msit-onelake.dfs.fabric.microsoft.com/{lh_id}/"
        for name, lh_id in lh_ids.items()
    }

    for name, path in paths.items():

        print(f" using {name} Lakehouse {name}: {path}")
    return paths

lh_paths = get_lh_paths()

MyFabricWS1_lh = lh_paths["LakehouseFab"]
#MyFabricWS1_proc = lh_paths["Lakehouse2"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def get_lh_paths():
    """Get current workspace and return lakehouse paths."""
    
    cur_ws = notebookutils.mssparkutils.env.getWorkspaceName().lower()
    print(f"current WS: {cur_ws}")

    # Define environments
    env_config = {
        "WS1": {
            "workspace_id": "9e63bea0-77f2-4cd2-bc30-749d31851c0c",
            "lakehouses": {
                "bronze": "5c3a5650-5485-4b19-beb2-3661c144c400"
                #"LakehouseProd": "test5c3a5650-5485-4b19-beb2-3661c144c400"
            }
        },
        "processing": {
            "workspace_id": "test9e63bea0-77f2-4cd2-bc30-749d31851c0c",
            "lakehouses": {
                "bronze": "test5c3a5650-5485-4b19-beb2-3661c144c400"
                 #"silver": "test5c3a5650-5485-4b19-beb2-3661c144c400"
            }
        }
    }

    # Determine environment
    environment = None
    for env in env_config:
        if env.lower() in cur_ws:
            environment = env
            break

    if not environment:
        raise ValueError("Workspace name must contain WS1 or processing.")

    # Extract config
    workspace_id = env_config[environment]["workspace_id"]
    lh_ids = env_config[environment]["lakehouses"]

    if not lh_ids:
        raise ValueError("No lakehouses defined for this environment.")

    # Build paths
    paths = {
        name: f"abfss://{workspace_id}@msit-onelake.dfs.fabric.microsoft.com/{lh_id}/"
        for name, lh_id in lh_ids.items()
    }

    for name, path in paths.items():
       print(f"Using {name.capitalize()} Lakehouse ({name}_lh): {path}")

    return paths

lh_paths = get_lh_paths()

bronze_lh = lh_paths["bronze"]
#silver_lh = lh_paths["silver"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_lh_paths()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_folder(path):
    if not mssparkutils.fs.exists(path):
        print(f"{path} doesn't exist")
    else:
        mssparkutils.fs.rm(path, True)
        print(f"{path} is deleted")

def path_exists(path):
    if "/" in path:
        if not mssparkutils.fs.exists(path):
            raise FileNotFoundError(f"{path} not exist")
        else:
            print(f"{path} exists")
    else:
        if not spark.catalog.tableExists(path):
            raise FileNotFoundError(f"{path} does not exist")
        else:
            print(f"{path} file exists")

def path_has_data(path):
    path_exists(path) #done
    if "/" in path:
        df = spark.read.format("delta").load(path)
    else:
        df = spark.sql(f"select * from {path}")
    if df.limit(1).count() == 0:
        raise ValueError(f"{path} has not data")
    else:
        print(F"{path} has data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def custom_truncate(source_table, target_delta_path):
    skip = False
    if "/" in source_table:
        source = "path"
    else:
        source = "table"
    
    if source == "table":
        if not spark.catalog.tableExists(source_table):
            raise FileNotFoundError(f"source table '{source_table}' not exist.")
        else:
            source_df = spark.sql(f"SELECT * FROM {source_table}")
    else:
        if not DeltaTable.isDeltaTable(spark, source_table):
            raise FileNotFoundError(f"The source '{source_table}' must be deltal folder or table/view")
        else: 
            source_df=spark.read.format("delta").load(source_table)
    
    if not DeltaTable.isDeltaTable(spark, target_delta_path):
        print(f"target Delta folder '{target_delta_path}' not found. creating target Delta folder 1st time run.")
        skip = True
        target_row_count = 0
    else:
        target_delta_df = spark.read.format("delta").load(target_delta_path)

    if skip != True:
        target_row_count = target_delta_df.limit(1).count()
    source_row_count = source_df.limit(1).count()

    if source_row_count == 0:
        subject = f"drop data notification - {target_delta_df}"
        body = f"source table '{source_table}' has no data, pls check. \nTarget file '{target_delta_path}' data table. please check process."
        print(f"{subject} \n {body}")
    elif target_row_count == 0:
        print("No data in target folder")

    #delete target and write to destination 
    if source_row_count > 0:
        source_df.write.format("delta").mode("overwrite").option("overwriteschema", True).save(target_delta_path)
        print(f"Success data transfered from {source_table} to {target_delta_path}.")

#Below is extra process to automaticaly update downstream LH; for example if there is reporting Gold LH; once the Bronze is refresh, no need of other pipeline;
#automatically update the Gold LH table
        ##To create /refresh table in downstream lakehouse to prod
        #isCMap = target_delta_path.find("/Tables/CMP/")
        #isGold = target_delta_path.find(LakehouseProd_lh)
        #if isCMap != -1 and isGold != -1:
        #    CMapPath = downstream_lh + target_delta_path[isCMap+1:]
        #    spark.read.format("delta").load(target_delta_path).write.format("delta").mode("overwrite").option("overwriteschema", True).save(CMapPath)
        #    print(f"Corresponding CMap downstream LH been refresh/create - {CMapPath}")

    



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_preload_path(rPath):
    bronze_lh = lh_paths["bronze"]
    tblPos = rPath.find("/Tables/")
    filPos = rPath.find("/Files/")
    pos = tblPos if tblPos != -1 else filePos
    if pos == -1:
        raise ValueError(f"wrong path, path must have /Tables/ or /Files/: {rPath}.")
    return bronze_lh + rPath[pos+1:]+'_preload'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#check record limit or threshold
def check_threshold (final_vw, rPath, threshold, aggFun, col=None):
    col = col if col is not None else "*"
    preloadpath = build_preload_path(rPath)
    skip = 0
    path_has_data(final_vw)
    if "/" in final_vw:
        df_preload = spark.read.format("delta").load(final_vw)
    else:
        df_preload = spark.sql(f"SELECT * FROM {final_vw}")
    if "/" in rPath:
        if not mssparkutils.fs.exists(rPath):
            print(f"Destination forlder {rPath} not found.")
            skip = 1
        else:
            df_dest = spark.read.format("delta").load(rPath)
    else:
        if not spark.catalog.tableExists(source_table):
            print(f"Dest folder {rPath} not found. It is 1st time load")
            skip = 1
            df_dest = spark.sql(f"SELECT * FROM {rPath}")
    if skip ==0:
        if threshold !=0:
            df_preload.createOrReplaceTempView("vw_final_vw")
            df_dest.createOrReplaceTempView("vw_rPath")
            preloadResult = spark.sql(f"select {aggFun} ({col}) as col from vw_final_vw").collect()[0][0]
            destResult =spark.sql(f"select {aggFun} ({col}) as col from vw_rPath").collect()[0][0]
            data_diff = (builtins.abs(preloadResult - destResult)/ (destResult)) * 100
            if data_diff >= threshold:
                custom_truncate(final_vw, preloadpath)  #done
                print(f"threshold not correct, data diffce is {data_diff:.2f}%")
                print(f"copied source data preloadPath for further validation : \n preloadPth - {preloadpath}")
                raise ValueError("loading fail; preload has high/low data")
        else:
            drop_folder(preloadpath) #done
            print("The threshold meet and complete")
    else:
        print("threshold skipped for 1st run")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#def LoadTable_fromSharePointExcelSheet(fPath,tgtTable, Sheetname=None):
#    #read binary data from sharePoint (DBFS)
#    binary_data = spark.read.format("binaryFile").load(fPath).collect().[0].content#

#    #get sheetName
#    xls=pd.ExcelFile(io.BytesIO(binary_data))#

#    #Default to 1st sheet if not given
#    if Sheetname is None:
#        Sheetname = xls.sheet_names[0]#

#    #Read the given sheet
#    pdf = pd.read_excel(io.BytesIO(binary_data), sheet_name=Sheetname, keep_default_na=False)#

#    #transform to spark df
#    df=spark.createDataFrame(pdf)
#    df=df.withcolumn("DataModifiedDate", from_utc_timestamp(current_timestamp(), "PST"))#

#    #add in temp view
#    df.createOrReplaceTempView("vw")#

#    #truncate and load in to target table
#    custom_truncate("vw", tgtTable)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Call sharePoint load function - Load sharePoint file
#First create shortcut from sharePoint folder

filePath = f"{bronze_lh}Files/ShrePointFolder/ProductTableFile.xlsx"
targetTable = f"{bronze_lh}/Table/dbo/ProductTableFile"
sheetName = "Sheet1"
LoadTable_fromSharePointExcelSheet(filePath, targetTable, sheetName)

spark.read.format("delta").load(bronze_lh + 'Tables/ad/ProductTableFile').createOrReplaceTempView("vw_Prod")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_shortcut(shortcutPath, shortcutName, sourcePath, sourceSubPath, sourceConnectionID):
    client = fabric.FabricRestClient()
    worspaceId = fabric.get_workspace_id()
    lakehouseName = 'LH_processing'
    lakehouseID = fabric.resolve_item_id(lakehhouseName, 'lakehouse', workspaceId)
    payload = {
        "path": shortcutPath,
        "name": shortcutName ,
        "target": {
            "location": sourcePath,
            "subpath": sourceSubPath,
            "connectionId": sourceConnectionID
        }
    }
    scPath = f"{shortcutPath}{shortcutName}"
    print(scPath)
    if not mssparkutils.fs.exists(scPath):
        response=client.post(f"v1/workspace/{workspaceId}/onelake/resetShortcutCache", json=payload)

        print(f"v1/workspace/{workspaceID}/items/{lakehouseID}/shortcuts")
        print(f"shortcut -{shourcutPth}{shortcutName} created successfuly")
    else:
        print(f"shourtcut -{shortcutPath}{shortctuName} existed. Same name not allowerd")
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_duplicates(df: DataFrame, entity_name: str, columns: list):
    "check and through error for duplicate reocrds"
    df_dupl =(
        df.groupBy(columns)
        .agg(count("*").alias("RecordCount"))
        .filter(col("RecordCount")>1)
    )

    duplicate_count = df_duplicates.count()

    if duplicate_count > 0:
        print(f"fail due to duplicate count: {duplicate_count} {entity_name} with duplicates.")
        raise Exception(f"Duplicate {entity_name} records existed.")
    else:
        print("No duplicate record exist. Ssuceesfuly run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Update record modified date from NB into rptSourceRefresh table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tblSourceRefreshDetail = bronze_lh + "Tables/ad/tblSourceRefresh" 
spark = SparkSession.builder.master("local").appName("Usp_SourceRefreshDetails").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def source_refresh_details(data_category, upstream_refresh_data):   
    try:
        #create temp df
        temp_df = spark.createDataFrame([(data_category, upstream_refresh_data)], ["DataCategory", "UpStreamRefreshData"])

        #load source df existed data from source refresh table
        source_df=spark.read.format("delta").load(tblSourceRefreshDetail)
        
        print("test step: source_df is done")

        #manage upstreamfresh logic - check flags
        update_df=source_df.filter(
            (lower(col("IsActive"))=='y') & (loewr(col("Datacategory"))==lower(lit(data_category)))
        )

        update_df = source_df.filter(
            "UpStreamRefreshData",
            when(col(UpStreamRefreshData).isNull() | (col("UpStreamRefreshData")=='') | (col("UpStreamRefreshData")<lit(upstream_refresh_data)), lit(UpStreamRefreshData))
            .otherwise(col("UpStreamRefreshData"))
        )

        #update data updated date and upstream refresh date
        final_df = source_df.withColumn(
            "SourceRefreshDetail",
            from_utc_timestamp(current_timestamp(), "PST")
        )

        #update by joining main df and applying change
        final_df = source_df.alias('src').join(updated_df.alias('upd'), on=["DataCategory", "IsActive"], how="left_outer").select(
            #select all column from source_df except these need to update
            col('src.workStream'),
            col('src.DataCategory'),
            col('src.supportAlias'),
            col('src.LogedBy'),
            col('src.IsActive'),
            when(col('src.DataCategory') == data_category, col('upd.UpStreamRefreshData')).otherwise(col('src.UpStreamRefreshData')).alias('UpStreamRefreshData'),
            when(col('src.DataCategory') == data_category, col('upd.SourceRefreshDetail')).otherwise(col('src.SourceRefreshDetail')).alias('SourceRefreshDetail')
        )

        print("test step: final_df is done")

        #else the data with the final df
        final_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(tblSourceRefreshDetail)

        print("test step: final_df load is done")

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
