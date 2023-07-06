# Databricks notebook source
# the code sets up a streaming DataFrame that reads CSV data from cloud storage
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", 'csv')\
    .option("cloudFiles.schemaLocation", '/dbfs/FileStore/tables/schema/PLANE')\
    .load('/mnt/raw_datalake/PLANE/')

# COMMAND ----------

# Creating a new dataframe and selecting some of the columns and also renaming some of them
df_base = df.selectExpr("tailnum as tailid", "type", "manufacturer", "to_date(issue_date) as issue_date","model", "status", "aircraft_type", "engine_type", "cast('year' as int) as year", "Date_Part")

# Writing df_base to cleansed container
df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option("checkpointLocation", '/dbfs/FileStore/tables/checkpointLocation/PLANE')\
    .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

display(df_base)

# COMMAND ----------

df_base = df.selectExpr("tailnum as tailid", "type", "manufacturer", "to_date(issue_date) as issue_date","model", "status", "aircraft_type", "engine_type", "cast('year' as int) as year", 
                        "to_date(Date_Part, 'yyyy-MM-dd') as Date_Part")

# COMMAND ----------

df_base.dtypes

# COMMAND ----------

# Writing df_base to cleansed container
df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option("checkpointLocation", '/dbfs/FileStore/tables/checkpointLocation/PLANE')\
    .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

df_base

# COMMAND ----------

# Checking whether our schema updated file has been saved or not(check Dtype of Date_part if it is String- schema not update , if it is date - schema updated)
df = spark.read.format('delta').load('/mnt/cleansed_datalake/plane')

# COMMAND ----------

# MAGIC %py
# MAGIC # Creating a new dataframe and selecting some of the columns and also renaming some of them
# MAGIC df_base = df.selectExpr("tailnum as tailid", "type", "manufacturer", "to_date(issue_date) as issue_date","model", "status", "aircraft_type", "engine_type", "cast('year' as int) as year", "to_date(Date_Part, 'yyyy-MM-dd') as Date_Part")
# MAGIC
# MAGIC # Writing df_base to cleansed container
# MAGIC df_base.writeStream.trigger(once=True)\
# MAGIC     .format('delta')\
# MAGIC     .option("checkpointLocation", '/dbfs/FileStore/tables/checkpointLocation/PLANE')\
# MAGIC     .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

# MAGIC %md 
# MAGIC As we can see above code gives an error because when we tried to update schema it will surely give error so first we need to delete it from both checkpoint and mount location

# COMMAND ----------

# Deleting data from mount location i.e. deleting from sink location of Datalake
dbutils.fs.rm('/mnt/cleansed_datalake/plane', True)

# COMMAND ----------

# # Deleting data from mount location i.e. deleting from checkpoint location of Datalake
dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/PLANE', True)

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %py
# MAGIC # Creating a new dataframe and selecting some of the columns and also renaming some of them
# MAGIC df_base = df.selectExpr("tailnum", "type", "manufacturer", "to_date(issue_date) as issue_date","model", "status", "aircraft_type", "engine_type", "cast('year' as int) as year", "to_date(Date_Part, 'yyyy-MM-dd') as Date_Part")
# MAGIC
# MAGIC # Writing df_base to cleansed container
# MAGIC df_base.writeStream.trigger(once=True)\
# MAGIC     .format("delta")\
# MAGIC     .option("checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/PLANE")\
# MAGIC     .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

# MAGIC %md
# MAGIC If we got error like [WRITE_STREAM_NOT_ALLOWED] `writeStream` can be called only on streaming Dataset/DataFrame then we need to first run `readStream` query and then run writeStream Query because 
# MAGIC
# MAGIC n Spark Structured Streaming dataframes/datasets are created out stream using readStream on SparkSession. If the dataframe/dataset are not created using stream then you are not allowed store using writeStream.
# MAGIC
# MAGIC So create the dataframes/datasets using readStream and store the dataframes/datasets using writeStream

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_datalake/plane')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Creating schema and creating tables

# COMMAND ----------

l= df.dtypes
l

# COMMAND ----------

# Now we are modifying l in such a way that it will give data like tailnum string, type string.......,Date_Part date so that we can create an easy way to create an schema variable and pass it to sql query
def pre_schema(df):
    try:
        schema=""
        for i in df.dtypes:
            schema= schema+i[0]+" "+i[1]+","
        return schema[0:-1]## because in last we don't want ,
    except Exception as err:
        print("Error Occured", str(err))

# COMMAND ----------

schema = pre_schema(df)

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_datalake/plane')
schema = pre_schema(df) 

# COMMAND ----------

# MAGIC %md
# MAGIC Please note the difference between above 2 queries and why we need to create df again(for now i belive it is due to lazy evaluatiom)

# COMMAND ----------

# MAGIC %md
# MAGIC Before creating tables we need to create database
# MAGIC Go to Geekcoders folder, create, notebook(Creation Database)
# MAGIC There is just 1 query
# MAGIC `create database if not exists cleansed_geekcoders`
# MAGIC but for our understanding we are creating a new file there
# MAGIC through this query we try to create database

# COMMAND ----------

## Creating a table through below function

# COMMAND ----------

# MAGIC %py
# MAGIC def f_delta_cleansed_load(table_name, location,schema, database):
# MAGIC     try:
# MAGIC         spark.sql(f"""
# MAGIC         create table {database}.{table_name}
# MAGIC         ({schema})
# MAGIC         using delta
# MAGIC         location '{location}'
# MAGIC         """)
# MAGIC     except Exception as err:
# MAGIC         print("Error Occured", str(err))

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_datalake/plane')
schema = pre_schema(df) 
f_delta_cleansed_load('plane', '/mnt/cleansed_datalake/plane',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.plane

# COMMAND ----------


