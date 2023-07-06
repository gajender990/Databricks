# Databricks notebook source
# the code sets up a streaming DataFrame that reads CSV data from cloud storage
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", 'parquet')\
    .option("cloudFiles.schemaLocation", '/dbfs/FileStore/tables/schema/Cancellation.parquet')\
    .load('/mnt/raw_datalake/Cancellation.parquet/') # this load specifies from which location(folder) we need to read data i.e. cloud container raw
    #option  # it will specify the location at which we need to store the schema at databricks 

# COMMAND ----------

# MAGIC %md
# MAGIC  we specify parquet because the folder name contains parquet in raw datalake

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

display(df)

# COMMAND ----------

# Creating a new dataframe and selecting some of the columns and also renaming some of them
df_base = df.selectExpr("Code as code", 
                        "Description",
                        "to_date(Date_Part, 'yyyy-MM-dd') as Date_Part",
                        )
# Writing df_base to cleansed container in Azure
df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option("checkpointLocation", '/dbfs/FileStore/tables/checkpointLocation/Cancellation')\
    .start("/mnt/cleansed_datalake/Cancellation")# this load specifies at which location we need to write data i.e. cloud container raw

# COMMAND ----------

display(df_base)

# COMMAND ----------

# Format will give us schema details
df = spark.read.format('delta').load('/mnt/cleansed_datalake/Cancellation')

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

df = spark.read.format('delta').load('/mnt/cleansed_datalake/Cancellation')
schema = pre_schema(df) 
f_delta_cleansed_load('Cancellation', '/mnt/cleansed_datalake/Cancellation',schema,'cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.Cancellation

# COMMAND ----------


