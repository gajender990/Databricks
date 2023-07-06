# Databricks notebook source
# MAGIC %md
# MAGIC Autloader code to read dataset but we are not using it for airlines because we want to read through stuructured programming
# MAGIC
# MAGIC df = spark.readStream.format("cloudFiles").option("cloudFiles.format", 'json')\
# MAGIC     .option("cloudFiles.schemaLocation", '/dbfs/FileStore/tables/schema/airlines')\
# MAGIC     .load('/mnt/raw_datalake/airlines/') # this load specifies from which location(folder) we need to read data i.e. cloud container raw
# MAGIC     
# MAGIC     #option  # it will specify the location at which we need to store the schema at databricks 

# COMMAND ----------

# reading data through strcutured streaming
df= spark.read.json('/mnt/raw_datalake/airlines/')
display(df)

# COMMAND ----------

df= spark.read.json('/mnt/raw_datalake/airlines/')
display(df)

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

display(df)

# COMMAND ----------

# Writing data to datalake (Azure gen2 cleansed)
df.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

# Now we are modifying l in such a way that it will give data like tailnum string, type string.......,Date_Part date so that we can create an easy way to create an schema variable and pass it to sql query
def pre_schema(location):
    try:
        # We are reading schema through below command and limit 1 is there because we don't want to read scheams of all the rows
        # because schema of 1 rows will be same as below rows because we are working for structured data
        df= spark.read.format('delta').load(f'{location}').limit(1)
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
# MAGIC def f_delta_cleansed_load(table_name, location, database):
# MAGIC     try:
# MAGIC         schema = pre_schema(f'{location}') 
# MAGIC         spark.sql(f"""DROP TABLE IF EXISTS {database}.{table_name}""");
# MAGIC         spark.sql(f"""
# MAGIC         create table {database}.{table_name}
# MAGIC         ({schema})
# MAGIC         using delta
# MAGIC         location '{location}'
# MAGIC         """)
# MAGIC     except Exception as err:
# MAGIC         print("Error Occured", str(err))

# COMMAND ----------

# We are reading schema through below command but now we don't need it as we are alreading running below command in pre_schema function
#df = spark.read.format('delta').load('/mnt/cleansed_datalake/airline')
# we are now calling pre_schema function inside d_delta_cleansed_load
#schema = pre_schema('/mnt/cleansed_datalake/airline') 
f_delta_cleansed_load('airline', '/mnt/cleansed_datalake/airline','cleansed_geekcoders')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airline

# COMMAND ----------


