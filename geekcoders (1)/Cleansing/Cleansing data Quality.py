# Databricks notebook source
# MAGIC %py
# MAGIC # We are not using try and except blocks because whenever there is an greater difference we want to raise an exception and stops the code which will stop the pipeline
# MAGIC # if we use try and except it will just raise an exception and below code will work means pipeline wont stop
# MAGIC def f_count_check(database, operation_type, table_name, number_diff):
# MAGIC     #try:
# MAGIC         spark.sql(f"""DESC HISTORY {database}.{table_name}""").createOrReplaceTempView("Table_count")
# MAGIC         count_current = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where lower(trim(operation))= lower('{operation_type}'))""")
# MAGIC         if(count_current.first().numOutputRows is None):
# MAGIC             final_count_current = 0
# MAGIC         else:
# MAGIC             final_count_current= int(count_current.first().numOutputRows)
# MAGIC         ## previous file ko identify k liye max se 1 niche version dekha phr dono ka count store kiya aur print kiya
# MAGIC         count_previous = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where lower(trim(operation))= lower('{operation_type}') order by version desc limit 1)""")
# MAGIC         if(count_previous.first().numOutputRows is None):
# MAGIC             final_count_previous = 0
# MAGIC         else:
# MAGIC             final_count_previous= int(count_previous.first().numOutputRows)
# MAGIC         if ((final_count_current-final_count_previous)> number_diff):
# MAGIC             #print("Difference is huge")
# MAGIC             print(final_count_current-final_count_previous)
# MAGIC             raise Exception("DIfference is huge in", table_name)
# MAGIC         else:
# MAGIC             print("Difference is " , final_count_current-final_count_previous, " in", table_name)
# MAGIC     #except Exception as err:
# MAGIC         #print("Error occured", str(err))

# COMMAND ----------

dbutils.fs.ls('/mnt/source_blob_new/')

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

list_table_info = [
    ("STREAMING UPDATE", "plane", 100000),
    ("STREAMING UPDATE", "flights", 24124953),
    ("STREAMING UPDATE", "airport", 10000),
    ("STREAMING UPDATE", "cancellation", 100),
    ("STREAMING UPDATE", "unique_carriers", 20000),
    ("Write", "airline", 100),
]

for i in list_table_info:
    f_count_check("cleansed_geekcoders", i[0], i[1], i[2])

# COMMAND ----------

## As expected code will fail for Write so for airline we are creating below code for data quality check

# COMMAND ----------

# MAGIC %py
# MAGIC spark.sql("""DESC HISTORY cleansed_geekcoders.airline""").createOrReplaceTempView("Table_count")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Table_count

# COMMAND ----------

# MAGIC %py
# MAGIC ## We are getting the counts of current files and previous file-- current file ko identify k liye max version dekha
# MAGIC count_current = spark.sql("""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where operation= 'WRITE')""")
# MAGIC if(count_current.first() is None):
# MAGIC     final_count_current = 0
# MAGIC else:
# MAGIC     final_count_current= int(count_current.first().numOutputRows)
# MAGIC ## previous file ko identify k liye max se 1 niche version dekha phr dono ka count store kiya aur print kiya
# MAGIC count_previous = spark.sql("""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where operation= 'WRITE' order by version desc limit 1)""")
# MAGIC if(count_previous.first() is None):
# MAGIC     final_count_previous = 0
# MAGIC else:
# MAGIC     final_count_previous= int(count_previous.first().numOutputRows)
# MAGIC if ((final_count_current-final_count_previous)> 100):
# MAGIC     #print("Difference is huge")
# MAGIC     print(final_count_current-final_count_previous)
# MAGIC     raise Exception("DIfference is huge in airline")
# MAGIC else:
# MAGIC     print("Difference is " , final_count_current-final_count_previous, " in airline")

# COMMAND ----------


