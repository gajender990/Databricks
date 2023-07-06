# Databricks notebook source
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

# MAGIC %sql
# MAGIC desc history cleansed_geekcoders.airport
# MAGIC --version tells us that how many times our data got updated, so it will increase that many times 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airport

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
# MAGIC     print("Difference is huge")
# MAGIC else:
# MAGIC     print(final_count_current-final_count_previous)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history cleansed_geekcoders.plane

# COMMAND ----------

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
# MAGIC             raise Exception("DIfference is huge in", table_name)
# MAGIC         else:
# MAGIC             print("Difference is" , final_count_current-final_count_previous)
# MAGIC     #except Exception as err:
# MAGIC         #print("Error occured", str(err))

# COMMAND ----------

f_count_check('cleansed_geekcoders', 'Streaming Update', 'unique_carriers',100)

# COMMAND ----------


