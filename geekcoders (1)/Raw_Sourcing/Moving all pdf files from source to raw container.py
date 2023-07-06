# Databricks notebook source
pip install tabula-py 

# COMMAND ----------

# MAGIC %md
# MAGIC Below function is getting all the pdf files from /dbfs/mnt/source_blob/ which is pointing to Blob storage and sinking into 'mnt/raw_datalake/ as an csv file and also for all he pages which is pointing to datalake

# COMMAND ----------

import tabula
from datetime import date
def f_source_pdf_datalake(source_path,sink_path,output_format,page,file_name):
    try:
        # Creating directory in sink path sometimes we will get error if directory does not exist
        dbutils.fs.mkdirs(f"/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
        tabula.convert_into(f'{source_path}{file_name}',f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format,pages=page)
    except Exception as err:
        print("Error Occured ",str(err))

# COMMAND ----------

# through this we can see there is an name property so we will be accessing all the filenames through this name property
dbutils.fs.ls('/mnt/source_blob_new/')

# COMMAND ----------

# Getting only the pdf filenames from blob storage
list_files=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob_new/') if(i.name.split('.')[1]=='pdf')]
for i in list_files:
    # Calling function to drop files
    f_source_pdf_datalake('/dbfs/mnt/source_blob_new/','mnt/raw_datalake/','csv','all',i[0])

# COMMAND ----------


