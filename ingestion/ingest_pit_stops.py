from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col,*

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

%run "../configuration/config"

%run "../utils/common_functions"


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Create spark session
spark = SparkSession.builder.getOrCreate()

client_id = dbutils.secrets.get(scope="<databricks-scope>",key="<client-id-key-name>")
tenant_id = dbutils.secrets.get(scope="<databricks-scope>",key="<tenant-id-key-name>")
client_secret = dbutils.secrets.get(scope="<databricks-scope>",key="<client-secret-key-name>")

#Set the configs to authenticate to the storage account using service principal
spark.conf.set("fs.azure.account.auth.type."<storage_account>".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."<storage_account>".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."<storage_account>".dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret."<storage_account>".dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."<storage_account>".dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

pit_stops_df = spark.read \
                    .schema(pit_stops_schema) \
                    .option("multiLine", True) \
                    .json(f"{raw_folder_path}/pit_stops.json")

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)


final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("data_source", lit(v_data_source)) \
                    .withColumn("file_date", lit(v_file_date))

final_df.write.mode("overwrite").partitionBy('race_id').format("delta").save(f"{presentation_folder_path}/stops")

dbutils.notebook.exit("Success")