from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col,*
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

%run "../configuration/config"

%run "../utils/common_functions"




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

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
                                            .withColumnRenamed("number", "driver_number") \
                                            .withColumnRenamed("name", "driver_name") \
                                            .withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
                            .withColumnRenamed("name", "team")

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location")


races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
                            .withColumnRenamed("name", "race_name") \
                            .withColumnRenamed("race_timestamp", "race_date")

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
                            .filter(f"file_date = '{v_file_date}'") \
                            .withColumnRenamed("time", "race_time") \
                            .withColumnRenamed("race_id", "result_race_id") \
                            .withColumnRenamed("file_date", "result_file_date")

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)


race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"

merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')



