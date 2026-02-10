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

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                        .filter(f"file_date = '{v_file_date}'")

race_year_list = df_column_to_list(race_results_df, 'race_year')

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                        .filter(col("race_year").isin(race_year_list))

driver_standings_df = race_results_df \
                        .groupBy("race_year", "driver_name", "driver_nationality") \
                        .agg(sum("points").alias("total_points"),
                             count(when(col("position") == 1, True)).alias("wins"))

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"

merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')




