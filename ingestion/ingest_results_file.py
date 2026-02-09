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

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

results_df = spark.read \
                .schema(results_schema) \
                .json(f"{raw_folder_path}/results.json")

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))


results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

results_final_df.write.mode("append").partitionBy('race_id').format("delta").save(f"{presentation_folder_path}/results")

dbutils.notebook.exit("Success")
