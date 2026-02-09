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

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
])

#Set the configs to authenticate to the storage account using service principal
spark.conf.set("fs.azure.account.auth.type."<storage_account>".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."<storage_account>".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."<storage_account>".dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret."<storage_account>".dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."<storage_account>".dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

drivers_df = spark.read \
                .schema(drivers_schema) \
                .json(f"{raw_folder_path}/drivers.json")


drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

drivers_final_df = drivers_with_columns_df.drop(col("url"))

drivers_final_df.write.mode("overwrite").format("delta").save(f"{presentation_folder_path}/drivers")

dbutils.notebook.exit("Success")






