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
spark.conf.set("fs.azure.account.auth.type.dlkt.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlkt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlkt.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dlkt.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlkt.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

circuits_df = spark.read \
                .option("header", True) \
                .schema(circuits_schema) \
                .csv(f"{raw_folder_path}/circuits.csv")

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                            .withColumnRenamed("circuitRef", "circuit_ref") \
                            .withColumnRenamed("lat", "latitude") \
                            .withColumnRenamed("lng", "longitude") \
                            .withColumnRenamed("alt", "altitude") \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))

circuits_final_df = add_ingestion_date(circuits_renamed_df)



circuits_final_df.write.mode("overwrite").format("delta").save(f"{presentation_folder_path}/circuits")

dbutils.notebook.exit("Success")



