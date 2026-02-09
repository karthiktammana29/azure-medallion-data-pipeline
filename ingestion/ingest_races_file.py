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

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

races_df = spark.read \
            .option("header", True) \
            .schema(races_schema) \
            .csv(f"{raw_folder_path}/races.csv")

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))


races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)


races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'),
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

races_selected_df.write.mode("overwrite").format("delta").save(f"{presentation_folder_path}/races")

dbutils.notebook.exit("Success")









