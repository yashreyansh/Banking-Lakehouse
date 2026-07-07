
storage_account = "bankinglakehouse"


staging_container = f"abfss://staging-data@{storage_account}.dfs.core.windows.net/account_stage/"

account_checkpoint = f"abfss://staging-data@{storage_account}.dfs.core.windows.net/checkpoints/account_checkpoint/"

target_table = "banking_prod.banking_bronze.bronze_accounts" 

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","parquet")
    .option("cloudFiles.schemaLocation",account_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(staging_container)
)

df.writeStream\
.format("delta")\
.option("checkpointLocation",account_checkpoint)\
.trigger(availableNow=True)\
.outputMode("append")\
.toTable(target_table)
