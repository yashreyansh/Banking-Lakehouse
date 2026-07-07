import os 

def get_secret(key: str , scope: str = "banking-lakehouse") ->str:
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=scope, key=key)
    except:
        return os.getenv(key)