# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install psycopg2-binary
# MAGIC import psycopg2
# MAGIC import pandas as pd
# MAGIC
# MAGIC connection = psycopg2.connect(
# MAGIC     host="34.93.124.156",
# MAGIC     port="5432",
# MAGIC     database="market_pipeline",
# MAGIC     user="crypto_user",
# MAGIC     password="<YOUR_DB_PASSWORD>",
# MAGIC     sslmode="require"
# MAGIC )
# MAGIC
# MAGIC df = pd.read_sql("SELECT * FROM crypto_prices ORDER BY event_time DESC LIMIT 10;", connection)
# MAGIC connection.close()
# MAGIC
# MAGIC df

# COMMAND ----------

# Bronze: Cloud SQL se poora raw data Spark DataFrame mein laate hain
jdbc_url = "jdbc:postgresql://34.93.124.156:5432/market_pipeline?sslmode=require"

bronze_df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "crypto_prices")
    .option("user", "crypto_user")
    .option("password", "<YOUR_DB_PASSWORD>")
    .option("driver", "org.postgresql.Driver")
    .load()
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_crypto_prices")

display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

silver_df = (bronze_df
    .dropDuplicates(["symbol", "event_time"])
    .withColumn("event_time", to_timestamp(col("event_time")))
    .select("symbol", "price", "currency", "event_time")
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_crypto_prices")

display(silver_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import date_trunc, avg, min, max, count

gold_df = (silver_df
    .withColumn("hour", date_trunc("hour", col("event_time")))
    .groupBy("symbol", "hour")
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("num_records")
    )
    .orderBy("hour", "symbol")
)

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_crypto_hourly_summary")

display(gold_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

