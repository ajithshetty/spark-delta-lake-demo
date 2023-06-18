// Databricks notebook source
val file_location = "/FileStore/tables/Salaries-1.csv"
val file_type = "csv"


val salariesDF = spark.read.format(file_type)
  .option("inferSchema", "true")
  .option("header", "true")
  .option("sep", ",")
  .load(file_location)

display(salariesDF)

// COMMAND ----------

val deltaPath = "/FileStore/tables1/delta-salaries"
salariesDF.write.format("delta").mode("overwrite").save(deltaPath)

// COMMAND ----------

salariesDF.write.format("delta").mode("overwrite").saveAsTable("delta_salaries1")

// COMMAND ----------

display(salariesDF.select("Year").distinct)

// COMMAND ----------

salariesDF.write.format("delta").mode("overwrite").partitionBy("Year").option("overwriteSchema", "true").save(deltaPath)

// COMMAND ----------

display(dbutils.fs.ls(deltaPath))

// COMMAND ----------

display(dbutils.fs.ls(deltaPath + "/_delta_log/"))

// COMMAND ----------

display(spark.read.json(deltaPath + "/_delta_log/00000000000000000000.json"))

// COMMAND ----------

display(spark.read.json(deltaPath + "/_delta_log/00000000000000000001.json"))

// COMMAND ----------

display(dbutils.fs.ls(deltaPath + "/Year=2012/"))

// COMMAND ----------

val df = spark.read.format("delta").load(deltaPath)
display(df)

// COMMAND ----------

salariesDF.filter($"Basepay".cast("int") >15000).count()

// COMMAND ----------

val salaries_df_update = salariesDF.filter($"Basepay".cast("int") >15000)
display(salaries_df_update)

// COMMAND ----------

salaries_df_update.write.format("delta").mode("overwrite").save(deltaPath)

// COMMAND ----------

val new_df = spark.read.format("delta").load(deltaPath)
display(new_df)

// COMMAND ----------

display(dbutils.fs.ls(deltaPath + "/Year=2012/"))

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS delta_versions")
spark.sql(f"CREATE TABLE delta_versions USING DELTA LOCATION '$deltaPath'")

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY delta_versions

// COMMAND ----------

val df_0 = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath)
display(df_0)

// COMMAND ----------

val prev_timeStamp = "2021-07-31T12:44:52.000+0000"
val df = spark.read.format("delta").option("timestampAsOf", prev_timeStamp).load(deltaPath)
display(df)

// COMMAND ----------

import io.delta.tables._

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
val delta_table = DeltaTable.forPath(spark, deltaPath)
delta_table.vacuum(0)

// COMMAND ----------

display(dbutils.fs.ls(deltaPath + "/Year=2012/"))

// COMMAND ----------

val old_df = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath)
display(old_df)

// COMMAND ----------


