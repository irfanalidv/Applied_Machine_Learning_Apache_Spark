# Databricks notebook source
library(SparkR)

# COMMAND ----------

df <- createDataFrame(faithful)
# Displays the content of the DataFrame to stdout
head(df)

# COMMAND ----------

# From Data Sources using Spark SQL
diamondsDF <- read.df("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",
                    source = "csv", header="true", inferSchema = "true")
head(diamondsDF)

# COMMAND ----------

# SparkR automatically infers the schema from the CSV file.
printSchema(diamondsDF)

# COMMAND ----------

display(diamondsDF)

# COMMAND ----------

#  Let's use spark-avro package to load an Avro file.
# First take an existing data.frame, convert to SparkDataFrame and save it as an Avro file. 
irisDF <- createDataFrame(iris)
write.df(irisDF, source = “com.databricks.spark.avro”, path = “dbfs:/tmp/iris.avro”, mode = “overwrite”)

# COMMAND ----------


