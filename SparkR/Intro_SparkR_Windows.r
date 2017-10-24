#run Everytime
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "C:/spark-2.2.0-bin-hadoop2.7/spark-2.2.0-bin-hadoop2.7")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))


# Databricks notebook source
library(SparkR)

sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))


# Creating SparkDataFrames
df <- as.DataFrame(faithful)

# Displays the first part of the SparkDataFrame
head(df)

# Set this to where Spark is installed
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
