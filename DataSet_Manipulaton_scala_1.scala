// Databricks notebook source
import org.apache.spark.sql.Dataset

implicit class DatasetDisplay(ds: Dataset[_]) {
  def display(): Unit = {
    com.databricks.backend.daemon.driver.EnhancedRDDFunctions.display(ds)
  }
}

// COMMAND ----------

dbutils.fs.head("/databricks-datasets/samples/population-vs-price/data_geo.csv")

// COMMAND ----------

display(spark.read.option("header", "true").option("inferSchema", "true").csv("/databricks-datasets/samples/population-vs-price/data_geo.csv"))

// COMMAND ----------

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

case class City(rank: Int, city: String, state: String, stateCode: String, popEstimate: Option[Int], medianSalePrice: Option[Double])

// COMMAND ----------

val ds = df.withColumnRenamed("2014 rank", "rank")
           .withColumnRenamed("State Code", "stateCode")
           .withColumnRenamed("2014 Population Estimate", "popEstimate")
           .withColumnRenamed("2015 median sales price", "medianSalePrice")
           .as[City]

// COMMAND ----------

display(ds)

// COMMAND ----------

ds.map(_.city).display()

// COMMAND ----------

ds.filter(_.rank <= 20).orderBy($"rank").display()
