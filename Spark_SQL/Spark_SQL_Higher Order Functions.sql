-- Databricks notebook source
CREATE OR REPLACE TEMPORARY VIEW nested_data AS

SELECT   id AS key,
         ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT), CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT)) AS values
         ,
         ARRAY(ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT)), ARRAY(CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT))) AS nested_values
FROM range(5)

-- COMMAND ----------

-- MAGIC %sql SELECT * FROM nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The Simple Example
-- MAGIC Adding one to every value in an array.

-- COMMAND ----------

SELECT  key,
        values,
        TRANSFORM(values, value -> value + 1) AS values_plus_one
FROM    nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Capturing variables
-- MAGIC We can also use other variables than the arguments in a lambda function; this is called capture. We can use variables defined on the top level, or variables defined in intermediate lambda functions. For 

-- COMMAND ----------

SELECT  key,
        values,
        TRANSFORM(values, value -> value + key) AS values_plus_key
FROM    nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###Nesting
-- MAGIC 
-- MAGIC Sometimes data is deeply nested, if you want to transform such data, you can can use nested lambda functions to do this. The following example transforms an array of integer arrays, and adds the `key` (top level) column and the size of the intermediate array to each element in the nested array.

-- COMMAND ----------

SELECT   key,
         nested_values,
         TRANSFORM(nested_values,
           values -> TRANSFORM(values,
             value -> value + key + SIZE(values))) AS new_nested_values
FROM     nested_data

-- COMMAND ----------

SELECT   key,
         values,
         TRANSFORM(values, value -> value + key) transformed_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC exists(array<T>, function<T, V, Boolean>): Boolean
-- MAGIC Return true if predicate function holds for any element in input array.

-- COMMAND ----------

SELECT   key,
         values,
         EXISTS(values, value -> value % 10 == 1) filtered_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC filter(array<T>, function<T, Boolean>): array<T>
-- MAGIC   
-- MAGIC Produce an output array<T> from an input array<T> by only only adding elements for which predicate function<T, boolean> holds.

-- COMMAND ----------

SELECT   key,
         values,
         FILTER(values, value -> value > 50) filtered_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####`aggregate(array<T>, B, function<B, T, B>, function<B, R>): R`
-- MAGIC 
-- MAGIC Reduce the elements of `array<T>` into a single value `R` by merging the elements into a buffer `B` using `function<B, T, B>` and by applying a finish `function<B, R>` on the final buffer. The initial value `B` is determined by a zero expression. The finalize function is optional, if you do not specify the function the finalize function the identity function `(id -> id)` is used.
-- MAGIC 
-- MAGIC This is the only higher order function that takes two lambda functions.
-- MAGIC 
-- MAGIC The following example sums (aggregates) the `values` array into a single (sum) value. Both a version with a finalize function (`summed_values`) and one without a finalize function `summed_values_simple` is shown:

-- COMMAND ----------

SELECT   key,
         values,
         REDUCE(values, 0, (value, acc) -> value + acc, acc -> acc) summed_values,
         REDUCE(values, 0, (value, acc) -> value + acc) summed_values_simple
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can also compute more complex aggregates. The code below shows the computation of the geometric mean of the array elements.

-- COMMAND ----------

SELECT   key,
         values,
         AGGREGATE(values,
           (1.0 AS product, 0 AS N),
           (buffer, value) -> (value * buffer.product, buffer.N + 1),
           buffer -> Power(buffer.product, 1.0 / buffer.N)) geomean
FROM     nested_data
