2. DataFrame API: Using the DataFrame API, perform a series of transformations: filter
out records with missing values, select a subset of columns, group by a categorical column,
and compute the average of a numeric column. 

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataFrameTransformations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrame Transformations")
      .master("local[*]")
      .getOrCreate()

    // Step 1: Define schema
    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("department", StringType, true),
      StructField("marks", DoubleType, true)
    ))

    // Step 2: Load CSV into DataFrame
    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("hdfs:///user/root/students.csv")

    // Step 3: Filter out records with missing values
    val cleanedDF = df.na.drop()

    // Step 4: Select subset of columns
    val selectedDF = cleanedDF.select("department", "marks")

    // Step 5: Group by department and compute average marks
    val resultDF = selectedDF
      .groupBy("department")
      .agg(avg("marks").alias("average_marks"))

    // Show result
    resultDF.show()

    // writing output 
    resultDF.write
  .mode("overwrite")
  .option("header", "true")
  .csv("hdfs:///user/root/output_avg_marks")

    spark.stop()
  }
}

