1. Data Loading: Create a sample dataset of your choice (e.g., movies, students, or prod-
ucts) in CSV format. Load it into a DataFrame, ensuring you define a strict schema
beforehand.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataLoadingWithSchema {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Data Loading with Schema")
      .getOrCreate()

    // Define strict schema
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("marks", DoubleType, nullable = false)
    ))

    // Load CSV with schema
    val df = spark.read
      .format("csv")
      .option("header", "false")
      .schema(schema)
      .load("/user/root/students.csv")

    // Show data
    df.show()

    // Print schema (important for viva)
    df.printSchema()

    spark.stop()
  }
}