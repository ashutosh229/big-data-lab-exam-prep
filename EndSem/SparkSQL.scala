3. Spark SQL Equivalency: Write the exact equivalent of the transformations from Ex-
ercise 2 using standard SQL syntax. Compare the output to ensure they match. 

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkSQLExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL Equivalent")
      .master("local[*]")
      .getOrCreate()

    // Define schema
    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("department", StringType, true),
      StructField("marks", DoubleType, true)
    ))

    // Load data
    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("hdfs:///user/root/students.csv")

    // Register as temporary SQL table
    df.createOrReplaceTempView("students")

    // SQL Query (Equivalent transformation)
    val resultDF = spark.sql(
      """
        SELECT department, AVG(marks) AS average_marks
        FROM students
        WHERE department IS NOT NULL AND marks IS NOT NULL
        GROUP BY department
      """
    )

    // Show result
    resultDF.show()

    spark.stop()
  }
}