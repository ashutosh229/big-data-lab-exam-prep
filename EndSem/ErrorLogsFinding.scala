1. Filtering: Load a log file (e.g., logs.txt) and filter for rows containing the keyword
”ERROR”. Count the total number of errors.

import org.apache.spark.sql.SparkSession

object ErrorCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Error Count")
      .getOrCreate()

    val sc = spark.sparkContext

    // Load the log file from HDFS
    val logsRDD = sc.textFile("/user/root/logs.txt")

    // Filter lines containing "ERROR"
    val errorRDD = logsRDD.filter(line => line.contains("ERROR"))

    // Count total errors
    val errorCount = errorRDD.count()

    // Print result
    println(s"Total number of ERROR logs: $errorCount")

    spark.stop()
  }
}