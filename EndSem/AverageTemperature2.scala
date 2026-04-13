2. Averaging: Given a dataset of (Year, Temperature), calculate the average temperature
for each year using RDD transformations. 

import org.apache.spark.sql.SparkSession

object AverageTemperature {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Average Temperature Per Year")
      .getOrCreate()

    val sc = spark.sparkContext

    // Load data from HDFS
    val data = sc.textFile("/user/root/temperature.txt")

    // Parse into (year, temperature)
    val parsedRDD = data.map { line =>
      val parts = line.split(",")
      (parts(0).toInt, parts(1).toDouble)
    }

    // Convert to (year, (temperature, 1))
    val pairedRDD = parsedRDD.mapValues(temp => (temp, 1))

    // Reduce to (year, (totalTemp, count))
    val reducedRDD = pairedRDD.reduceByKey {
      case ((temp1, count1), (temp2, count2)) =>
        (temp1 + temp2, count1 + count2)
    }

    // Calculate average
    val avgRDD = reducedRDD.mapValues {
      case (total, count) => total / count
    }

    // Collect and print
    avgRDD.collect().foreach {
      case (year, avg) =>
        println(s"Year: $year, Average Temperature: $avg")
    }
    
    // for saving into file 
    // Convert to string before saving
    val output = avgRDD.map {
      case (year, avg) => s"$year,$avg"
    }
    output.saveAsTextFile("/user/root/output_avg_temp")

    spark.stop()
  }
}