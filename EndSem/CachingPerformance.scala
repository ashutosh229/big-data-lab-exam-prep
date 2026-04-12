4. Performance: Use .cache() on an RDD after a heavy transformation and observe the
execution time of subsequent actions compared to an un-cached RDD.

import org.apache.spark.sql.SparkSession

object CachePerformance {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Cache Performance Demo")
      .getOrCreate()

    val sc = spark.sparkContext

    // Load file
    val data = sc.textFile("/user/root/logs.txt")

    // Heavy transformation (simulate with filter + map)
    val transformedRDD = data
      .filter(line => line.contains("ERROR"))
      .map(line => line.toUpperCase)

    // ---------------- WITHOUT CACHE ----------------
    val start1 = System.currentTimeMillis()

    transformedRDD.count()
    transformedRDD.collect()

    val end1 = System.currentTimeMillis()
    println(s"Time WITHOUT cache: ${end1 - start1} ms")

    // ---------------- WITH CACHE ----------------
    val cachedRDD = transformedRDD.cache()

    val start2 = System.currentTimeMillis()

    cachedRDD.count()   // first action → fills cache
    cachedRDD.collect() // second action → uses cache

    val end2 = System.currentTimeMillis()
    println(s"Time WITH cache: ${end2 - start2} ms")

    spark.stop()
  }
}