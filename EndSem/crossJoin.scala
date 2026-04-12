import org.apache.spark.sql.SparkSession

object CrossJoinExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Cross Join Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val students = Seq("Alice", "Bob").toDF("name")
    val subjects = Seq("Math", "Science").toDF("subject")

    // 🔴 Cross Join
    val crossJoinDF = students.crossJoin(subjects)

    crossJoinDF.show()

    spark.stop()
  }
}

students.createOrReplaceTempView("students")
subjects.createOrReplaceTempView("subjects")

spark.sql("""
  SELECT *
  FROM students
  CROSS JOIN subjects
""").show()