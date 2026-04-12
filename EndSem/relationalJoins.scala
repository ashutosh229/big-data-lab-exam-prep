import org.apache.spark.sql.SparkSession

object JoinsExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Joins")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrames
    val students = Seq(
      (1, "Alice", 101),
      (2, "Bob", 102),
      (3, "Charlie", 103),
      (4, "David", 101)
    ).toDF("id", "name", "dept_id")

    val departments = Seq(
      (101, "CS"),
      (102, "IT"),
      (104, "Mechanical")
    ).toDF("dept_id", "dept_name")

    // 🔹 INNER JOIN
    val innerJoin = students.join(departments, "dept_id")

    // 🔹 LEFT OUTER JOIN
    val leftJoin = students.join(departments, Seq("dept_id"), "left")

    // 🔹 RIGHT OUTER JOIN
    val rightJoin = students.join(departments, Seq("dept_id"), "right")

    // 🔹 FULL OUTER JOIN
    val fullJoin = students.join(departments, Seq("dept_id"), "outer")

    // 🔹 LEFT SEMI JOIN (only matching rows from left)
    val semiJoin = students.join(departments, Seq("dept_id"), "left_semi")

    // 🔹 LEFT ANTI JOIN (non-matching rows from left)
    val antiJoin = students.join(departments, Seq("dept_id"), "left_anti")

    // Show examples
    println("=== INNER JOIN ===")
    innerJoin.show()

    println("=== LEFT JOIN ===")
    leftJoin.show()

    println("=== LEFT SEMI JOIN ===")
    semiJoin.show()

    println("=== LEFT ANTI JOIN ===")
    antiJoin.show()

    spark.stop()
  }
}