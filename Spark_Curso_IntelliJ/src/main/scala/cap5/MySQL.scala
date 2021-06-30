package cap5

import org.apache.spark.sql.SparkSession

object MySQL {
  def helloWorld(): Unit ={

    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    // Read Option 1: Loading data from a JDBC source using load method
    val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
      .option("dbtable", "departments")
      .option("user", "root")
      .option("password", "123456")
      .load()
      .show()


  }
}
