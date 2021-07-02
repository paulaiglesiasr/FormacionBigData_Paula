package cap5

import org.apache.spark.sql.SparkSession

object MySQL_empleados_API_DF {
  def consulta1() {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    spark.conf.set("spark.sql.adaptive.enabled",true)

    val t0 = System.nanoTime()
    // Read Option 1: Loading data from a JDBC source using load method
    val employeesDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
      .option("dbtable", "employees")
      .option("user", "root")
      .option("password", "123456")
      .load()
      //.createOrReplaceTempView("empleados")


    val salariesDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
      .option("dbtable", "salaries")
      .option("user", "root")
      .option("password", "123456")
      .load()
      //.createOrReplaceTempView("salarios")

    val titlesDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
      .option("dbtable", "titles")
      .option("user", "root")
      .option("password", "123456")
      .load()
      //.createOrReplaceTempView("titulos")

    val employeesDFRenamed = employeesDF.withColumnRenamed("emp_no","emp_no_employee")

    val salariesDFRenamed = salariesDF
      .withColumnRenamed("from_date","from_date_salaries")
      .withColumnRenamed("to_date","to_date_salaries")

    val joins = employeesDFRenamed
      .join(salariesDFRenamed, employeesDFRenamed("emp_no_employee")===salariesDFRenamed("emp_no"))
      .join(titlesDF, employeesDFRenamed("emp_no_employee")===titlesDF("emp_no"))

    val result = joins
      .select( "emp_no_employee", "first_name", "birth_date", "gender", "hire_date", "salary", "from_date_salaries", "to_date_salaries", "title")

    val t1 = System.nanoTime()
    val elapsedNs = (t1 - t0)
    val elapsedMS = elapsedNs / 1000000000.0
    println("Elapsed time: " + elapsedMS  + "s" )
  }
}
