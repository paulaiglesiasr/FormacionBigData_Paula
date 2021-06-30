package cap5

import org.apache.spark.sql.SparkSession

object MySQL_empleados {
  def consulta1(){}
  val spark = SparkSession
    .builder
    .appName("MnMCount")
    .getOrCreate()

  // Read Option 1: Loading data from a JDBC source using load method
  val jdbcDF1 = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
    .option("dbtable", "employees")
    .option("user", "root")
    .option("password", "123456")
    .load()
    .createOrReplaceTempView("empleados")

  val jdbcDF2 = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
    .option("dbtable", "salaries")
    .option("user", "root")
    .option("password", "123456")
    .load()
    .createOrReplaceTempView("salarios")

  val jdbcDF3 = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/employees?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=UTC")
    .option("dbtable", "titles")
    .option("user", "root")
    .option("password", "123456")
    .load()
    .createOrReplaceTempView("titulos")

  spark.sql("""SELECT e.emp_no, e.first_name, e.birth_date, e.gender, e.hire_date, s.salary, s.from_date, s.to_date, t.title
              | FROM empleados e
              |	INNER JOIN salarios s ON s.emp_no=e.emp_no
              |	INNER JOIN titulos t ON t.emp_no=e.emp_no
              """.stripMargin).show()

  //Utilizando operaciones de ventana, obtener ultimo salario y ultimo cambio


//  val jdbcDF4 = jdbcDF3.s
//    .select("emp_no", " first_name", "birth_date")



}
