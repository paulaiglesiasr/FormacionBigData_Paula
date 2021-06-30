package cap5

import org.apache.spark.sql.SparkSession

object PostreSQL {
  def helloWorld(): Unit ={

    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    // Read Option 1: Loading data from a JDBC source using load method
    val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5433/TiendaRopa")
      .option("dbtable", "public.clientes")
      .option("user", "postgres")
      .option("password", "123456")
      .load()
      .show()


  }
}
