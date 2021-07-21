package padron

import org.apache.spark.sql.SparkSession

object CreateExternalTable {
  def main(): Unit ={
    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .getOrCreate()

    spark.sql("CREATE EXTERNAL TABLE prueba (id int) location '/tmp/hive'").show()
  }
}
