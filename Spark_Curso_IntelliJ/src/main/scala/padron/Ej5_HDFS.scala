package padron

import org.apache.spark.sql.SparkSession

object Ej5_HDFS {
  def main(): Unit ={
    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .getOrCreate()

    val df = spark.read.text("Datos/datos1.txt")

    df.repartition(1)
      .write.format("com.databricks.spark.csv")
      .save("hdfs://192.168.1.118:63856/tmp/hive/datos1.csv")
  }
}
