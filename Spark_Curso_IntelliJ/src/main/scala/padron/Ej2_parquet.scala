package padron

import org.apache.spark.sql.SparkSession

object Ej2_parquet {
  def main(): Unit ={


    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .getOrCreate()

    /* 2.2) Crear tabla hive guardada en formato parquet a partir de padron_txt */

    spark.sql("USE datos_padron")

    spark.sql("DROP TABLE IF EXISTS padron_parquet")

    spark.sql(
      """CREATE TABLE padron_parquet
        |STORED AS PARQUET
        |AS (SELECT * FROM padron_txt)""".stripMargin)

    /* 2.3) Crear tabla hive guardada en formato parquet a partir de padron_txt_2 */

    spark.sql("DROP TABLE IF EXISTS padron_parquet_2")

    spark.sql(
      """CREATE TABLE padron_parquet_2
        |STORED AS PARQUET
        |AS (SELECT * FROM padron_txt_2)""".stripMargin)



    /* 2.6) Comparar tamaño de los dicheros de los datos de las tablas */

    println("\n padron_txt " + spark.read.table("padron_txt").queryExecution.analyzed.stats)
    println("\n padron_txt_2 " + spark.read.table("padron_txt").queryExecution.analyzed.stats)
    println("\n padron_parquet " + spark.read.table("padron_txt").queryExecution.analyzed.stats)
    println("\n padron_parquet_2 " + spark.read.table("padron_txt").queryExecution.analyzed.stats)
  }
}
