package padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Ej1_CreacionTablas_expresionregular {
  def main(): Unit ={

    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Application")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)


    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .getOrCreate()


    spark.sql("USE datos_padron")

    println("\n1.6) Crar tabla padron_txt_2 con expresiones regulares\n")

    spark.sql("SELECT * FROM padron_txt").show()

    // Para nombres con espacios  (.*?)\s
    //spark.sql("DROP TABLE IF EXISTS pruebaRegex")


//    desc_distrito string,
//    cod_dist_barrio int,
//    desc_barrio string,
//    cod_barrio int,
//    cod_dist_seccion int,
//    cod_seccion int,
//    cod_edad_int int,
//    espanoleshombres int,
//    espanolesmujeres int,
//    extranjeroshombres int,
//    extranjerosmujeres int

//    spark.sql(
//      """CREATE TABLE pruebaRegex (
//        |cod_distrito string)
//        |""".stripMargin)
//
////,"CENTRO"   ,101,"PALACIO"    ,1,1006,6,100,2,1,3,4
//
//    spark.sql(
//      """INSERT INTO pruebaRegex VALUES
//        |(1)
//        |""".stripMargin)

    // \s*(\d+)\s*\s*(\w+)\s*\s*(\d+)\s*\s*(\w+)\s*\s*(\d+)\s*\s*(\d+)\s*\s*(\d+)\s*\s*(\d+)\s*\s*(\d*)\s*\s*(\d*)\s*\s*(\d*)\s*\s*(\d*)\s*
    // \s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.*)\s*\s*(.*)\s*\s*(.*)\s*\s*(.*)\s*
    // "\"(.*)\"\;\"(.*?)\\s*\"\;\"(.*)\"\;\"(.*?)\\s*\"\;\"(.*)\"\;\"(.*)\"\;\"(.*)\"\;\"(.*)\"\;\"(.*)\"\;\"(.*)\"\;\"(.*)\"\;\"(.*)\""


    //\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.+)\s*\s*(.*)\s*\s*(.*)\s*\s*(.*)\s*\s*(.*)\s*

    //%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s
    spark.sql("DROP TABLE IF EXISTS padron_txt_2_regex")
    spark.sql(
      """
        |CREATE TABLE padron_txt_2_regex
        | ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
        |    WITH SERDEPROPERTIES (
        |       'input.regex'="(\\d+)\\s+(\\w+)\\s+(\\d+)\\s+(\\w+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s(\\d*|\\s)\\s?(\\d*|\\s)\\s?(\\d*|\\s)\\s?(\\d*|\\s)\\s?",
        |       'output.format.string'="%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s"
        |    )
        |    STORED AS TEXTFILE
        |AS (SELECT * FROM padron_txt)
        |""".stripMargin)

    val padron_txt_2_regexDF = spark.sql("SELECT * FROM padron_txt_2_regex")
    padron_txt_2_regexDF.show()

    println("\nFIN\n")
  }
}
