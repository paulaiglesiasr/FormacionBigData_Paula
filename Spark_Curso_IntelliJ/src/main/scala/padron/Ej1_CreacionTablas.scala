package padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, trim}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Ej1_CreacionTablas {
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

    // 1.1) Crear la BD

    spark.sql("CREATE DATABASE IF NOT EXISTS datos_padron")
    spark.sql("USE datos_padron")

    // 1.2) Crear tabla y cargar datos
    spark.sql("DROP TABLE IF EXISTS padron_txt")

    spark.sql(
      """
        |CREATE TABLE padron_txt(
        |COD_DISTRITO int,
        |DESC_DISTRITO string,
        |COD_DIST_BARRIO int,
        |DESC_BARRIO string,
        |COD_BARRIO int,
        |COD_DIST_SECCION int,
        |COD_SECCION int,
        |COD_EDAD_INT int,
        |EspanolesHombres int,
        |EspanolesMujeres int,
        |ExtranjerosHombres int,
        |ExtranjerosMujeres int
        |)
        | ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |    WITH SERDEPROPERTIES (
        |      "separatorChar" = ";"
        |    )
        |    STORED AS TEXTFILE
        |""".stripMargin)



    spark.sql("LOAD DATA LOCAL INPATH 'Datos/padron/padron.csv' INTO TABLE padron_txt")


    val padronDF = spark.sql("SELECT * FROM padron_txt")
    padronDF.show()

    // 1.3) Hacer trim sobre datos y guardar en padron_txt_2

    println("\n1.3) Hacer trim sobre datos y guardar en padron_txt_2\n")

    spark.sql("DROP TABLE IF EXISTS padron_txt_2")

    spark.sql("CREATE TABLE padron_txt_2 AS " +
      "                               (SELECT  " +
      "                                       COD_DISTRITO, " +
      "                                       TRIM(DESC_DISTRITO), " +
      "                                       COD_DIST_BARRIO, " +
      "                                       TRIM(DESC_BARRIO), " +
      "                                       COD_BARRIO, " +
      "                                       COD_DIST_SECCION, " +
      "                                       COD_SECCION, " +
      "                                       COD_EDAD_INT, " +
      "                                       EspanolesHombres, " +
      "                                       EspanolesMujeres, " +
      "                                       ExtranjerosHombres , " +
      "                                       ExtranjerosMujeres " +
      "                                FROM padron_txt) ")

    val padronDF2 = spark.sql("SELECT * FROM padron_txt_2")
    padronDF2.show()

     //1.4) Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD DATA.

      //'LOCAL' significa que el fichero proporcionado está en el sistema de ficheros local. Si el 'LOCAL'
      //es omitido, entonces busca el fichero en HDFS.

     //1.5) Comprobar que solo hay espacios en blanco en los 4 últimos campos y después sustituirlos por 0

     println("\n1.5) Comprobar que solo hay espacios en blanco en los 4 últimos campos y después sustituirlos por 0\n")
     //Como no se pueden poner sentencias IF:

//    var padron_txtDF_sinblancos = spark.sql(
//      """
//        | IF( (SELECT COUNT(*) INTO FROM padron_txt WHERE
//        | COD_DISTRITO="" OR DESC_DISTRITO="" OR COD_DIST_BARRIO="" OR  DESC_BARRIO="" OR COD_BARRIO="" OR
//        | COD_DIST_SECCION="" OR COD_SECCION="" OR COD_EDAD_INT="") > 0,
//        |
//        | SELECT COD_DISTRITO, DESC_DISTRITO, COD_DIST_BARRIO, DESC_BARRIO, COD_BARRIO,
//        | COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT,
//        | "EspanolesHombres" = CASE WHEN EspanolesHombres="" THEN "0" ELSE EspanolesHombres END
//        |
//        | FROM padron_txt, false)
//        |""".stripMargin)

//    var cuentaBlancos = padronDF.agg(count("*").alias("cuenta"))
//                                .where(col("COD_DISTRITO")==="")
//                                .where(col("DESC_DISTRITO")==="")
//                                .where(col("COD_DIST_BARRIO")==="")
//                                .where(col("DESC_BARRIO")==="")
//                                .where(col("COD_BARRIO")==="")
//                                .where(col("COD_DIST_SECCION")==="")
//                                .where(col("COD_SECCION")==="")
//                                .where(col("COD_EDAD_INT")==="")


    var cuentaBlancos = spark.sql ("SELECT COUNT(*) cuenta FROM padron_txt WHERE COD_DISTRITO=\"\" OR " +
      "                                       DESC_DISTRITO=\"\" OR COD_DIST_BARRIO=\"\" OR  DESC_BARRIO=\"\" OR " +
      "                                       COD_BARRIO=\"\" OR COD_DIST_SECCION=\"\" OR COD_SECCION=\"\" OR " +
      "                                       COD_EDAD_INT=\"\"" +
      "                                          ")

    // another way
    //
    //val wherePaulaCondition = col(x).equalTo(lit("")) or col(y).equalTo(lit("")) .....etc

    //
    //y luego.......df.where(wherePaulaCondition).count()

    //
    //o bueno: val cuenta: Long = df.where(wherePaulaCondition).count()

    val cuenta = cuentaBlancos.select("cuenta").first.getAs[Long](0)
    print("\nCampos en blanco en los campos que no son los 4 últimos :" + cuenta )

    if (cuenta == 0) {
      var padron_txtDF_sinblancos  = spark.sql(
        """
          | SELECT COD_DISTRITO, DESC_DISTRITO, COD_DIST_BARRIO, DESC_BARRIO, COD_BARRIO,
          | COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT,
          | CASE WHEN EspanolesHombres="" THEN "0" ELSE EspanolesHombres END AS EspanolesHombres,
          | CASE WHEN EspanolesMujeres="" THEN "0" ELSE EspanolesMujeres END AS EspanolesMujeres,
          | CASE WHEN ExtranjerosHombres="" THEN "0" ELSE ExtranjerosHombres END AS ExtranjerosHombres,
          | CASE WHEN ExtranjerosMujeres="" THEN "0" ELSE ExtranjerosMujeres END AS ExtranjerosMujeres
          |
          | FROM padron_txt
          |""".stripMargin)
          padron_txtDF_sinblancos.show()
    }else{
      println("Hay campos vacíos que no están entre los últimos 4 ")
    }


    // 1.6) Crar tabla padron_txt_2 con expresiones regulares
    // He usado  org.apache.hadoop.hive.contrib.serde2.RegexSerDe porque el otro daba error de serialización

    //("\n1.6) Crar tabla padron_txt_2 con expresiones regulares\n")

//    spark.sql(
//      """
//        |CREATE TABLE padron_txt_2_regex
//        | ROW FORMAT SERDE ' org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
//        |    WITH SERDEPROPERTIES (
//        |       'input.regex'=' *[a-z] *'
//        |       'output.format.string'='[a-z]'
//        |    )
//        |    STORED AS TEXTFILE
//        |AS (SELECT * FROM padron_txt)
//        |""".stripMargin)
//
//    val padron_txt_2_regexDF = spark.sql("SELECT * FROM padron_txt_2_regex")
//    padron_txt_2_regexDF.show()

    println("\nFIN\n")
  }
}
