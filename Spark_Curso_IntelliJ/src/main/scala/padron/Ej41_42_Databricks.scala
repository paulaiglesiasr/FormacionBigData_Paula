package padron

import org.apache.spark.sql.SparkSession

object Ej41_42_Databricks {
  def main(): Unit = {
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
    spark.sql("DROP TABLE IF EXISTS padron_txt_sincast")

    println("\n show create table \n")

    spark.sql(
      """
        |CREATE TABLE padron_txt_sincast(
        |COD_DISTRITO string,
        |DESC_DISTRITO string,
        |COD_DIST_BARRIO string,
        |DESC_BARRIO string,
        |COD_BARRIO string,
        |COD_DIST_SECCION string,
        |COD_SECCION string,
        |COD_EDAD_INT string,
        |EspanolesHombres string,
        |EspanolesMujeres string,
        |ExtranjerosHombres string,
        |ExtranjerosMujeres string
        |)
        | ROW FORMAT DELIMITED
        | FIELDS TERMINATED BY ';'
        | LINES TERMINATED BY '\n'
        |    STORED AS TEXTFILE
        |""".stripMargin).show()

    spark.sql("LOAD DATA LOCAL INPATH 'Datos/padron/padron.csv' INTO TABLE padron_txt_sincast")


    //spark.sql("SELECT * FROM padron_txt_sincast").show()

    spark.sql("DROP TABLE IF EXISTS padron_txt_replace")

    spark.sql(
      """
        |CREATE TABLE padron_txt_replace AS
        | (SELECT
        |REPLACE( COD_DISTRITO, '"' , '' ) COD_DISTRITO,
        |DESC_DISTRITO,
        |REPLACE( COD_DIST_BARRIO, '"' , '' ) COD_DIST_BARRIO,
        |DESC_BARRIO,
        |REPLACE( COD_BARRIO, '"' , '' ) COD_BARRIO,
        |REPLACE( COD_DIST_SECCION, '"' , '' ) COD_DIST_SECCION,
        |REPLACE( COD_SECCION, '"' , '' ) COD_SECCION,
        |REPLACE( COD_EDAD_INT, '"' , '' ) COD_EDAD_INT,
        |REPLACE( EspanolesHombres, '"' , '' ) EspanolesHombres,
        |REPLACE( EspanolesMujeres, '"' , '' ) EspanolesMujeres,
        |REPLACE( ExtranjerosHombres, '"' , '' ) ExtranjerosHombres,
        |REPLACE( ExtranjerosMujeres, '"' , '' ) ExtranjerosMujeres
        | FROM padron_txt_sincast)
        |""".stripMargin)


    spark.sql("SELECT * FROM padron_txt_sincast").show()
    spark.sql("SELECT * FROM padron_txt_replace").show()
    spark.sql("DROP TABLE IF EXISTS padron_txt")

    spark.sql(
      """
        |CREATE TABLE padron_txt AS
        | (SELECT
        |CAST (COD_DISTRITO AS int) COD_DISTRITO,
        |DESC_DISTRITO,
        |CAST (COD_DIST_BARRIO  AS int) COD_DIST_BARRIO,
        |DESC_BARRIO,
        |CAST (COD_BARRIO  AS int) COD_BARRIO,
        |CAST (COD_DIST_SECCION  AS int) COD_DIST_SECCION,
        |CAST (COD_SECCION  AS int) COD_SECCION,
        |CAST (COD_EDAD_INT  AS int) COD_EDAD_INT,
        |CAST (EspanolesHombres  AS int) EspanolesHombres,
        |CAST (EspanolesMujeres  AS int) EspanolesMujeres,
        |CAST (ExtranjerosHombres  AS int) ExtranjerosHombres,
        |CAST (ExtranjerosMujeres  AS int) ExtranjerosMujeres
        | FROM padron_txt_replace)
        |""".stripMargin)


    /*
    spark.sql(
          """
            |CREATE TABLE padron_txt AS
            | (SELECT
            |CAST((REPLACE( COD_DISTRITO, '"' , '' ) COD_DISTRITO)  AS int),
            |DESC_DISTRITO,
            |CAST((REPLACE( COD_DIST_BARRIO, '"' , '' ) COD_DIST_BARRIO)  AS int),
            |DESC_BARRIO,
            |CAST((REPLACE( COD_BARRIO, '"' , '' ) COD_BARRIO)  AS int),
            |CAST((REPLACE( COD_DIST_SECCION, '"' , '' ) COD_DIST_SECCION)  AS int),
            |CAST((REPLACE( COD_SECCION, '"' , '' ) COD_SECCION)  AS int),
            |CAST((REPLACE( COD_EDAD_INT, '"' , '' ) COD_EDAD_INT)  AS int),
            |CAST((REPLACE( EspanolesHombres, '"' , '' ) EspanolesHombres)  AS int),
            |CAST((REPLACE( EspanolesMujeres, '"' , '' ) EspanolesMujeres)  AS int),
            |CAST((REPLACE( ExtranjerosHombres, '"' , '' ) ExtranjerosHombres)  AS int),
            |CAST((REPLACE( ExtranjerosMujeres, '"' , '' ) ExtranjerosMujeres)  AS int),
            | FROM padron_txt_sincast)
            |""".stripMargin)
            */

    spark.sql("SELECT * FROM padron_txt").show()

    spark.sql("DROP TABLE IF EXISTS padron_txt_21")

    spark.sql("""CREATE TABLE padron_txt_21 AS
             (SELECT COD_DISTRITO, trim(DESC_DISTRITO) DESC_DISTRITO,
             COD_DIST_BARRIO, trim(DESC_BARRIO) DESC_BARRIO, COD_BARRIO,
             COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT,
             CASE WHEN EspanolesHombres="" THEN 0 ELSE EspanolesHombres END AS EspanolesHombres,
             CASE WHEN EspanolesMujeres="" THEN 0 ELSE EspanolesMujeres END AS EspanolesMujeres,
             CASE WHEN ExtranjerosHombres="" THEN 0 ELSE ExtranjerosHombres END AS ExtranjerosHombres,
             CASE WHEN ExtranjerosMujeres="" THEN 0 ELSE ExtranjerosMujeres END AS ExtranjerosMujeres
             FROM padron_txt) """)
    spark.sql("SELECT * FROM padron_txt_21").show()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql("DROP TABLE IF EXISTS padron_particionado")

    spark.sql(
      """
        |CREATE TABLE padron_particionado(
        |COD_DISTRITO int,
        |COD_DIST_BARRIO int,
        |COD_BARRIO int,
        |COD_DIST_SECCION int,
        |COD_SECCION int,
        |COD_EDAD_INT int,
        |EspanolesHombres int,
        |EspanolesMujeres int,
        |ExtranjerosHombres int,
        |ExtranjerosMujeres int
        |)
        | PARTITIONED BY (DESC_DISTRITO string, DESC_BARRIO string)
        | STORED AS PARQUET
        |
        |""".stripMargin)

    spark.sql(
      """INSERT overwrite TABLE padron_particionado
        |PARTITION(DESC_DISTRITO, DESC_BARRIO)
        |SELECT COD_DISTRITO, COD_DIST_BARRIO, COD_BARRIO, COD_DIST_SECCION,
        |   COD_SECCION, COD_EDAD_INT, EspanolesHombres, EspanolesMujeres,
        |    ExtranjerosHombres, ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO
        |FROM padron_txt_21""".stripMargin)

    spark.sql("SELECT * FROM padron_particionado").show()

  }
}
