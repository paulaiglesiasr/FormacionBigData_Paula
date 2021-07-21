package padron

import org.apache.spark.sql.SparkSession

object Ej4_TablasParticionadas_2 {
  def main(): Unit = {

    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .config("spark.hadoop.hive.exec.dynamic.partition","true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.dynamic.partition",true)
      .config("hive.exec.max.dynamic.partitions",50000)
      .config("hive.exec.max.dynamic.partitions.pernode",5000)
      .getOrCreate()

    //spark.sql("USE datos_padron")

//    spark.sql("DROP TABLE IF EXISTS padron_parquet_qtiene")
//    spark.sql(
//      """
//        |CREATE TABLE padron_parquet_qtiene
//        | ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
//        |    WITH SERDEPROPERTIES (
//        |       'input.regex'="(.*)(.*)",
//        |       'output.format.string'="%1$s %2$s"
//        |    )
//        |    STORED AS TEXTFILE
//        |AS (SELECT * FROM padron_parquet_2)
//        |""".stripMargin)
//
//    spark.sql("Select * from padron_parquet_qtiene ").show()
    /*
     * 4.1) Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO
     * y DESC_BARRIO cuyos datos estén en formato parquet.
     */

    spark.sql("USE datos_padron")


    println("\n\n set dynamic partition\n\n")

    spark.sql( "set spark.hadoop.hive.exec.dynamic.partition = true")
    spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode = nonstrict")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    println("\n\n fin set dynamic partition\n\n")

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

//    spark.sql("alter table my_table add partition (DESC_DISTRITO string, DESC_BARRIO string) location Datos")

    /*
     * 4.2) Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla
     * recién creada a partir de un select de la tabla padron_parquet_2.
     */


    //println("\npadron_parquet_2:\n")
    println("\n describe padron_txt_2 \n")

    spark.sql("describe padron_txt_2").show()



    spark.sql(
        """INSERT overwrite TABLE padron_particionado
          |PARTITION(DESC_DISTRITO, DESC_BARRIO)
          |SELECT COD_DISTRITO, COD_DIST_BARRIO, COD_BARRIO, COD_DIST_SECCION,
          |   COD_SECCION, COD_EDAD_INT, EspanolesHombres, EspanolesMujeres,
          |    ExtranjerosHombres, ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO
          |FROM datos_padron.padron_txt_2""".stripMargin)

    spark.sql("SELECT * FROM padron_particionado").show()

    //spark.sql("SELECT * FROM padron_parquet_2").show()

    /*
      4.4)Calcular el total de EspanolesHombres, EspanolesMujeres,
          ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y
          DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.
     */
//    spark.sql(
//      """
//        |SELECT DESC_DISTRITO, DESC_BARRIO, SUM(EspanolesHombres) EspanolesHombres,
//        |       SUM(EspanolesMujeres) EspanolesMujeres,
//        |       SUM(ExtranjerosHombres) ExtranjerosHombres,
//        |       SUM(ExtranjerosMujeres) ExtranjerosMujeres
//        |FROM datos_padron.padron_parquet_2
//        |WHERE DESC_DISTRITO="CENTRO" OR DESC_DISTRITO="LATINA" OR DESC_DISTRITO="CHAMARTIN"
//        |      OR DESC_DISTRITO="TETUAN" OR DESC_DISTRITO="VICALVARO" OR DESC_DISTRITO="BARAJAS"
//        |GROUP BY DESC_DISTRITO, DESC_BARRIO
//        |""".stripMargin).show(100)


  }
}
