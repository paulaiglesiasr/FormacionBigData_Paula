package padron

import org.apache.spark.sql.SparkSession

object Ej3_Impala {
  def main(): Unit = {

    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .getOrCreate()

    //spark.sql("USE datos_padron")

    /*
     * 3.4) Hacer INVALIDATE METADATA en Impala de la base de datos datos_padron.
     */

    // En MV con Cloudera

    /*
     * 3.5) Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres
     * y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.
     */

    // En SQL
    val t0 = System.nanoTime()
    val query1 = spark.sql(
      """
        |SELECT DESC_DISTRITO, DESC_BARRIO, SUM(EspanolesHombres) EspanolesHombres,
        |       SUM(EspanolesMujeres) EspanolesMujeres,
        |       SUM(ExtranjerosHombres) ExtranjerosHombres,
        |       SUM(ExtranjerosMujeres) ExtranjerosMujeres
        |FROM datos_padron.padron_txt
        |GROUP BY DESC_DISTRITO, DESC_BARRIO
        |""".stripMargin)
    val t1 = System.nanoTime()
    //query1.show()
    val elapsedNs1 = (t1 - t0)
    val elapsedMS1 = elapsedNs1 / 1000000000.0

    val t2 = System.nanoTime()
    val query2 = spark.sql(
      """
        |SELECT DESC_DISTRITO, DESC_BARRIO, SUM(EspanolesHombres) EspanolesHombres,
        |       SUM(EspanolesMujeres) EspanolesMujeres,
        |       SUM(ExtranjerosHombres) ExtranjerosHombres,
        |       SUM(ExtranjerosMujeres) ExtranjerosMujeres
        |FROM datos_padron.padron_txt_2
        |GROUP BY DESC_DISTRITO, DESC_BARRIO
        |""".stripMargin)
    val t3 = System.nanoTime()

    val elapsedNs2 = (t1 - t0)
    val elapsedMS2 = elapsedNs2 / 1000000000.0

    val t4 = System.nanoTime()
    val query3 = spark.sql(
      """
        |SELECT DESC_DISTRITO, DESC_BARRIO, SUM(EspanolesHombres) EspanolesHombres,
        |       SUM(EspanolesMujeres) EspanolesMujeres,
        |       SUM(ExtranjerosHombres) ExtranjerosHombres,
        |       SUM(ExtranjerosMujeres) ExtranjerosMujeres
        |FROM datos_padron.padron_parquet_2
        |GROUP BY DESC_DISTRITO, DESC_BARRIO
        |""".stripMargin)
    val t5 = System.nanoTime()

    val elapsedNs3 = (t5 - t4)
    val elapsedMS3 = elapsedNs3 / 1000000000.0

    println( "\nTiempo transcurrido para la consulta en la tabla padron_txt: "+ elapsedMS1)
    println( "\nTiempo transcurrido para la consulta en la tabla padron_txt_2: "+ elapsedMS2)
    println( "\nTiempo transcurrido para la consulta en la tabla padron_parquet_2: "+ elapsedMS3)

  }
}
