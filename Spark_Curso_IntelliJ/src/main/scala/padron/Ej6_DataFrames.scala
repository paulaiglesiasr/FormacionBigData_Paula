package padron

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext

object Ej6_DataFrames {
  def main(): Unit ={

    val spark = SparkSession
      .builder
      .appName("practica_padron")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:/Users/paula.iglesias/Documents/FormacionBigData_Paula/Spark_Curso_IntelliJ/hive_location")
      .getOrCreate()

    /*
    6.1)Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable
    intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos,
    que sustituyan los valores vacíos por 0 y que infiera el esquema.
     */
    val padronDF_sintrim =
      spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("emptyValue", 0)
        .option("delimiter", ";")
        //.option("ignoreLeadingWhiteSpace", "true")
        //.option("ignoreTrailingWhiteSpace", "true")
        .load("Datos/padron/padron.csv")

    var padronDF = padronDF_sintrim
                      .withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
                      .withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))

    println("\nTabla padronDF\n")
    padronDF.show()

    /*
    6.3)Enumera todos los barrios diferentes.
     */
    println("\nEnumera todos los barrios diferentes\n")
    padronDF.select("DESC_BARRIO").distinct().show()

    /*
    6.4)Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barriosdiferentes que hay.
     */
    padronDF.createOrReplaceTempView("padron")

    spark.sql("SELECT COUNT(*) NUMERO_BARRIOS FROM (SELECT DISTINCT DESC_BARRIO FROM padron )").show()

    /*
    6.5)Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".
     */
    padronDF.withColumn("Long_DESC_DISTRITO", length(col("DESC_DISTRITO"))).show()

    /*
    6.6)Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
     */
    println("\n6.6)Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.\n")
    padronDF = padronDF.withColumn("valor5", lit(5))
    padronDF.show()

    /*
    6.7)Borra esta columna.
     */
    println("\n6.7)Borra esta columna\n")
    padronDF = padronDF.drop("valor5")
    padronDF.show()

    /*
    6.8)Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.
     */
    println("\n6.8)Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.\n")
    val padronDF_part = padronDF.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
    padronDF_part.show()

    /*
    6.9)Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estadode
    los rdds almacenados.
     */
    println("\n6.9)Almacénalo en caché.\n")
    padronDF.cache()

    //while(true){}

    /*
    6.10)Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres",
    "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito.
    Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados
    en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".
     */
    println("\n6.10)Total de españoles y extranejeros agrupados por distrito y barrio.\n")
    padronDF
      .select("DESC_DISTRITO", "DESC_BARRIO", "espanoleshombres", "espanolesmujeres", "extranjeroshombres", "extranjerosmujeres" )
      .groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .agg(sum("espanoleshombres").alias("espanoleshombres"),
           sum("espanolesmujeres").alias("espanolesmujeres"),
           sum("extranjeroshombres").alias("extranjeroshombres"),
           sum("extranjerosmujeres").alias("extranjerosmujeres"))
      .sort(desc("extranjerosmujeres"), desc("extranjeroshombres") )
      .show()

    /*
    6.11)Elimina el registro en caché.
     */
    padronDF.unpersist()

    /*
    6.12)Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con
    DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio.
    Únelo (con un join) con el DataFrame original a través de las columnas en común.
     */

    println("\n6.12)número total de \"espanoleshombres\" residentes en cada distrito de cada barrio + join.\n")
    val padronDF2 = padronDF.
      select("DESC_BARRIO", "DESC_DISTRITO", "espanoleshombres" )
      .groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .agg(sum("espanoleshombres"))


    import spark.sqlContext.implicits._

    padronDF.join(padronDF2, padronDF("DESC_BARRIO") === padronDF2("DESC_BARRIO") &&
      padronDF("DESC_DISTRITO") === padronDF2("DESC_DISTRITO"), "inner")
      .show()

    /*
    6.13)Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
     */
    println("\n6.13)repite usando funciones de ventana.\n")
    //val padronDF_group = padronDF.groupBy("DESC_DISTRITO", "DESC_BARRIO")

    val windowDF = Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")

    val windowQueryDF = padronDF
      .withColumn("Sum_esp_hombres",sum(col("espanoleshombres")).over(windowDF))
      //.groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .select("DESC_BARRIO", "DESC_DISTRITO", "Sum_esp_hombres" )
      .show()

    /*
    6.14)Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) quecontenga los
    valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT).
    Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas .
    El aspecto debe ser similar a este:
     */
    println("\n6.14)pivot\n")

    val padronDF_pivot = padronDF
      .filter(col("DESC_DISTRITO") ==="BARAJAS" ||
        col("DESC_DISTRITO") ==="CENTRO" ||
        col("DESC_DISTRITO") ==="RETIRO")
      .groupBy( "COD_EDAD_INT")
      .pivot("DESC_DISTRITO")
      .agg(sum("EspanolesMujeres").alias("Sum_EspanolesMujeres"))

    padronDF_pivot.show()

    /*
    6.15)Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de
    "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos.
    Debe estar redondeada a 2 decimales. Puedes imponerte la condición extra de no apoyarte en ninguna columna
    auxiliar creada para el caso.
     */

    padronDF_pivot
      .withColumn("%BARAJAS",round((col("BARAJAS")/(col("CENTRO")+col("RETIRO")+col("BARAJAS")))*100/5)*5/100)
      .withColumn("%CENTRO",round(col("CENTRO")/(col("CENTRO")+col("RETIRO")+col("BARAJAS"))*100/5)*5/100)
      .withColumn("%RETIRO",round(col("RETIRO")/(col("CENTRO")+col("RETIRO")+col("BARAJAS"))*100/5)*5/100)
      .show()

    /*
    6.16)Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local.
    Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.
     */

    padronDF_part
      .write
      .mode("overwrite")
      .format("csv")
      .save("Datos/padron/padron_part.csv")

    /*
    6.17)Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.
     */

    padronDF_part
      .write
      .mode("overwrite")
      .format("parquet")
      .save("Datos/padron/padron_part.parquet")

  }
}
