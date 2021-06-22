package cap2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Quijote {
  def main(): Unit = {

    // Create a DataFrame using SparkSession
    val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .getOrCreate()

    val quijote = spark.read.text("C:/Users/paula.iglesias/Desktop/Datasets_CursoSpark/el_quijote.txt")
    val nLineas = quijote.count()
    println("\n\nNumero de lineas en el Quijote: " + nLineas + "\n\n")
  }
}
