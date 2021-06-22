package cap3

import org.apache.spark.{SparkConf, SparkContext}

object Main_cap3 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Documents/winutils-master/hadoop-2.7.1")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Application")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Cap 3

    //ejercicios3.ej_parquet()
    //DataFrameAPI.createDataFrame()
    DataFrameAPI.definiendoSchema()
  }
}
