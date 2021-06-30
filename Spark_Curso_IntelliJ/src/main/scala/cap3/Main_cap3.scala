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

    Save_Parquet.ej_parquet()
    //DataFrameAPI.createDataFrame()
    //DataFrameAPI.definiendoSchema()
    //Read_From_JSON.readFromJSON(Array("Datos/cap3/blogs.json"))
    //Operaciones_filas_col.op(Array("Datos/cap3/blogs.json"))
    //ejercicios_extra.leer_csv()
  }
}
