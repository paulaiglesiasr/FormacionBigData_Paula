package padron

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main_padron_2 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:/Users/paula.iglesias/Documents/winutils-master/hadoop-2.7.1")

    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark Application")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    // Cap 5

    //"COD_DISTRITO";"DESC_DISTRITO";"COD_DIST_BARRIO";"DESC_BARRIO";"COD_BARRIO";"COD_DIST_SECCION";"COD_SECCION";"COD_EDAD_INT";"EspanolesHombres";"EspanolesMujeres";"ExtranjerosHombres";"ExtranjerosMujeres"

    //Ej1_CreacionTablas_2.main()

    //Ej1_CreacionTablas_expresionregular.main()

    //Ej2_parquet_2.main()

    //Ej3_Impala_2.main()

    //Ej4_TablasParticionadas_2.main()
    //Ej41_42_Databricks.main()

    //CreateExternalTable.main()

    //Ej5_HDFS.main()

    Ej6_DataFrames.main()
  }
}
