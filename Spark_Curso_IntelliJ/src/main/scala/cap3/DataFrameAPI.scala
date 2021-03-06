package cap3

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataFrameAPI {
  // (Cap 3 empieza en pag 41). Pag 45
  def createDataFrame(): Unit = {
    // In Scala
    import org.apache.spark.sql.functions.avg
    import org.apache.spark.sql.SparkSession

    // Create a DataFrame using SparkSession
    val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .getOrCreate()

    // Create a DataFrame of names and ages
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

    // Group the same names together, aggregate their ages, and compute an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    // Show the results of the final execution

    // Show the results of the final execution
    avgDF.show()
  }
  // Pag 51
  def definiendoSchema(): Unit ={
    //Definiendo esquema

    // Programando (Programatically)
    val schema = StructType(Array(StructField("author", StringType, false),
                  StructField("title", StringType, false),
                  StructField("pages", IntegerType, false)))

    print("\nSchema programando: " + schema + "\n")

    //In DDL
    val schemaDDL = "author STRING, title STRING, pages INT"
    print("\nSchema en DDL: " + schema+ "\n")
  }
}
