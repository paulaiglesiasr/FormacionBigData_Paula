package cap3

import org.apache.spark.sql.SparkSession

object Save_Parquet {
  def ej_parquet(){

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    val data = Seq(("James ","","Smith","36636","M",3000),
      ("Michael ","Rose","","40288","M",4000),
      ("Robert ","","Williams","42114","M",4000),
      ("Maria ","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","","F",-1))

    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")

    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    println("\n Numero de particiones: "+ df.rdd.getNumPartitions)

    df.repartition(4)
      .write
      .format("json")
      .mode("overwrite")
      .save("/Filestore/Tables/example_cap3_json")

  }
}
