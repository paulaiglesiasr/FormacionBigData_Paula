package cap5

object EjBasico_y_comparacionSQLAPI {
  def comparacion() {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder
      .appName("SparkSQLExampleApp")
      .getOrCreate()

    //This generally happens when a cluster is shutdown while writing a table.
    // The recomended solution from Databricks documentation:
    //This flag deletes the _STARTED directory and returns the process to the original state. For example, you can set it in the notebook
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    import spark.sqlContext.implicits._

    // Crear DataFrame
    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA")).toDF("Author", "State")
    rows.write.mode("overwrite").saveAsTable("autores_y_estados")

    val t0 = System.nanoTime()
    val plan1 = spark.sql("SELECT * FROM autores_y_estados")
    val t1 = System.nanoTime()
    val elapsedNs = (t1 - t0)
    val elapsedMS = elapsedNs / 1000000000.0

    val t2 = System.nanoTime()
    val plan2 = rows.select("*")
    val t3 = System.nanoTime()
    val elapsedNs2 = (t3 - t2)
    val elapsedMS2 = elapsedNs2 / 1000000000.0

    println("Elapsed time SQL: " + elapsedMS  + "s" )
    println(plan1.queryExecution.logical)
    println(plan1.queryExecution.optimizedPlan)
    println("Elapsed time API: " + elapsedMS2  + "s" )
    println(plan2.queryExecution.logical)
    println(plan2.queryExecution.optimizedPlan)



  }
}
